"""
Main ETL pipeline for ingesting weather data from Open-Meteo API.

The pipeline follows these steps:
1. Load: Fetch data from API and save raw responses to CSV
2. Check: Validate data quality and completeness
3. Parse: Convert to Parquet with schema validation

Data is organized in a multi-stage data lake:
/data
  /raw          - Original CSV files from API (unstructured, no parsing)
  /processed    - Parquet files with schema validation (structured, parsed)
  /transformed  - (Future) Final cleaned and transformed data (structured, normalized)

Each stage adds structure and validation while maintaining data lineage.
"""

import re
from datetime import datetime, timezone

import openmeteo_requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from openmeteo_sdk.Unit import Unit
from openmeteo_sdk.WeatherApiResponse import WeatherApiResponse

from open_meteo_etl.config import get_queries
from open_meteo_etl.src.utils import (
    PROCESSED_DATA_DIR,
    RAW_DATA_DIR,
    setup_api_client,
    setup_logging,
)
from open_meteo_etl.src.weather_query import BaseWeatherQueryConfig
from open_meteo_etl.src.weather_schema import FREQUENCY_TO_SCHEMA, BaseWeatherMetrics

log = setup_logging()

# Create mapping of Open Meteo Unit enum values to their column names
UNIT_NAMES = {
    getattr(Unit, name): name for name in dir(Unit) if not name.startswith("_")
}


class OpenMeteoMissingResponseError(Exception):
    """Custom exception for when Open-Meteo API returns no response data"""


class OpenMeteoClientError(Exception):
    """Custom exception for client-side errors when making requests to Open-Meteo API"""


def load(
    openmeteo: openmeteo_requests.Client,
    config: BaseWeatherQueryConfig,
) -> pd.DataFrame:
    """
    Fetch weather data from Open-Meteo API for a single location and save to raw storage.

    Args:
        openmeteo: Open-Meteo API client
        config: Weather query configuration object containing API parameters

    Returns:
        DataFrame containing weather data

    Raises:
        OpenMeteoClientError: If the API request fails
        OpenMeteoMissingResponseError: If no data is returned from the API
    """

    def _process_response(response: WeatherApiResponse) -> pd.DataFrame:
        """Process a single API response into a DataFrame."""
        log.debug("Processing response from Open-Meteo API")

        # Get the response method based on frequency
        response_method = getattr(response, config.response_method)
        if not (data_obj := response_method()):
            raise OpenMeteoMissingResponseError(f"No data received for {repr(config)}")

        log.debug(f"Creating DataFrame with {len(config.metrics)} metrics")
        # Create timeseries DataFrame with field names from the metrics
        data = {}
        for idx, metric in enumerate(config.metrics):
            field_name = metric.value[0]
            data[field_name] = data_obj.Variables(idx).ValuesAsNumpy()

        # Get the start time and interval
        data["obs_timestamp"] = pd.date_range(
            start=pd.to_datetime(data_obj.Time(), unit="s", utc=True),
            end=pd.to_datetime(data_obj.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=data_obj.Interval()),
            inclusive="left",
        )

        # Create DataFrame from the dictionary
        df = pd.DataFrame(data)

        # Add metadata columns
        df["latitude"] = response.Latitude()
        df["longitude"] = response.Longitude()
        df["elevation"] = response.Elevation()
        df["model"] = response.Model()
        df["utc_offset_seconds"] = response.UtcOffsetSeconds()
        df["timezone"] = response.Timezone()
        df["timezone_abbreviation"] = response.TimezoneAbbreviation()
        df["generation_time_ms"] = response.GenerationTimeMilliseconds()
        df["ingested_at"] = datetime.now(tz=timezone.utc).isoformat()

        # Add unit columns for each metric
        for idx, metric in enumerate(config.metrics):
            field_name = metric.value[0]
            unit = UNIT_NAMES[data_obj.Variables(idx).Unit()]
            df[f"unit_{field_name}"] = unit

        log.debug(f"Generated DataFrame with shape {df.shape}")
        return df

    log.info(f"Fetching weather data for {repr(config)}")
    log.debug(f"Using API URL: {config.api_url}")
    log.debug(f"Request params: {config.request_params()}")

    try:
        responses = openmeteo.weather_api(
            config.api_url,
            params=config.request_params(),
            verify=False,
        )
        log.debug(f"Received {len(responses)} response(s) from API")

        if not len(responses) == 1:
            raise OpenMeteoMissingResponseError(
                f"Expected 1 response for {repr(config)}, but got {len(responses)=}"
            )

        df = _process_response(responses[0])

        # Save raw data
        log.debug(f"Preparing to save DataFrame with shape {df.shape}")
        filepath = RAW_DATA_DIR / config.output_filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        log.debug(f"Saving to path: {filepath}")
        df.to_csv(filepath, index=False)
        log.info(f"Successfully loaded to {filepath}")

        return df

    except Exception as e:
        raise OpenMeteoClientError(f"Failed to fetch data: {str(e)}") from e


def check_data_integrity(df: pd.DataFrame) -> None:
    """
    Perform quality checks on weather data and log any issues found.

    Args:
        df: DataFrame containing weather data
    """

    log.info("Checking weather data quality")
    errors = []

    # Check if DataFrame is empty
    if df.empty:
        errors.append("Weather data DataFrame is empty")
        return

    # Check for minimum expected rows
    if len(df) < 24:  # Assuming we expect at least 24 readings
        errors.append(
            f"Insufficient data points: found {len(df)} rows, expected at least 24"
        )

    # Check for duplicate timestamps
    duplicate_times = df["obs_timestamp"].duplicated().sum()
    if duplicate_times > 0:
        errors.append(f"Found {duplicate_times} duplicate timestamp entries")

    # Check data shape consistency
    metadata = {field for field, _ in BaseWeatherMetrics.get_metadata_schema()}
    metric_cols = [
        col for col in df.columns if not (col in metadata or col.startswith("unit_"))
    ]

    assert metric_cols, "No metric columns found"

    # Check for metrics columns exceeding 20% null threshold
    null_counts = df[metric_cols].isnull().sum()
    total_rows = len(df)
    null_percentages = (null_counts / total_rows * 100).round(2)
    cols_exceeding_threshold = null_percentages[null_percentages > 20].to_dict()

    if cols_exceeding_threshold:
        errors.append(
            f"Columns exceeding 20% null threshold: {cols_exceeding_threshold}"
        )

    # If any errors were found, log them all as warnings
    if errors:
        for error in errors:
            log.warning(f"Data integrity issue detected: {error}")
        return

    log.info("Weather data integrity check passed")


def parse(df: pd.DataFrame, output_filename: str) -> None:
    """
    Convert raw weather data to Parquet format with schema validation.

    Args:
        df: DataFrame containing raw weather data
        output_filename: Name of the output file

    Raises:
        ValueError: If data doesn't conform to expected schema
            or frequency cannot be extracted
    """
    log.debug(f"Preparing to parse and save DataFrame with shape {df.shape}")

    # Extract frequency from path like "frequency=15_MINUTE"
    if not (frequency_match := re.search(r"frequency=([^/]+)", output_filename)):
        raise ValueError(f"Could not extract frequency from: {output_filename}")
    frequency = frequency_match.group(1)
    schema = FREQUENCY_TO_SCHEMA[frequency]

    # Add unit columns to schema
    unit_columns = [col for col in df.columns if col.startswith("unit_")]
    schema.extend([(col, pa.string()) for col in unit_columns])

    # Verify all DataFrame columns exist in schema
    if invalid := set(df.columns) - set(field[0] for field in schema):
        raise ValueError(f"DataFrame contains columns not defined in schema: {invalid}")

    # Filter schema to only include columns present in DataFrame
    filtered_schema = [(name, dtype) for name, dtype in schema if name in df.columns]
    pa_schema = pa.schema(filtered_schema)

    # Convert fields to datetime
    df["ingested_at"] = pd.to_datetime(df["ingested_at"])
    df["obs_timestamp"] = pd.to_datetime(df["obs_timestamp"])

    try:
        table = pa.Table.from_pandas(df, schema=pa_schema)
    except pa.ArrowInvalid as e:
        raise ValueError(
            "Data does not conform to expected schema:\n"
            f"Schema: {pa_schema}\n"
            f"DataFrame dtypes:\n{df.dtypes}\n"
            f"Sample of data:\n{df.head(2)}\n"
            f"Original error: {str(e)}"
        ) from e

    # Save to parquet file
    parquet_filename = output_filename.replace(".csv", ".snappy.parquet")
    filepath = PROCESSED_DATA_DIR / parquet_filename
    filepath.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, filepath, compression="snappy")
    log.info(f"Successfully parsed to {filepath}")


if __name__ == "__main__":
    log.info("Starting weather data extraction")
    openmeteo = setup_api_client()
    configs = get_queries()
    log.info(f"Processing {len(configs)} weather query configs")

    for config in configs:
        weather_data = load(openmeteo=openmeteo, config=config)
        check_data_integrity(weather_data)
        parse(weather_data, output_filename=config.output_filename)
        # Future transformation steps:
        # - Separate timeseries and reference data
        # - Calculate derived metrics
        # - Deduplication
        # - Normalize to common unit system
        # - Handle missing values and data validation
        # - Split data into daily files by location and frequency
        # - Store data to final storage location

    log.info("Weather data ingestion completed")

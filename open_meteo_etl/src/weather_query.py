"""
Data models for the Open-Meteo Weather API.

Provides strongly-typed configurations for querying weather data:
- WeatherForecastQueryConfig: Future forecasts (-92d to +16d)
- WeatherHistoricalQueryConfig: Historical data (1940 to -2d)

Features:
- Location support: Both predefined cities and custom coordinates
- Frequency options: 15-min, hourly, and daily data
- Metric selection: Customizable weather measurements
- Validation: Ensures API compatibility and data consistency
- Output paths: Generates standardized paths in data lake

Output File Structure:
{Historical|Forecast}/
  location={BERLIN|52.52_13.41}/
    frequency={15_MINUTE|1_HOUR|EOD}/
      {start_time}_{end_time}.{csv|parquet}
"""

import datetime as dtm
from abc import ABC, abstractmethod
from datetime import date
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Type, TypedDict, Union

from openmeteo_sdk.WeatherApiResponse import WeatherApiResponse
from pydantic import BaseModel, Field, field_validator, model_validator

from open_meteo_etl.src.weather_schema import (
    BaseWeatherMetrics,
    DailyWeatherMetrics,
    HourlyWeatherMetrics,
    MinutelyWeatherMetrics,
)


class Location(str, Enum):
    """Predefined locations with their coordinates (latitude, longitude)."""

    BERLIN = "BERLIN"
    LONDON = "LONDON"
    PARIS = "PARIS"
    NEW_YORK = "NEW_YORK"

    def get_coordinates(self) -> Tuple[float, float]:
        """Returns the coordinates (latitude, longitude) for the location."""
        return LOCATION_COORDINATES[self]


# Mapping of locations to their coordinates
LOCATION_COORDINATES: Dict[Location, Tuple[float, float]] = {
    Location.BERLIN: (52.52, 13.41),
    Location.LONDON: (51.51, -0.13),
    Location.PARIS: (48.85, 2.35),
    Location.NEW_YORK: (40.71, -74.01),
}


class WeatherFrequency(str, Enum):
    """Frequency of weather data measurements."""

    MINUTELY_15 = "15_MINUTE"
    HOURLY = "1_HOUR"
    EOD = "EOD"


class FrequencyMapping(TypedDict):
    """Mapping of frequency-specific configuration parameters.

    Attributes:
        field_name: API field name for the frequency (e.g., 'hourly', 'daily')
        response_method: WeatherApiResponse method to process the data
        start_param: API parameter name for start time
        end_param: API parameter name for end time
        weather_metrics_cls: Class defining the available metrics for this frequency
    """

    field_name: str
    response_method: Type[WeatherApiResponse]
    start_param: str
    end_param: str
    weather_metrics_cls: Type[BaseWeatherMetrics]


class BaseWeatherQueryConfig(BaseModel, ABC):
    """Base class for weather query configurations"""

    def __repr__(self) -> str:
        """Returns a readable string representation of the weather query."""
        params = self.request_params()
        params_str = "\n  ".join(f"{k}: {v}" for k, v in params.items())

        return (
            f"{self.__class__.__name__}:\n"
            f"  {params_str}\n"
            f"  API URL: {self.api_url}\n"
            f"  Output: {self.output_filename}"
        )

    # Mapping for frequency-related data
    FREQUENCY_MAPPINGS: Dict[WeatherFrequency, FrequencyMapping] = {
        WeatherFrequency.MINUTELY_15: {
            "field_name": "minutely_15",
            "response_method": WeatherApiResponse.Minutely15,
            "start_param": "start_minutely_15",
            "end_param": "end_minutely_15",
            "weather_metrics_cls": MinutelyWeatherMetrics,
        },
        WeatherFrequency.HOURLY: {
            "field_name": "hourly",
            "response_method": WeatherApiResponse.Hourly,
            "start_param": "start_hour",
            "end_param": "end_hour",
            "weather_metrics_cls": HourlyWeatherMetrics,
        },
        WeatherFrequency.EOD: {
            "field_name": "daily",
            "response_method": WeatherApiResponse.Daily,
            "start_param": "start_date",
            "end_param": "end_date",
            "weather_metrics_cls": DailyWeatherMetrics,
        },
    }

    # Model fields
    location: Union[Location, Tuple[float, float]] = Field(
        ...,
        description=(
            "Location as either a predefined Location enum or a (latitude, longitude)"
            " tuple"
        ),
    )

    elevation: Optional[float] = Field(
        default=None,
        description="The elevation in meters used for statistical downscaling.",
    )

    metrics: List[BaseWeatherMetrics] = Field(
        ..., description="List of weather metrics to retrieve"
    )

    frequency: WeatherFrequency = Field(
        ..., description="Frequency of weather data measurements."
    )

    start_dt: dtm.datetime = Field(
        ...,
        description=(
            "Start time as datetime object. "
            "For daily frequency, time will be rounded to start of day (00:00). "
            "For forecast queries: -92 days to +16 days from today. "
            "For historical queries: 1940 to 2 days ago."
        ),
    )
    end_dt: dtm.datetime = Field(
        ...,
        description=(
            "End time as datetime object. "
            "For daily frequency, time will be rounded to start of next day (00:00). "
            "For forecast queries: -92 days to +16 days from today. "
            "For historical queries: 1940 to 2 days ago."
        ),
    )

    # Validation
    @model_validator(mode="after")
    def validate_metrics_frequency(self) -> "BaseWeatherQueryConfig":
        """Validate that the metrics match the frequency."""
        expected_metric_type = self.FREQUENCY_MAPPINGS[self.frequency][
            "weather_metrics_cls"
        ]  # noqa: E501

        invalid_metrics = [
            metric
            for metric in self.metrics
            if not isinstance(metric, expected_metric_type)
        ]

        if invalid_metrics:
            raise ValueError(
                f"Invalid metrics for frequency {self.frequency}: {invalid_metrics}. "
                f"Expected metrics of type {expected_metric_type.__name__}"
            )

        return self

    @field_validator("frequency")
    @classmethod
    def validate_frequency(cls, value: WeatherFrequency) -> WeatherFrequency:
        """Validate that the frequency is supported for this query type."""
        if value not in cls.supported_frequencies():
            raise ValueError(f"Frequency {value} not supported for {cls.__name__}")
        return value

    # Properties
    @property
    @abstractmethod
    def api_url(self) -> str:
        """Base URL for the API endpoint."""

    @classmethod
    @abstractmethod
    def supported_frequencies(cls) -> List[WeatherFrequency]:
        """List of supported frequencies for this query type."""

    def request_params(self, **kwargs: Any) -> dict:
        """Convert model to request parameters for the Open-Meteo API query."""
        latitude, longitude = self.coordinates
        data = {
            "latitude": str(latitude),
            "longitude": str(longitude),
            **({"elevation": str(self.elevation)} if self.elevation else {}),
        }

        mapping = self.FREQUENCY_MAPPINGS[self.frequency]

        # Handle datetime formatting based on frequency
        if self.frequency == WeatherFrequency.EOD:
            # For daily, round start to beginning of day and end to beginning of next day
            start = self.start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            end = self.end_dt.replace(
                hour=0, minute=0, second=0, microsecond=0
            ) + dtm.timedelta(days=1)
            formatted_dt = "%Y-%m-%d"
        else:
            start = self.start_dt
            end = self.end_dt
            formatted_dt = "%Y-%m-%dT%H:%M"

        data.update(
            {
                mapping["field_name"]: BaseWeatherMetrics.format_list(self.metrics),
                mapping["start_param"]: start.strftime(formatted_dt),
                mapping["end_param"]: end.strftime(formatted_dt),
            }
        )

        return data

    @property
    def response_method(self) -> str:
        """Get response method name to extract data from the Open-Meteo API response."""
        return self.FREQUENCY_MAPPINGS[self.frequency]["response_method"].__name__

    @property
    def coordinates(self) -> Tuple[float, float]:
        """Get the coordinates for the location."""
        if isinstance(self.location, Location):
            return LOCATION_COORDINATES[self.location]
        return self.location

    @property
    @abstractmethod
    def output_prefix(self) -> str:
        """Returns the prefix for the output filename (e.g., 'Forecast/' or 'Historical/')."""

    @property
    def output_filename(self) -> str:
        """
        Generates the output filename to store the results of the API call.

        For predefined locations:
            'Forecast/location=BERLIN/frequency=15_MINUTE/20241201_1200_20241201_1800.csv'
        For custom coordinates:
            'Historical/location=52.52_13.41/frequency=15_MINUTE/20241201_1200_20241201_1800.csv'
        """
        # pylint: disable=no-member

        # Format the location part
        if isinstance(self.location, Location):
            location_str = self.location.value
        else:
            lat, lon = self.location
            location_str = f"{lat}_{lon}"

        # Format the datetime part
        dt_format = "%Y%m%d_%H%M" if not isinstance(self.start_dt, date) else "%Y%m%d"
        start_str = self.start_dt.strftime(dt_format)
        end_str = self.end_dt.strftime(dt_format)

        return (
            f"{self.output_prefix}"
            f"location={location_str}/"
            f"frequency={self.frequency.value}/"
            f"{start_str}_{end_str}.csv"
        )


class WeatherForecastQueryConfig(BaseWeatherQueryConfig):
    """Configuration for fetching weather forecast data.

    Supports forecasts from -92 days to +16 days from today.
    Provides access to minutely (15min), hourly, and daily forecasts.
    See: https://open-meteo.com/en/docs/forecast-api
    """

    @property
    def api_url(self) -> str:
        return "https://api.open-meteo.com/v1/forecast"

    @property
    def output_prefix(self) -> str:
        return "Forecast/"

    @classmethod
    def supported_frequencies(cls) -> List[WeatherFrequency]:
        """List of supported frequencies for this query type."""
        return [
            WeatherFrequency.MINUTELY_15,
            WeatherFrequency.HOURLY,
            WeatherFrequency.EOD,
        ]


class WeatherHistoricalQueryConfig(BaseWeatherQueryConfig):
    """Configuration for fetching historical weather data.

    Supports historical data from 1940 to 2 days ago.
    Provides access to hourly and daily historical measurements.
    See: https://open-meteo.com/en/docs/historical-weather-api
    """

    @property
    def api_url(self) -> str:
        return "https://archive-api.open-meteo.com/v1/archive"

    @property
    def output_prefix(self) -> str:
        return "Historical/"

    @classmethod
    def supported_frequencies(cls) -> List[WeatherFrequency]:
        """List of supported frequencies for this query type."""
        return [
            WeatherFrequency.HOURLY,
            WeatherFrequency.EOD,
        ]

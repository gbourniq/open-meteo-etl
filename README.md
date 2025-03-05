# Open-Meteo ETL Pipeline

## Overview

This project is a Python-based ETL (Extract, Transform, Load) pipeline that integrates with the Open-Meteo API to fetch weather data, validate its quality, and store it in a structured format for further analysis. The pipeline is designed to handle both historical and forecast weather data at various temporal resolutions (e.g., 15-minute, hourly, daily).

Features
* Data Extraction: Fetches weather data from the Open-Meteo API for specific locations and time ranges. Supports multiple frequencies: 15-minutely, hourly, and daily. Handles both historical and forecast data.
* Data Validation: Performs integrity checks to ensure data completeness and quality. Identifies issues like missing values or duplicate timestamps.
* Data Transformation: Converts raw API responses into structured formats (CSV and Parquet). Validates data against predefined schemas for consistency.
* Data Storage: Organizes data into a multi-stage data lake:
* `/data/raw`: Unprocessed CSV files.
* `/data/processed`: Validated Parquet files.
* `/data/transformed`: Placeholder for future transformations.

## Project Structure

```
open_meteo_etl/
├── config.py
├── main.py
├── src/
│   ├── utils.py
│   ├── weather_query.py
│   ├── weather_schema.py
├── data/
└── README.md
```

## Usage

### Prerequisites

1. Python 3.8+
2. Install dependencies: `pip install -r requirements.txt`

### Running the Pipeline

1. Configure your queries in `config.py` by specifying locations, metrics, and time ranges.
2. Run the ETL pipeline: `python main.py`
3. Processed data will be saved in the `/data` directory.

### Example Query Configuration

The following example in `config.py` demonstrates how to fetch historical hourly weather data for Berlin:

```python
from datetime import datetime, timedelta
from open_meteo_etl.src.weather_query import WeatherHistoricalQueryConfig, Location, WeatherFrequency

now = datetime.now()

query = WeatherHistoricalQueryConfig(
    location=Location.BERLIN,
    metrics=["temperature_2m", "precipitation"],
    frequency=WeatherFrequency.HOURLY,
    start_dt=now - timedelta(days=365),
    end_dt=now - timedelta(days=2),
)
```

## Key Design Considerations

1. Scalability: The pipeline supports parallel processing of multiple queries using `ThreadPoolExecutor`. Data is stored in Parquet format for efficient querying and long-term storage.
2. Abstraction: Modular design with separate classes for query configuration (`weather_query.py`) and schema validation (`weather_schema.py`). Easily extendable to support new APIs or metrics.
3. Reliability: Comprehensive logging at each stage of the pipeline. Custom exceptions to handle API errors gracefully.

## Notes

* This project was developed as a personal initiative to explore ETL pipelines and weather APIs.
* The Open-Meteo API does not require an API key, making it accessible for non-commercial use cases.
* Time spent on this project: ~12 hours (including research and implementation).

## Future Enhancements

1. Add support for additional storage backends (e.g., databases or cloud storage).
2. Implement advanced orchestration using tools like Apache Airflow.
3. Enhance validation rules to include statistical anomaly detection.
4. Integrate visualization tools for exploratory analysis of weather trends.

## References

* [Open-Meteo API Documentation](https://open-meteo.com/en/docs)
* [PyPI: openmeteo-requests](https://pypi.org/project/openmeteo-requests/)

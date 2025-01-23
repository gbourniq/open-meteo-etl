"""
Configuration module for Open-Meteo ETL pipelines. Defines atomic weather data query
configurations for API calls, with each config representing a single location,
frequency, and time range.

Atomic configurations enable:
- Parallel ingestion: Each config can be processed independently
- Fault isolation: Failures in one query don't affect others
- Simplified error handling: Retry/resume at the query level
- Flexible scheduling: Configs can be executed on different schedules

Example configs:
- Historical Berlin data at hourly and daily frequencies for past year
- Mount Everest 15-min forecast for next 7 days with specific metrics
"""

from datetime import datetime, timedelta
from typing import List

from etl_open_meteo.src.weather_query import (
    BaseWeatherQueryConfig,
    Location,
    WeatherForecastQueryConfig,
    WeatherFrequency,
    WeatherHistoricalQueryConfig,
)
from etl_open_meteo.src.weather_schema import (
    DailyWeatherMetrics,
    HourlyWeatherMetrics,
    MinutelyWeatherMetrics,
)


def get_queries() -> List[BaseWeatherQueryConfig]:
    """
    Returns a predefined list of weather query configurations.

    These configurations can be executed by ETL orchestration tools like Airflow.
    """
    now = datetime.now()

    queries = [
        # Historical Berlin for all metrics and frequencies
        *[
            WeatherHistoricalQueryConfig(
                location=Location.BERLIN,
                metrics=metrics.get_all(),
                frequency=freq,
                start_dt=now - timedelta(days=365),
                end_dt=now - timedelta(days=2),
            )
            for freq, metrics in zip(
                [WeatherFrequency.EOD, WeatherFrequency.HOURLY],
                [DailyWeatherMetrics, HourlyWeatherMetrics],
            )
        ],
        # Mount Everest forecast 15 minutely for next 7 days
        WeatherForecastQueryConfig(
            location=(27.9881, 86.9250),
            elevation=8848,
            metrics=[
                MinutelyWeatherMetrics.TEMPERATURE_2M,
                MinutelyWeatherMetrics.WIND_SPEED_10M,
                MinutelyWeatherMetrics.WIND_GUSTS_10M,
                MinutelyWeatherMetrics.VISIBILITY,
                MinutelyWeatherMetrics.SNOWFALL,
            ],
            frequency=WeatherFrequency.MINUTELY_15,
            start_dt=now,
            end_dt=now + timedelta(days=7),
        ),
    ]
    return queries


if __name__ == "__main__":
    for query in get_queries():
        print(repr(query))

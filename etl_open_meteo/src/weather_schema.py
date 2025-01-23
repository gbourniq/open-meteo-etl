"""
Schema definitions for weather data from Open-Meteo API.

Provides strongly-typed schemas for different data frequencies:
- MinutelyWeatherMetrics: High-frequency measurements (15-min)
- HourlyWeatherMetrics: Standard hourly observations
- DailyWeatherMetrics: Daily aggregates and statistics

Each metric includes:
- Field name: Matches API response fields
- Arrow data type: For schema validation
- Unit information

Schema Evolution:
- New metrics can be added to appropriate frequency class
- Data types ensure consistency across pipeline
"""

from enum import Enum
from typing import List, Tuple

import pyarrow as pa


class BaseWeatherMetrics(Enum):
    """Base class for weather metrics that defines common functionality
    for schema generation and metric formatting. Each metric is defined as a tuple of
    (field_name, arrow_data_type)."""

    @classmethod
    def get_all(cls) -> List["BaseWeatherMetrics"]:
        """Returns a list of all available weather metrics defined in the enum."""
        return list(cls)

    @classmethod
    def format_list(cls, metrics: List["BaseWeatherMetrics"]) -> str:
        """Formats a list of metrics into a comma-separated string of field names.

        Args:
            metrics: List of weather metrics to format
        Returns:
            Comma-separated string of metric field names
        """
        return ",".join(metric.field_name for metric in metrics)

    @staticmethod
    def get_metadata_schema() -> List[Tuple[str, pa.DataType]]:
        """Returns the common metadata schema fields required for all weather data."""
        return [
            ("obs_timestamp", pa.timestamp("ns", tz="utc")),
            ("latitude", pa.float64()),
            ("longitude", pa.float64()),
            ("elevation", pa.float32()),
            ("model", pa.int64()),
            ("utc_offset_seconds", pa.int32()),
            ("timezone", pa.string()),
            ("timezone_abbreviation", pa.string()),
            ("generation_time_ms", pa.float32()),
            ("ingested_at", pa.timestamp("ns", tz="utc")),
        ]

    @property
    def schema_type(self) -> pa.DataType:
        return self.value[1]

    @property
    def field_name(self) -> str:
        assert isinstance(self.value[0], str)
        return self.value[0]

    @classmethod
    def get_schema(cls) -> List[Tuple[str, pa.DataType]]:
        return BaseWeatherMetrics.get_metadata_schema() + [
            (metric.field_name, metric.schema_type) for metric in cls
        ]


class MinutelyWeatherMetrics(BaseWeatherMetrics):
    """Weather metrics available at minute-level granularity.
    Includes temperature, radiation, precipitation, wind, and other atmospheric
    measurements typically used for high-frequency weather monitoring."""

    # Temperature and Humidity
    TEMPERATURE_2M = ("temperature_2m", pa.float32())
    RELATIVE_HUMIDITY_2M = ("relative_humidity_2m", pa.float32())
    DEW_POINT_2M = ("dew_point_2m", pa.float32())
    APPARENT_TEMPERATURE = ("apparent_temperature", pa.float32())

    # Radiation
    SHORTWAVE_RADIATION = ("shortwave_radiation", pa.float32())
    DIRECT_RADIATION = ("direct_radiation", pa.float32())
    DIRECT_NORMAL_IRRADIANCE = ("direct_normal_irradiance", pa.float32())
    DIFFUSE_RADIATION = ("diffuse_radiation", pa.float32())
    SUNSHINE_DURATION = ("sunshine_duration", pa.float32())

    # Precipitation and Snow
    PRECIPITATION = ("precipitation", pa.float32())
    SNOWFALL = ("snowfall", pa.float32())
    RAIN = ("rain", pa.float32())
    SHOWERS = ("showers", pa.float32())

    # Wind (keeping 10m as standard height)
    WIND_SPEED_10M = ("wind_speed_10m", pa.float32())
    WIND_DIRECTION_10M = ("wind_direction_10m", pa.float32())
    WIND_GUSTS_10M = ("wind_gusts_10m", pa.float32())

    # Other
    LIGHTNING_POTENTIAL = ("lightning_potential", pa.float32())
    FREEZING_LEVEL_HEIGHT = ("freezing_level_height", pa.float32())
    CAPE = ("cape", pa.float32())
    VISIBILITY = ("visibility", pa.float32())
    WEATHER_CODE = ("weather_code", pa.int32())


class HourlyWeatherMetrics(BaseWeatherMetrics):
    """Weather metrics available at hourly granularity.
    Provides a comprehensive set of atmospheric measurements including temperature,
    pressure, cloud cover, wind, radiation, precipitation, soil conditions, and derived
    indices."""

    # Temperature and Humidity
    TEMPERATURE_2M = ("temperature_2m", pa.float32())
    RELATIVE_HUMIDITY_2M = ("relative_humidity_2m", pa.float32())
    DEW_POINT_2M = ("dew_point_2m", pa.float32())
    APPARENT_TEMPERATURE = ("apparent_temperature", pa.float32())

    # Pressure
    SURFACE_PRESSURE = ("surface_pressure", pa.float32())

    # Cloud Cover (keeping mid-level only)
    CLOUD_COVER_MID = ("cloud_cover_mid", pa.float32())

    # Wind (keeping 10m as standard height)
    WIND_SPEED_10M = ("wind_speed_10m", pa.float32())
    WIND_DIRECTION_10M = ("wind_direction_10m", pa.float32())
    WIND_GUSTS_10M = ("wind_gusts_10m", pa.float32())

    # Radiation
    SHORTWAVE_RADIATION = ("shortwave_radiation", pa.float32())
    DIRECT_RADIATION = ("direct_radiation", pa.float32())
    DIRECT_NORMAL_IRRADIANCE = ("direct_normal_irradiance", pa.float32())
    DIFFUSE_RADIATION = ("diffuse_radiation", pa.float32())

    # Precipitation and Snow
    PRECIPITATION = ("precipitation", pa.float32())
    SNOWFALL = ("snowfall", pa.float32())
    RAIN = ("rain", pa.float32())
    SHOWERS = ("showers", pa.float32())
    PRECIPITATION_PROBABILITY = ("precipitation_probability", pa.float32())
    SNOW_DEPTH = ("snow_depth", pa.float32())

    # Soil (keeping representative depths)
    SOIL_TEMPERATURE_18CM = ("soil_temperature_18cm", pa.float32())
    SOIL_MOISTURE_3_TO_9CM = ("soil_moisture_3_to_9cm", pa.float32())

    # Other
    VAPOUR_PRESSURE_DEFICIT = ("vapour_pressure_deficit", pa.float32())
    CAPE = ("cape", pa.float32())
    EVAPOTRANSPIRATION = ("evapotranspiration", pa.float32())
    ET0_FAO_EVAPOTRANSPIRATION = ("et0_fao_evapotranspiration", pa.float32())
    FREEZING_LEVEL_HEIGHT = ("freezing_level_height", pa.float32())
    VISIBILITY = ("visibility", pa.float32())
    WEATHER_CODE = ("weather_code", pa.int32())
    IS_DAY = ("is_day", pa.bool_())


class DailyWeatherMetrics(BaseWeatherMetrics):
    """Weather metrics aggregated at daily granularity.
    Includes daily extremes (min/max), cumulative values (sums), and other daily weather
    statistics useful for daily weather analysis and forecasting."""

    # Temperature
    TEMPERATURE_2M_MAX = ("temperature_2m_max", pa.float32())
    TEMPERATURE_2M_MIN = ("temperature_2m_min", pa.float32())
    APPARENT_TEMPERATURE_MAX = ("apparent_temperature_max", pa.float32())
    APPARENT_TEMPERATURE_MIN = ("apparent_temperature_min", pa.float32())

    # Sun and Radiation
    SUNRISE = ("sunrise", pa.timestamp("ns", tz="utc"))
    SUNSET = ("sunset", pa.timestamp("ns", tz="utc"))
    DAYLIGHT_DURATION = ("daylight_duration", pa.float32())
    SUNSHINE_DURATION = ("sunshine_duration", pa.float32())
    SHORTWAVE_RADIATION_SUM = ("shortwave_radiation_sum", pa.float32())
    UV_INDEX_MAX = ("uv_index_max", pa.float32())
    UV_INDEX_CLEAR_SKY_MAX = ("uv_index_clear_sky_max", pa.float32())

    # Precipitation
    PRECIPITATION_SUM = ("precipitation_sum", pa.float32())
    RAIN_SUM = ("rain_sum", pa.float32())
    SHOWERS_SUM = ("showers_sum", pa.float32())
    SNOWFALL_SUM = ("snowfall_sum", pa.float32())
    PRECIPITATION_HOURS = ("precipitation_hours", pa.float32())
    PRECIPITATION_PROBABILITY_MAX = ("precipitation_probability_max", pa.float32())

    # Wind
    WIND_SPEED_10M_MAX = ("wind_speed_10m_max", pa.float32())
    WIND_GUSTS_10M_MAX = ("wind_gusts_10m_max", pa.float32())
    WIND_DIRECTION_10M_DOMINANT = ("wind_direction_10m_dominant", pa.float32())

    # Other
    WEATHER_CODE = ("weather_code", pa.int32())
    ET0_FAO_EVAPOTRANSPIRATION = ("et0_fao_evapotranspiration", pa.float32())


FREQUENCY_TO_SCHEMA = {
    "15_MINUTE": MinutelyWeatherMetrics.get_schema(),
    "1_HOUR": HourlyWeatherMetrics.get_schema(),
    "EOD": DailyWeatherMetrics.get_schema(),
}

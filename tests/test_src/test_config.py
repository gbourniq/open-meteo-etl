"""Unit tests for config module."""

from contextlib import nullcontext as does_not_raise
from datetime import datetime, timedelta
from typing import Any, List

import pytest

from etl_open_meteo.src.weather_query import (
    Location,
    WeatherForecastQueryConfig,
    WeatherFrequency,
    WeatherHistoricalQueryConfig,
)
from etl_open_meteo.src.weather_schema import (
    BaseWeatherMetrics,
    DailyWeatherMetrics,
    HourlyWeatherMetrics,
    MinutelyWeatherMetrics,
)


class TestWeatherQueryConfig:
    """Tests for weather query configurations."""

    @pytest.fixture
    def historical_query(self) -> WeatherHistoricalQueryConfig:
        """Setup historical query config."""
        now = datetime(2024, 1, 1, 12, 0)
        return WeatherHistoricalQueryConfig(
            location=Location.BERLIN,
            metrics=[DailyWeatherMetrics.TEMPERATURE_2M_MAX],
            frequency=WeatherFrequency.EOD,
            start_dt=now - timedelta(days=7),
            end_dt=now,
        )

    @pytest.fixture
    def forecast_query(self) -> WeatherForecastQueryConfig:
        """Setup forecast query config."""
        now = datetime(2024, 1, 1, 12, 0)
        return WeatherForecastQueryConfig(
            location=(27.9881, 86.9250),
            elevation=8848,
            metrics=[MinutelyWeatherMetrics.TEMPERATURE_2M],
            frequency=WeatherFrequency.MINUTELY_15,
            start_dt=now,
            end_dt=now + timedelta(days=1),
        )

    @pytest.mark.parametrize(
        "query_fixture,expected_url",
        [
            ("historical_query", "https://archive-api.open-meteo.com/v1/archive"),
            ("forecast_query", "https://api.open-meteo.com/v1/forecast"),
        ],
    )
    def test_api_url(
        self,
        query_fixture: str,
        expected_url: str,
        request: pytest.FixtureRequest,
    ) -> None:
        """Test API URL for different query types."""
        # Given
        query = request.getfixturevalue(query_fixture)

        # When
        result = query.api_url

        # Then
        assert result == expected_url

    @pytest.mark.parametrize(
        "query_fixture,expected_prefix",
        [
            ("historical_query", "Historical/"),
            ("forecast_query", "Forecast/"),
        ],
    )
    def test_output_prefix(
        self,
        query_fixture: str,
        expected_prefix: str,
        request: pytest.FixtureRequest,
    ) -> None:
        """Test output prefix for different query types."""
        # Given
        query = request.getfixturevalue(query_fixture)

        # When
        result = query.output_prefix

        # Then
        assert result == expected_prefix

    @pytest.mark.parametrize(
        "query_fixture,expected_params",
        [
            (
                "historical_query",
                {
                    "latitude": "52.52",
                    "longitude": "13.41",
                    "daily": "temperature_2m_max",
                },
            ),
            (
                "forecast_query",
                {
                    "latitude": "27.9881",
                    "longitude": "86.925",
                    "elevation": "8848.0",
                    "minutely_15": "temperature_2m",
                },
            ),
        ],
    )
    def test_request_params(
        self,
        query_fixture: str,
        expected_params: dict[str, str],
        request: pytest.FixtureRequest,
    ) -> None:
        """Test request parameters for different query types."""
        # Given
        query = request.getfixturevalue(query_fixture)

        # When
        params = query.request_params()

        # Then
        for key, value in expected_params.items():
            assert params[key] == value

    @pytest.mark.parametrize(
        "query_fixture,expected_method",
        [
            ("historical_query", "Daily"),
            ("forecast_query", "Minutely15"),
        ],
    )
    def test_response_method(
        self,
        query_fixture: str,
        expected_method: str,
        request: pytest.FixtureRequest,
    ) -> None:
        """Test response method for different query types."""
        # Given
        query = request.getfixturevalue(query_fixture)

        # When
        result = query.response_method

        # Then
        assert result == expected_method

    @pytest.mark.parametrize(
        "query_fixture,expected_repr",
        [
            (
                "historical_query",
                (
                    "WeatherHistoricalQueryConfig:\n  latitude: 52.52\n  longitude:"
                    " 13.41\n  daily: temperature_2m_max\n  start_date: 2023-12-25\n "
                    " end_date: 2024-01-02\n  API URL:"
                    " https://archive-api.open-meteo.com/v1/archive\n  Output:"
                    " Historical/location=BERLIN/frequency=EOD/20231225_20240101.csv"
                ),
            ),
            (
                "forecast_query",
                (
                    "WeatherForecastQueryConfig:\n  latitude: 27.9881\n  longitude:"
                    " 86.925\n  elevation: 8848.0\n  minutely_15: temperature_2m\n "
                    " start_minutely_15: 2024-01-01T12:00\n  end_minutely_15:"
                    " 2024-01-02T12:00\n  API URL:"
                    " https://api.open-meteo.com/v1/forecast\n  Output:"
                    " Forecast/location=27.9881_86.925/frequency=15_MINUTE/20240101_20240102.csv"
                ),
            ),
        ],
    )
    def test_query_repr(
        self, query_fixture: str, expected_repr: str, request: pytest.FixtureRequest
    ) -> None:
        """Test string representation of query configs."""
        # Given
        query = request.getfixturevalue(query_fixture)

        # When
        result = repr(query)

        # Then
        assert result == expected_repr

    @pytest.mark.parametrize(
        "query_fixture,expected_frequencies",
        [
            (
                "historical_query",
                [WeatherFrequency.HOURLY, WeatherFrequency.EOD],
            ),
            (
                "forecast_query",
                [
                    WeatherFrequency.MINUTELY_15,
                    WeatherFrequency.HOURLY,
                    WeatherFrequency.EOD,
                ],
            ),
        ],
    )
    def test_supported_frequencies(
        self,
        query_fixture: str,
        expected_frequencies: list[WeatherFrequency],
        request: pytest.FixtureRequest,
    ) -> None:
        """Test supported frequencies for different query types."""
        # Given
        query: WeatherForecastQueryConfig | WeatherHistoricalQueryConfig = (
            request.getfixturevalue(query_fixture)
        )

        # When
        result = query.supported_frequencies()

        # Then
        assert result == expected_frequencies


class TestWeatherQueryValidation:
    """Tests for weather query validation logic."""

    @pytest.mark.parametrize(
        "query_class,frequency,metrics,expectation",
        [
            (
                WeatherHistoricalQueryConfig,
                WeatherFrequency.MINUTELY_15,
                [MinutelyWeatherMetrics.TEMPERATURE_2M],
                pytest.raises(ValueError, match="Frequency .* not supported"),
            ),
            (
                WeatherHistoricalQueryConfig,
                WeatherFrequency.HOURLY,
                [HourlyWeatherMetrics.TEMPERATURE_2M],
                does_not_raise(),
            ),
            (
                WeatherForecastQueryConfig,
                WeatherFrequency.MINUTELY_15,
                [MinutelyWeatherMetrics.TEMPERATURE_2M],
                does_not_raise(),
            ),
            (
                WeatherForecastQueryConfig,
                WeatherFrequency.HOURLY,
                [HourlyWeatherMetrics.TEMPERATURE_2M],
                does_not_raise(),
            ),
        ],
    )
    def test_validate_frequency(
        self,
        query_class: WeatherHistoricalQueryConfig | WeatherForecastQueryConfig,
        frequency: WeatherFrequency,
        metrics: List[BaseWeatherMetrics],
        expectation: Any,
    ) -> None:
        """Test frequency validation for different query types."""
        # Given
        now = datetime(2024, 1, 1, 12, 0)
        config = {
            "location": Location.BERLIN,
            "metrics": metrics,
            "frequency": frequency,
            "start_dt": now,
            "end_dt": now + timedelta(days=1),
        }

        # When/Then
        with expectation:
            _ = query_class(**config)  # type: ignore

    def test_validate_metrics(self) -> None:
        """Test validation of mismatched frequencies and metrics combinations."""
        # Given
        now = datetime(2024, 1, 1, 12, 0)
        config = {
            "location": Location.BERLIN,
            "metrics": [
                DailyWeatherMetrics.TEMPERATURE_2M_MAX,
                HourlyWeatherMetrics.TEMPERATURE_2M,
            ],
            "frequency": WeatherFrequency.EOD,
            "start_dt": now,
            "end_dt": now + timedelta(days=1),
        }

        # When/Then
        with pytest.raises(
            ValueError,
            match=(
                "Invalid metrics for frequency WeatherFrequency\\.EOD:"
                " \\[<HourlyWeatherMetrics\\.TEMPERATURE_2M: \\('temperature_2m',"
                " DataType\\(float\\)\\)>\\]\\. Expected metrics of type"
                " DailyWeatherMetrics"
            ),
        ):
            _ = WeatherForecastQueryConfig(**config)  # type: ignore

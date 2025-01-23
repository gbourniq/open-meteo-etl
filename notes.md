# Open Meteo ETL

## Get started

### Set up the environment

1. Verify Python 3.13 installation:
```bash
python3.13 --version
```

2. Initialize the environment using the Makefile:
```bash
make setup
source .venv/bin/activate
```

Alternatively, set up manually:
```bash
python3.13 -m venv .venv
source .venv/bin/activate
python3.13 -m pip install poetry==2.0.1
poetry --version
poetry install --with dev
```

### Run the main script

```bash
python open_meteo_etl/main.py
```

The script will:
- Save raw data to `./data/raw`
- Save processed data to `./data/processed`
- Use weather data configurations from `open_meteo_etl/config.py`

### Run the tests

```bash
make test
```

## Code Structure

```
├── Makefile                    # Automation for project setup, linting, and testing
├── data/
│   ├── processed/              # Parsed and validated Parquet files
│   └── raw/                    # Original API responses in CSV format
├── open_meteo_etl/
│   ├── config.py               # ETL configuration
│   ├── main.py                 # Pipeline orchestration and execution
│   └── src/
│       ├── utils.py
│       ├── weather_query.py    # Forecast and historical query builders
│       └── weather_schema.py   # Data validation schemas and weather metrics
└── tests/
    └── test_src/
        └── test_config.py      # ETL query config tests
```

> For more detailed information about each module's functionality, please refer to their respective docstrings.

## Extensions & Production Considerations

### Production Orchestration

The ETL pipeline could be orchestrated using either AWS Step Functions + Lambda or Apache Airflow:

- **Orchestration steps**:

  - Pipeline steps (as implemented in `main.py`):
    1. Raw data ingestion (CSV)
    2. Data validation and quality checks
    3. Parquet conversion with schema enforcement
    4. Future transformations (downsampling, normalization) ideally in a data warehouse
  - Each ETL configuration (defined by location and frequency) can be processed independently, enabling parallel execution for improved scalability

- **ETL Configuration**:
  - Defines atomic weather query configurations in `open_meteo_etl/config.py`
  - Each config represents a single location, frequency, and time range
  - Supports both predefined cities and custom coordinates
  - Example configs: Berlin historical data, Mount Everest forecasts

### Code Abstractions

The codebase implements several key abstractions for flexibility:

- **Query Building** (`weather_query.py`):

  - Abstract base class `BaseWeatherQueryConfig` for query configuration
  - Specialized classes for historical and forecast data
  - Strong typing and parameter validation with Pydantic models
  - Standardized output paths in data lake structure

- **Data Validation** (`weather_schema.py`):
  - Enum-based metric definitions with Arrow data types
  - Separate schemas for minutely, hourly, and daily data
  - Common metadata fields across all frequencies in base class
  - Schema evolution support for new metrics

These abstractions enable:

- Easy addition of new data sources and metrics
- Consistent data quality and schema validation
- Clear separation of concerns between components
- Type safety throughout the pipeline
- Simplified testing and maintenance

## Personal Notes

#### Use of AI Tools

I utilised Cursor (with Claude 3.5) throughout the development process to:

- Assist with code refactoring and optimization
- Generate and improve docstrings
- Debug and troubleshoot implementation issues

#### Time Investment

Estimated time spent on implementation: 8 hours

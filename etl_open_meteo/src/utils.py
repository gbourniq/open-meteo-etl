import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Final

import openmeteo_requests
import requests_cache
import urllib3
from retry_requests import retry

# Create timestamp for logging
SCRIPT_TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")

# Create cache directory if it doesn't exist
CACHE_DIR: Final[Path] = Path(__file__).parents[2] / ".cache"
CACHE_DIR.mkdir(exist_ok=True)

# Data lake directory structure:
# - raw: original data as received from the source
# - processed: intermediate processing results
# - transformed: final cleaned and transformed data (e.g., downsampled, normalized)
DATALAKE_ROOT_DIR: Final[Path] = Path(__file__).parents[2] / "data"
RAW_DATA_DIR: Final[Path] = DATALAKE_ROOT_DIR / "raw"
PROCESSED_DATA_DIR: Final[Path] = DATALAKE_ROOT_DIR / "processed"
TRANSFORMED_DATA_DIR: Final[Path] = DATALAKE_ROOT_DIR / "transformed"


def setup_logging() -> logging.Logger:
    """Configure and return a logger with stream handler.

    The log level can be set via LOG_LEVEL environment variable (INFO or DEBUG).
    Defaults to INFO if not set or invalid.

    Returns:
        logging.Logger: Configured logger instance
    """
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    if log_level not in ("INFO", "DEBUG"):
        log_level = "INFO"

    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
        ],
    )
    return logging.getLogger(__name__)


def setup_api_client() -> openmeteo_requests.Client:
    """Configure and return an Open-Meteo API client with caching and retry logic.

    Features:
    - Caches responses for 1 hour to reduce API calls
    - Implements retry logic (5 attempts with exponential backoff)

    Returns:
        openmeteo_requests.Client: Configured API client
    """
    # Disable SSL verification warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Open-meteo client setup
    cache_session = requests_cache.CachedSession(
        str(CACHE_DIR / "open_meteo_cache"), expire_after=3600
    )
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)

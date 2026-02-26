import requests
import logging
from datetime import datetime, timedelta, timezone
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log
)
from config.config_loader import (
    api_base_url as API_BASE_URL,
    hourly_variables as HOURLY_VARIABLES
)

logger = logging.getLogger(__name__)


class APIError(Exception):
    """
    Raised when the API returns an unexpected response.
    Distinct from network errors (requests.RequestException)
    because they require different retry behavior.

    APIError with a 400 status → don't retry, request is malformed
    APIError with a 429 status → retry after waiting
    requests.RequestException  → retry, likely a transient network issue
    """
    def __init__(self, message: str, status_code: int = None):
        super().__init__(message)
        self.status_code = status_code


def _is_retryable_error(exception: Exception) -> bool:
    """
    Determines whether an exception should trigger a retry.

    Retryable:
        - Network errors (timeouts, connection failures)
        - HTTP 429 Too Many Requests
        - HTTP 500/502/503/504 Server errors

    Not retryable:
        - HTTP 400 Bad Request (our request is wrong, retrying won't help)
        - HTTP 404 Not Found (resource doesn't exist)
        - HTTP 401/403 Auth errors (credentials wrong, retrying won't help)
    """
    if isinstance(exception, requests.RequestException):
        return True

    if isinstance(exception, APIError):
        retryable_codes = {429, 500, 502, 503, 504}
        return exception.status_code in retryable_codes

    return False


@retry(
    # Stop after 3 total attempts (1 original + 2 retries)
    stop=stop_after_attempt(3),

    # Exponential backoff: wait 2s, then 4s, then 8s between attempts
    # max=10 caps the wait so it never exceeds 10 seconds
    wait=wait_exponential(multiplier=2, min=2, max=10),

    # Only retry on errors we've identified as transient
    retry=retry_if_exception_type((requests.RequestException, APIError)),

    # Log a warning before each retry attempt so we can see it in logs
    before_sleep=before_sleep_log(logger, logging.WARNING),

    # Don't wrap the exception — let it propagate with original context
    reraise=True
)
def _fetch_with_retry(url: str, params: dict) -> dict:
    """
    Makes a single API request with automatic retry on transient failures.
    Decorated with @retry — tenacity handles the retry loop automatically.

    Separated from fetch_city so the retry decorator wraps only
    the HTTP call, not the city loop or response validation.
    """
    logger.debug(f"Requesting: {url} params={params}")

    response = requests.get(url, params=params, timeout=30)

    # Check status before attempting to parse body
    # Non-200 responses may not contain valid JSON
    if response.status_code != 200:
        raise APIError(
            f"API returned status {response.status_code}: {response.text[:200]}",
            status_code=response.status_code
        )

    data = response.json()

    # Validate response structure
    if "hourly" not in data:
        raise APIError(
            "Response missing 'hourly' key — API structure may have changed"
        )

    if not data["hourly"].get("time"):
        raise APIError(
            "Hourly time array is empty — API returned no data for this period"
        )

    return data


def fetch_city(city: dict, target_date: datetime.date = None) -> dict | None:
    """
    Fetches one day of hourly weather data for a single city.

    Args:
        city: dict with keys name, lat, lon, timezone
        target_date: date to fetch. Defaults to yesterday if not provided.

    Returns:
        dict with keys:
            - city_name: str
            - date: str (YYYY-MM-DD)
            - raw_response: dict (complete API response)
        Returns None if all retry attempts fail.

    Why return None instead of raising?
        Because fetch_all_cities handles the failure gracefully.
        Raising would require the caller to wrap every call in try/except.
        None is an explicit signal: "this city failed, skip it."
    """
    if target_date is None:
        target_date = datetime.now(timezone.utc).date() - timedelta(days=1)

    date_str = target_date.strftime("%Y-%m-%d")

    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": city["timezone"],
        "start_date": date_str,
        "end_date": date_str,
    }

    try:
        logger.info(f"Fetching {city['name']} for {date_str}")
        raw_response = _fetch_with_retry(API_BASE_URL, params)

        logger.info(
            f"Successfully fetched {city['name']} — "
            f"{len(raw_response['hourly']['time'])} hours of data"
        )

        return {
            "city_name": city["name"],
            "date": date_str,
            "raw_response": raw_response
        }

    except Exception as e:
        # All retry attempts exhausted or non-retryable error
        # Log with full context but do not re-raise
        # Returning None signals failure to fetch_all_cities
        logger.error(
            f"Failed to fetch {city['name']} after all retries: "
            f"{type(e).__name__}: {e}"
        )
        return None


def fetch_all_cities(target_date: datetime.date = None) -> tuple[list, list]:
    """
    Fetches weather data for all configured cities sequentially.
    One city failing never stops others from being processed.

    Returns:
        - successful: list of result dicts from fetch_city
        - failed: list of city names that could not be fetched

    Why return both instead of just successful?
        The caller (main.py) needs the failed list to log the
        pipeline summary accurately and alert if too many cities fail.
    """
    from config.config_loader import cities as CITIES

    if target_date is None:
        target_date = datetime.now(timezone.utc).date() - timedelta(days=1)

    successful = []
    failed = []

    logger.info(
        f"Starting ingestion for {len(CITIES)} cities "
        f"— date: {target_date}"
    )

    for city in CITIES:
        result = fetch_city(city, target_date)

        if result is not None:
            successful.append(result)
        else:
            failed.append(city["name"])
            # continue is implicit here — loop proceeds regardless

    logger.info(
        f"Ingestion complete — "
        f"successful: {len(successful)}/{len(CITIES)}, "
        f"failed: {failed if failed else 'none'}"
    )

    return successful, failed
import requests
import logging
from datetime import datetime, timedelta, timezone
from tenacity import (
    retry, stop_after_attempt, wait_exponential,
    retry_if_exception
)
from pathlib import Path
import sys 
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.config_loader import (
    api_base_url, hourly_variables
)

logger = logging.getLogger(__name__)

class APIError(Exception):
    """
    Raised when the API return an unexpected response.
    Distinct from network errors (requests.RequestException)
    because they require different retry behavior
    
    APIError with a 400 status -> don't retry, request is malformed
    APIError with a 429 -> retry after waiting
    requests.RequesException -> retry, likely a transient network issue
    """
    def __init__(self, message: str, status_code: int = None):
        super().__init__(message)
        self.status_code = status_code

    @staticmethod
    def is_retryable_error(exception: Exception) -> bool:
        """
        Determines whether an exception should trigger a retry
        
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

    # Exponential backoff: wait 2s, 4s, then 8s between attempts
    # max=10 caps the wait so it never exceeds 10 seconds
    wait=wait_exponential(multiplier=2, min=2, max=10),

    # ONly retry on erros we have identified as transient
    retry=retry_if_exception(APIError.is_retryable_error),

    # Don't wrap the exception - let it propagate with original context
    reraise=True
)
def fetch_with_retry(url: str, params: dict) -> dict:
    """
    Makes a single API request with automatic retry on transient 
    failures. Decorated with @retry - tenacity handles the retry loop automatically

    Separated from fetch_city so the retry decorator wraps only the HTTP call,
    not the city loop or response validation
    """
    logger.debug(f"Requesting: {url} params={params}")

    response = requests.get(url, params=params, timeout=30)

    # check status before attempting to parse body
    # Non 200 responses may not contain valid json
    if response.status_code != 200:
        raise APIError(
            f"API returned status {response.status_code} : {response.text[:200]}", status_code=response.status_code
        )

    data = response.json()

    # validate response structure
    if "hourly" not in data:
        raise APIError(
            "Response missing 'hourly' key - API structure may have changed"
        )
    
    if not data["hourly"].get("time"):
        raise APIError(
            "Hourly time array is empty - API returned no data for this period"
        )
    
    return data

def fetch_city(city: dict, target_date: datetime.date = None) -> dict | None:
    """
    Fetches one day of hourly weather data for a single city.
    
    Args:
        city: dict with key name, lat, lon, timezone
        target_date: date to fetch, defaults to yesterday if not provided
        
    Returns:
        dict with keys:
            - city_name: str
            - date: str(YYYY-MM-DD)
            - raw_response: dict(complete API response)
            
        Returns None if all retry attempts fail
    """
    if target_date is None:
        target_date = datetime.now(timezone.utc).date - timedelta(days=1)

    date_str = target_date.strftime("%Y-%m-%d")

    params = {
        "latitude" : city['lat'],
        "longitude" : city["lon"],
        "hourly" : ",".join(hourly_variables),
        "start_date": date_str,
        "end_date" : date_str
    }

    try:
        logger.info(f"Fetching {city['name']} for {date_str}")
        raw_response = fetch_with_retry(api_base_url, params)

        logger.info(
            f"Successfully fetched {city['name']} - {len(raw_response['hourly']['time'])} hours of data"
        )

        return {
            "city_name" : city['name'],
            "date" : date_str,
            "raw_response" : raw_response
        }
    
    except Exception as e:
        # All retry attempts exhausted or non-retryable error
        # Log with full context but do not re-raise
        # Returning None signals failure to fetch_all_cities
        logger.error(
            f"Failed to fetch {city['name']} after all retries: "
            f"{type(e).__name__}: {e}"
        )

def fetch_all_cities(target_date: datetime.date = None) -> tuple[list, list]:
    """
    Fetches weather data for all configured cities sequentially.
    One city failing never stops others from being processed
    
    Returns:
        - successful: list of result dicts from fetch_city
        - failed: list of city names that could not be fetched
    """
    from config.config_loader import cities

    if target_date is None:
        target_date = datetime.utcnow().date() - timedelta(days=1)

    successful = []
    failed = []

    logger.info(
        f"Starting ingestion for {len(cities)} cities "
        f"- date: {target_date}"
    )

    for city in cities:
        result = fetch_city(city, target_date)

        if result is not None:
            successful.append(result)

        else:
            failed.append(city["name"])
            # continue is implicit here - loop proceeds regardless

    logger.info(
        f"Ingestion complete - "
        f"successful: {len(successful) / len(cities)}, "
        f"failed: {failed if failed else 'None'}"
    )

    return successful, failed

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    successful, failed = fetch_all_cities()

    print(f"Succesful: {len(successful)}")
    for r in successful:
        print(f"{r['city_name']} - {len(r['raw_response']['hourly']['time'])} hours")

    print(f"Failed: {failed if failed else 'none'}")
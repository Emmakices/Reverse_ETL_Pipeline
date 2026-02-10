"""API extraction logic with pagination, retry, and rate limiting."""

from __future__ import annotations

import logging
import time
from datetime import timedelta

import requests
from requests.adapters import HTTPAdapter
from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from urllib3.util.retry import Retry

from extractor.config import ExtractorConfig
from extractor.exceptions import APIConnectionError, APIResponseError, APITimeoutError
from extractor.logging_utils import get_logger, log_operation

logger = get_logger(__name__)


def create_http_session(config: ExtractorConfig) -> requests.Session:
    """Create a requests session with retry configuration."""
    session = requests.Session()

    retry_strategy = Retry(
        total=config.api_max_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        backoff_factor=1,
    )

    adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    return session


@retry(
    retry=retry_if_exception_type((requests.exceptions.ConnectionError,)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
def fetch_page(
    session: requests.Session,
    url: str,
    params: dict,
    headers: dict,
    timeout: int,
) -> dict:
    """Fetch a single page from the API with retry logic."""
    try:
        response = session.get(url, params=params, headers=headers, timeout=timeout)
    except requests.exceptions.Timeout as e:
        raise APITimeoutError(
            f"API request timed out after {timeout}s",
            details={"url": url, "params": params},
        ) from e
    except requests.exceptions.ConnectionError as e:
        raise APIConnectionError(f"Failed to connect to API: {e}", details={"url": url}) from e

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise APIResponseError(
            f"API returned error status {response.status_code}",
            details={"url": url, "status_code": response.status_code, "response": response.text[:500]},
        ) from e

    try:
        return response.json()
    except ValueError as e:
        raise APIResponseError("API returned invalid JSON", details={"url": url}) from e


def fetch_events(
    config: ExtractorConfig,
    session: requests.Session | None = None,
) -> list[dict]:
    """
    Fetch events from the API for the configured week with pagination.
    Note: API filters with: event_time >= start_time AND event_time < end_time (end exclusive)
    """
    if session is None:
        session = create_http_session(config)

    url = f"{config.api_base_url}/datasets/ecom_events"

    start_time = f"{config.week_start.isoformat()}T00:00:00"
    end_date = config.week_end + timedelta(days=1)
    end_time = f"{end_date.isoformat()}T00:00:00"

    headers = {
        "X-API-KEY": config.api_key,
        "Accept": "application/json",
    }

    all_events: list[dict] = []
    page = 1
    page_size = min(config.api_page_size, 1000)
    rate_limit_delay = config.api_rate_limit_delay
    total_expected: int | None = None

    with log_operation(
        logger,
        "api_fetch_all_pages",
        url=url,
        start_time=start_time,
        end_time=end_time,
        page_size=page_size,
    ):
        while True:
            params = {
                "start_time": start_time,
                "end_time": end_time,
                "page": page,
                "page_size": page_size,
            }

            logger.info(f"Fetching page {page}", extra={"url": url, "params": params})

            response_data = fetch_page(
                session=session,
                url=url,
                params=params,
                headers=headers,
                timeout=config.api_timeout_seconds,
            )

            if isinstance(response_data, dict):
                if "data" in response_data:
                    page_events = response_data["data"]
                elif "events" in response_data:
                    page_events = response_data["events"]
                elif "results" in response_data:
                    page_events = response_data["results"]
                else:
                    page_events = response_data.get("items", [])
            elif isinstance(response_data, list):
                page_events = response_data
            else:
                raise APIResponseError(f"Unexpected response type: {type(response_data).__name__}")

            if not isinstance(page_events, list):
                raise APIResponseError(f"Events data is not a list (got {type(page_events).__name__})")

            logger.info(
                f"Fetched page {page}",
                extra={"records_in_page": len(page_events), "total_so_far": len(all_events) + len(page_events)},
            )

            all_events.extend(page_events)

            if isinstance(response_data, dict) and "meta" in response_data:
                meta = response_data["meta"]
                if "total" in meta:
                    total_expected = meta["total"]
                    logger.debug(f"API reports total records: {total_expected}")

            if total_expected is not None and len(all_events) >= total_expected:
                logger.info(
                    f"Fetched all records ({len(all_events)}/{total_expected})",
                    extra={"total_fetched": len(all_events), "total_expected": total_expected},
                )
                break

            if len(page_events) == 0:
                logger.info("Received empty page - pagination complete")
                break

            if len(page_events) < page_size:
                logger.info("Received partial page - pagination complete")
                break

            page += 1
            time.sleep(rate_limit_delay)

        logger.info(
            "Fetched all events from API",
            extra={"total_rows": len(all_events), "total_pages": page},
        )

        return all_events


def check_api_health(config: ExtractorConfig) -> bool:
    """Check if the API is reachable and responding."""
    session = create_http_session(config)
    headers = {"X-API-KEY": config.api_key}

    try:
        response = session.get(
            f"{config.api_base_url}/datasets/ecom_events",
            params={"page": 1, "page_size": 1},
            headers=headers,
            timeout=10,
        )
        return response.status_code < 500
    except Exception as e:
        logger.warning(f"API health check failed: {e}")
        return False

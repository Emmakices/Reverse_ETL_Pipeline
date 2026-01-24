#!/usr/bin/env python3
"""
Event Extraction Pipeline (Single-File Version)

Airflow-friendly checkpoint/resume:
- Each run processes ONE week only.
- If you run with --use-checkpoint, it picks the next week from state/extractor_state.json
- After a successful run, it writes the next week to the state file.

NEW (Reject handling + run status):
- Writes VALID events to events.parquet
- Writes REJECTED rows to rejects.parquet (with error metadata)
- Run status:
    - SUCCESS (0 rejects)
    - SUCCESS_WITH_REJECTS (rejects exist but within threshold)
    - FAILED (hard failure OR reject rate above threshold when strict validation is on)
- Checkpoint advances only when run status is SUCCESS or SUCCESS_WITH_REJECTS (within threshold)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd
import requests
from azure.core.exceptions import AzureError, ClientAuthenticationError, ServiceRequestError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from requests.adapters import HTTPAdapter
from tenacity import before_sleep_log, retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from urllib3.util.retry import Retry


# =============================================================================
# EXCEPTIONS
# =============================================================================


class ExtractorError(Exception):
    """Base exception for all extractor-related errors."""

    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class ConfigurationError(ExtractorError):
    """Raised when configuration is missing or invalid."""
    pass


class APIError(ExtractorError):
    """Base exception for API-related errors."""
    pass


class APIConnectionError(APIError):
    """Raised when unable to connect to the API."""
    pass


class APITimeoutError(APIError):
    """Raised when API request times out."""
    pass


class APIResponseError(APIError):
    """Raised when API returns an unexpected response."""
    pass


class ValidationError(ExtractorError):
    """Raised when data validation fails."""
    pass


class SchemaValidationError(ValidationError):
    """Raised when data doesn't match expected schema or reject rate too high."""
    pass


class DataQualityError(ValidationError):
    """Raised when data quality checks fail."""
    pass


class StorageError(ExtractorError):
    """Base exception for storage-related errors."""
    pass


class LocalStorageError(StorageError):
    """Raised when local file operations fail."""
    pass


class AzureStorageError(StorageError):
    """Raised when Azure storage operations fail."""
    pass


class AuthenticationError(ExtractorError):
    """Raised when authentication fails."""
    pass


# =============================================================================
# LOGGING
# =============================================================================


class JSONFormatter(logging.Formatter):
    """Format log records as JSON for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if hasattr(record, "correlation_id"):
            log_data["correlation_id"] = record.correlation_id

        standard_attrs = {
            "name", "msg", "args", "created", "filename", "funcName",
            "levelname", "levelno", "lineno", "module", "msecs", "pathname",
            "process", "processName", "relativeCreated", "stack_info",
            "exc_info", "exc_text", "thread", "threadName", "taskName",
            "message", "correlation_id",
        }

        for key, value in record.__dict__.items():
            if key not in standard_attrs and not key.startswith("_"):
                if isinstance(value, datetime):
                    log_data[key] = value.isoformat()
                elif hasattr(value, "__dict__"):
                    log_data[key] = str(value)
                else:
                    try:
                        json.dumps(value)
                        log_data[key] = value
                    except (TypeError, ValueError):
                        log_data[key] = str(value)

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, default=str)


class CorrelationIdFilter(logging.Filter):
    """Add correlation ID to all log records."""

    _correlation_id: str | None = None

    @classmethod
    def set_correlation_id(cls, correlation_id: str) -> None:
        cls._correlation_id = correlation_id

    @classmethod
    def generate_correlation_id(cls) -> str:
        correlation_id = str(uuid.uuid4())[:8]
        cls._correlation_id = correlation_id
        return correlation_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = self._correlation_id
        return True


def setup_logging(level: str = "INFO", json_format: bool = True) -> None:
    """Configure logging for the application."""
    root_logger = logging.getLogger()
    root_logger.setLevel(level.upper())
    root_logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level.upper())

    if json_format:
        handler.setFormatter(JSONFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    handler.addFilter(CorrelationIdFilter())
    root_logger.addHandler(handler)

    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name."""
    return logging.getLogger(name)


@contextmanager
def log_operation(logger: logging.Logger, operation: str, **context: Any):
    """Context manager for logging operation start/end with timing."""
    start_time = perf_counter()
    logger.info(f"Starting {operation}", extra=context)

    try:
        yield
    except Exception as e:
        duration_ms = int((perf_counter() - start_time) * 1000)
        logger.error(
            f"Failed {operation}",
            extra={**context, "duration_ms": duration_ms, "error": str(e)},
            exc_info=True,
        )
        raise
    else:
        duration_ms = int((perf_counter() - start_time) * 1000)
        logger.info(f"Completed {operation}", extra={**context, "duration_ms": duration_ms})


logger = get_logger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================


class ExtractorConfig(BaseSettings):
    """Configuration for the event extraction pipeline."""

    model_config = SettingsConfigDict(
        env_file="config.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Required fields
    api_base_url: str = Field(..., description="Base URL of the events API")
    api_key: str = Field(..., description="API key for authentication")
    week_start: date = Field(..., description="Start date of batch week")
    week_end: date = Field(..., description="End date of batch week")

    # Reject policy
    reject_threshold_pct: float = Field(default=5.0, ge=0.0, le=100.0, description="Max allowed rejected-row percentage")

    # Azure storage (optional)
    storage_account: str | None = Field(default=None)
    container: str | None = Field(default=None)
    adls_base_path: str = Field(default="reverse_etl/raw_events")
    adls_rejects_base_path: str = Field(default="reverse_etl/rejected_events")

    # Local storage
    local_data_dir: Path = Field(default=Path("data/raw_events"))
    local_rejects_dir: Path = Field(default=Path("data/rejected_events"))

    # Checkpoint/state (for Airflow resume)
    state_dir: Path = Field(default=Path("state"))
    state_file_name: str = Field(default="extractor_state.json")

    # API settings
    api_timeout_seconds: int = Field(default=60, ge=1, le=300)
    api_max_retries: int = Field(default=3, ge=1, le=10)
    api_page_size: int = Field(default=1000, ge=1, le=1000, description="Records per API page (max 1000)")
    api_rate_limit_delay: float = Field(default=2.0, ge=0, description="Delay between API requests in seconds")

    # Logging
    log_level: str = Field(default="INFO")

    @field_validator("api_base_url")
    @classmethod
    def validate_api_url(cls, v: str) -> str:
        v = v.rstrip("/")
        if not v.startswith(("http://", "https://")):
            raise ValueError("API_BASE_URL must start with http:// or https://")
        return v

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v = v.upper()
        if v not in valid_levels:
            raise ValueError(f"LOG_LEVEL must be one of {valid_levels}")
        return v

    @model_validator(mode="after")
    def validate_date_range(self) -> "ExtractorConfig":
        if self.week_end < self.week_start:
            raise ValueError(f"WEEK_END ({self.week_end}) must be >= WEEK_START ({self.week_start})")
        return self

    @model_validator(mode="after")
    def validate_azure_config(self) -> "ExtractorConfig":
        if (self.storage_account is None) != (self.container is None):
            raise ValueError("Both STORAGE_ACCOUNT and CONTAINER must be set together")
        return self

    @property
    def adls_enabled(self) -> bool:
        return self.storage_account is not None and self.container is not None

    @property
    def local_output_dir(self) -> Path:
        return self.local_data_dir / f"week_start={self.week_start}" / f"week_end={self.week_end}"

    @property
    def local_output_file(self) -> Path:
        return self.local_output_dir / "events.parquet"

    @property
    def local_rejects_output_dir(self) -> Path:
        return self.local_rejects_dir / f"week_start={self.week_start}" / f"week_end={self.week_end}"

    @property
    def local_rejects_output_file(self) -> Path:
        return self.local_rejects_output_dir / "rejects.parquet"

    @property
    def adls_output_path(self) -> str:
        return f"{self.adls_base_path}/week_start={self.week_start}/week_end={self.week_end}/events.parquet"

    @property
    def adls_rejects_output_path(self) -> str:
        return f"{self.adls_rejects_base_path}/week_start={self.week_start}/week_end={self.week_end}/rejects.parquet"

    @property
    def state_file(self) -> Path:
        return self.state_dir / self.state_file_name


def get_config() -> ExtractorConfig:
    """Load configuration from environment."""
    try:
        load_dotenv("config.env")
        return ExtractorConfig()
    except Exception as e:
        raise ConfigurationError(f"Failed to load configuration: {e}") from e


# =============================================================================
# DATA MODELS
# =============================================================================


class EventRecord(BaseModel):
    """Schema for individual event records from the API."""

    event_time: datetime = Field(..., description="Timestamp of the event")
    event_type: str = Field(..., description="Type of event")

    # API returns numeric ids, so we store as strings (coerced)
    user_id: str = Field(..., description="Unique user identifier")
    product_id: str | None = Field(default=None)
    category_id: str | None = Field(default=None)

    category_code: str | None = Field(default=None)
    brand: str | None = Field(default=None)
    price: Decimal | None = Field(default=None, ge=0)
    user_session: str | None = Field(default=None)

    model_config = {"extra": "ignore", "str_strip_whitespace": True}

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        return v.lower().strip()

    # Coerce numeric IDs from API into strings
    @field_validator("product_id", "category_id", "user_id", mode="before")
    @classmethod
    def coerce_ids_to_str(cls, v: Any) -> str | None:
        if v is None or v == "":
            return None
        return str(v).strip()

    @field_validator("user_id")
    @classmethod
    def validate_user_id(cls, v: str) -> str:
        v = str(v).strip()
        if not v:
            raise ValueError("user_id cannot be empty")
        return v

    @field_validator("price", mode="before")
    @classmethod
    def coerce_price(cls, v: Any) -> Decimal | None:
        if v is None or v == "":
            return None
        if isinstance(v, (int, float, str)):
            try:
                return Decimal(str(v))
            except Exception:
                return None
        return v


@dataclass
class ValidationResult:
    """Result of validating a batch of events."""

    total_records: int
    valid_records: int
    invalid_records: int
    validation_errors: list[dict] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return (self.valid_records / self.total_records) * 100

    @property
    def reject_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return (self.invalid_records / self.total_records) * 100


@dataclass
class PipelineMetrics:
    """Metrics collected during pipeline execution."""

    correlation_id: str = ""
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: datetime | None = None
    duration_seconds: float = 0.0

    rows_fetched: int = 0
    rows_validated: int = 0
    rows_rejected: int = 0
    reject_rate_pct: float = 0.0

    rows_output: int = 0

    output_file_path: str = ""
    output_file_size_bytes: int = 0
    output_file_sha256: str = ""

    rejects_file_path: str = ""
    rejects_file_size_bytes: int = 0
    rejects_file_sha256: str = ""

    adls_uploaded: bool = False
    adls_path: str = ""

    adls_rejects_uploaded: bool = False
    adls_rejects_path: str = ""

    run_status: str = "FAILED"  # SUCCESS | SUCCESS_WITH_REJECTS | FAILED
    success: bool = False
    error_message: str = ""
    error_type: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "correlation_id": self.correlation_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": round(self.duration_seconds, 2),
            "rows_fetched": self.rows_fetched,
            "rows_validated": self.rows_validated,
            "rows_rejected": self.rows_rejected,
            "reject_rate_pct": round(self.reject_rate_pct, 4),
            "rows_output": self.rows_output,
            "output_file_path": self.output_file_path,
            "output_file_size_bytes": self.output_file_size_bytes,
            "output_file_sha256": self.output_file_sha256,
            "rejects_file_path": self.rejects_file_path,
            "rejects_file_size_bytes": self.rejects_file_size_bytes,
            "rejects_file_sha256": self.rejects_file_sha256,
            "adls_uploaded": self.adls_uploaded,
            "adls_path": self.adls_path,
            "adls_rejects_uploaded": self.adls_rejects_uploaded,
            "adls_rejects_path": self.adls_rejects_path,
            "run_status": self.run_status,
            "success": self.success,
            "error_message": self.error_message,
            "error_type": self.error_type,
        }


# =============================================================================
# EXTRACTION
# =============================================================================


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
    import time
    from datetime import timedelta as _td

    if session is None:
        session = create_http_session(config)

    url = f"{config.api_base_url}/datasets/ecom_events"

    start_time = f"{config.week_start.isoformat()}T00:00:00"
    end_date = config.week_end + _td(days=1)
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
            logger.debug(f"Rate limit delay: {rate_limit_delay}s before next request")
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


# =============================================================================
# TRANSFORMATION
# =============================================================================

OUTPUT_COLUMNS = [
    "event_time", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session",
    "batch_week_start", "batch_week_end", "ingested_at",
]

REJECT_COLUMNS = [
    "row_index",
    "error_count",
    "errors",
    "raw_event",
    "batch_week_start",
    "batch_week_end",
    "ingested_at",
]


def validate_events(events: list[dict]) -> tuple[list[dict], list[dict], ValidationResult]:
    """Validate events against the schema. Returns (valid_events, reject_rows, result)."""
    from pydantic import ValidationError as PydanticValidationError

    valid_events: list[dict] = []
    reject_rows: list[dict] = []

    for idx, event in enumerate(events):
        try:
            validated = EventRecord.model_validate(event)
            valid_events.append(validated.model_dump())
        except PydanticValidationError as e:
            err_list = e.errors()
            reject_rows.append({
                "row_index": idx,
                "error_count": len(err_list),
                "errors": err_list,
                "raw_event": event,
            })

    result = ValidationResult(
        total_records=len(events),
        valid_records=len(valid_events),
        invalid_records=len(reject_rows),
        validation_errors=reject_rows[:100],  # sample for logs/debug
    )

    logger.info(
        "Schema validation complete",
        extra={
            "total_records": result.total_records,
            "valid_records": result.valid_records,
            "invalid_records": result.invalid_records,
            "success_rate": f"{result.success_rate:.2f}%",
            "reject_rate": f"{result.reject_rate:.2f}%",
        },
    )

    if reject_rows:
        for error in reject_rows[:5]:
            logger.warning("Validation error sample", extra={"row_index": error["row_index"]})

    return valid_events, reject_rows, result


def normalize_events(events: list[dict], config: ExtractorConfig) -> pd.DataFrame:
    """Normalize validated events into a DataFrame with batch metadata."""
    with log_operation(logger, "normalize_events", record_count=len(events)):
        if not events:
            return pd.DataFrame(columns=OUTPUT_COLUMNS)

        df = pd.DataFrame(events)

        df["batch_week_start"] = config.week_start.isoformat()
        df["batch_week_end"] = config.week_end.isoformat()
        df["ingested_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

        for col in OUTPUT_COLUMNS:
            if col not in df.columns:
                df[col] = None

        df = df[OUTPUT_COLUMNS]

        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce", utc=True)
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

        return df


def normalize_rejects(reject_rows: list[dict], config: ExtractorConfig) -> pd.DataFrame:
    """Normalize rejected rows into a DataFrame for storage/replay."""
    with log_operation(logger, "normalize_rejects", record_count=len(reject_rows)):
        if not reject_rows:
            return pd.DataFrame(columns=REJECT_COLUMNS)

        df = pd.DataFrame(reject_rows)
        df["batch_week_start"] = config.week_start.isoformat()
        df["batch_week_end"] = config.week_end.isoformat()
        df["ingested_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")

        for col in REJECT_COLUMNS:
            if col not in df.columns:
                df[col] = None

        df = df[REJECT_COLUMNS]
        return df


def run_data_quality_checks(df: pd.DataFrame, config: ExtractorConfig, min_records: int = 1) -> dict:
    """Run data quality checks on the normalized DataFrame."""
    checks: dict[str, Any] = {"passed": True, "record_count": len(df), "checks": {}}

    if min_records > 0 and len(df) < min_records:
        checks["checks"]["min_records"] = {"passed": False, "expected": f">= {min_records}", "actual": len(df)}
        checks["passed"] = False
    else:
        checks["checks"]["min_records"] = {"passed": True, "actual": len(df)}

    if len(df) == 0:
        logger.warning("Empty DataFrame - skipping remaining quality checks")
        return checks

    null_user_ids = df["user_id"].isna().sum()
    if null_user_ids > 0:
        checks["checks"]["null_user_ids"] = {"passed": False, "null_count": int(null_user_ids)}
        checks["passed"] = False
    else:
        checks["checks"]["null_user_ids"] = {"passed": True}

    week_start = pd.Timestamp(config.week_start, tz="UTC") - pd.Timedelta(days=1)
    week_end = pd.Timestamp(config.week_end, tz="UTC") + pd.Timedelta(days=2)

    if df["event_time"].notna().any():
        out_of_range = ((df["event_time"] < week_start) | (df["event_time"] > week_end)).sum()
        if out_of_range > 0:
            checks["checks"]["event_time_range"] = {"passed": False, "out_of_range_count": int(out_of_range)}
            logger.warning("Events outside expected date range", extra={"count": out_of_range})
        else:
            checks["checks"]["event_time_range"] = {"passed": True}

    if df["price"].notna().any():
        negative_prices = (df["price"] < 0).sum()
        if negative_prices > 0:
            checks["checks"]["negative_prices"] = {"passed": False, "count": int(negative_prices)}
            checks["passed"] = False
        else:
            checks["checks"]["negative_prices"] = {"passed": True}

    if not checks["passed"]:
        raise DataQualityError("Data quality checks failed", details=checks)

    logger.info("All data quality checks passed", extra=checks)
    return checks


def transform_events(
    raw_events: list[dict],
    config: ExtractorConfig,
    strict_validation: bool = True,
    min_records: int = 1,
) -> tuple[pd.DataFrame, pd.DataFrame, ValidationResult]:
    """Full transformation pipeline: validate -> normalize -> quality check. Returns (df_valid, df_rejects, validation_result)."""
    with log_operation(logger, "transform_pipeline", input_records=len(raw_events)):
        valid_events, reject_rows, validation_result = validate_events(raw_events)

        # Strict policy: fail if reject rate > threshold
        if strict_validation and validation_result.reject_rate > config.reject_threshold_pct:
            raise SchemaValidationError(
                f"Reject rate too high: {validation_result.reject_rate:.2f}% (threshold={config.reject_threshold_pct:.2f}%)",
                details={
                    "valid": validation_result.valid_records,
                    "rejected": validation_result.invalid_records,
                    "total": validation_result.total_records,
                },
            )

        df_valid = normalize_events(valid_events, config)
        df_rejects = normalize_rejects(reject_rows, config)

        run_data_quality_checks(df_valid, config, min_records=min_records)
        return df_valid, df_rejects, validation_result


# =============================================================================
# LOADING
# =============================================================================


def sha256_file(path: Path) -> str:
    """Calculate SHA256 hash of a file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_dir(path: Path) -> None:
    """Create directory if it doesn't exist."""
    path.mkdir(parents=True, exist_ok=True)


def check_output_exists(config: ExtractorConfig) -> bool:
    """Check if output file already exists."""
    return config.local_output_file.exists()


def write_parquet(df: pd.DataFrame, output_path: Path, compression: str = "snappy") -> dict:
    """Write DataFrame to parquet file."""
    with log_operation(logger, "write_parquet", output_path=str(output_path), row_count=len(df)):
        try:
            ensure_dir(output_path.parent)
            df.to_parquet(output_path, index=False, compression=compression, engine="pyarrow")

            file_size = output_path.stat().st_size
            file_hash = sha256_file(output_path)

            metadata = {
                "path": str(output_path),
                "size_bytes": file_size,
                "size_mb": round(file_size / (1024 * 1024), 2),
                "sha256": file_hash,
                "row_count": len(df),
            }

            logger.info("Parquet file written successfully", extra=metadata)
            return metadata

        except (PermissionError, OSError) as e:
            raise LocalStorageError(f"Failed to write parquet file: {e}") from e


def get_adls_credential():
    """Get Azure credential for ADLS access."""
    try:
        return DefaultAzureCredential(exclude_interactive_browser_credential=False)
    except Exception as e:
        raise AuthenticationError(
            "Azure authentication failed. Run 'az login' or configure service principal."
        ) from e


def upload_to_adls(local_file: Path, config: ExtractorConfig, adls_path: str) -> dict:
    """Upload file to Azure Data Lake Storage Gen2."""
    if not config.adls_enabled:
        raise AzureStorageError("ADLS upload not configured")

    with log_operation(logger, "upload_adls", storage_account=config.storage_account, path=adls_path):
        try:
            credential = get_adls_credential()
            account_url = f"https://{config.storage_account}.dfs.core.windows.net"
            service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
            fs_client = service_client.get_file_system_client(file_system=config.container)
            file_client = fs_client.get_file_client(adls_path)

            with local_file.open("rb") as f:
                data = f.read()

            file_client.upload_data(data, overwrite=True)

            metadata = {
                "storage_account": config.storage_account,
                "container": config.container,
                "path": adls_path,
                "size_bytes": len(data),
            }

            logger.info("File uploaded to ADLS successfully", extra=metadata)
            return metadata

        except ClientAuthenticationError as e:
            raise AuthenticationError(
                "Azure auth failed. Ensure 'Storage Blob Data Contributor' role is assigned."
            ) from e
        except (ServiceRequestError, AzureError) as e:
            raise AzureStorageError(f"Azure storage operation failed: {e}") from e


def save_outputs(
    df_valid: pd.DataFrame,
    df_rejects: pd.DataFrame,
    config: ExtractorConfig,
    upload_to_cloud: bool = True,
) -> dict:
    """Save valid + rejects to local storage and optionally upload to ADLS."""
    result = {"local": None, "adls": None, "rejects_local": None, "rejects_adls": None}

    # Valid
    result["local"] = write_parquet(df_valid, config.local_output_file)

    # Rejects (only write if any)
    if len(df_rejects) > 0:
        result["rejects_local"] = write_parquet(df_rejects, config.local_rejects_output_file)

    if upload_to_cloud and config.adls_enabled:
        result["adls"] = upload_to_adls(config.local_output_file, config, config.adls_output_path)

        if result["rejects_local"] is not None:
            result["rejects_adls"] = upload_to_adls(config.local_rejects_output_file, config, config.adls_rejects_output_path)
    elif upload_to_cloud and not config.adls_enabled:
        logger.info("ADLS upload skipped - not configured")

    return result


# =============================================================================
# CHECKPOINT / STATE
# =============================================================================


def compute_week_end(week_start: date, days: int = 6) -> date:
    return week_start + timedelta(days=days)


def read_state(config: ExtractorConfig) -> dict | None:
    path = config.state_file
    if not path.exists():
        return None
    try:
        # utf-8-sig handles BOM if present
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as e:
        raise ConfigurationError("Failed to read state file", details={"path": str(path), "error": str(e)}) from e


def write_state(config: ExtractorConfig, state: dict) -> None:
    try:
        ensure_dir(config.state_dir)
        config.state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
        logger.info("State updated", extra={"state_path": str(config.state_file)})
    except Exception as e:
        raise LocalStorageError("Failed to write state file", details={"path": str(config.state_file), "error": str(e)}) from e


# =============================================================================
# ORCHESTRATION
# =============================================================================


def run_pipeline(
    config: ExtractorConfig | None = None,
    skip_if_exists: bool = True,
    upload_to_cloud: bool = True,
    strict_validation: bool = True,
    dry_run: bool = False,
) -> PipelineMetrics:
    """Run the complete extraction pipeline."""
    start_time = perf_counter()
    metrics = PipelineMetrics(correlation_id=CorrelationIdFilter.generate_correlation_id())

    try:
        if config is None:
            config = get_config()

        logger.info(
            "Starting extraction pipeline",
            extra={
                "week_start": str(config.week_start),
                "week_end": str(config.week_end),
                "dry_run": dry_run,
                "reject_threshold_pct": config.reject_threshold_pct,
            },
        )

        if skip_if_exists and check_output_exists(config):
            logger.info("Output file already exists - skipping", extra={"path": str(config.local_output_file)})
            metrics.success = True
            metrics.run_status = "SUCCESS"
            metrics.output_file_path = str(config.local_output_file)
            return metrics

        with log_operation(logger, "extract_phase"):
            raw_events = fetch_events(config)
            metrics.rows_fetched = len(raw_events)

        with log_operation(logger, "transform_phase"):
            df_valid, df_rejects, validation_result = transform_events(
                raw_events,
                config,
                strict_validation=strict_validation,
                min_records=0 if dry_run else 1,
            )
            metrics.rows_validated = len(df_valid)
            metrics.rows_rejected = validation_result.invalid_records
            metrics.reject_rate_pct = validation_result.reject_rate
            metrics.rows_output = len(df_valid)

        if not dry_run:
            with log_operation(logger, "load_phase"):
                save_result = save_outputs(df_valid, df_rejects, config, upload_to_cloud=upload_to_cloud)

                if save_result["local"]:
                    metrics.output_file_path = save_result["local"]["path"]
                    metrics.output_file_size_bytes = save_result["local"]["size_bytes"]
                    metrics.output_file_sha256 = save_result["local"]["sha256"]

                if save_result["rejects_local"]:
                    metrics.rejects_file_path = save_result["rejects_local"]["path"]
                    metrics.rejects_file_size_bytes = save_result["rejects_local"]["size_bytes"]
                    metrics.rejects_file_sha256 = save_result["rejects_local"]["sha256"]

                if save_result["adls"]:
                    metrics.adls_uploaded = True
                    metrics.adls_path = save_result["adls"]["path"]

                if save_result["rejects_adls"]:
                    metrics.adls_rejects_uploaded = True
                    metrics.adls_rejects_path = save_result["rejects_adls"]["path"]
        else:
            logger.info("Dry run - skipping file output")

        # Run status
        if metrics.rows_rejected == 0:
            metrics.run_status = "SUCCESS"
        else:
            metrics.run_status = "SUCCESS_WITH_REJECTS"

        metrics.success = True
        logger.info("Pipeline completed successfully", extra=metrics.to_dict())

    except ExtractorError as e:
        metrics.success = False
        metrics.run_status = "FAILED"
        metrics.error_message = str(e)
        metrics.error_type = type(e).__name__
        logger.error(f"Pipeline failed: {e}", extra=metrics.to_dict())
        raise

    except Exception as e:
        metrics.success = False
        metrics.run_status = "FAILED"
        metrics.error_message = str(e)
        metrics.error_type = type(e).__name__
        logger.error(f"Unexpected error: {e}", extra=metrics.to_dict(), exc_info=True)
        raise ExtractorError(f"Unexpected error: {e}") from e

    finally:
        metrics.end_time = datetime.now(timezone.utc)
        metrics.duration_seconds = perf_counter() - start_time

    return metrics


# =============================================================================
# CLI
# =============================================================================


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract weekly event data from API to data lake",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python extractor/extract_events.py
  python extractor/extract_events.py --week-start 2019-12-01 --week-end 2019-12-07
  python extractor/extract_events.py --use-checkpoint
  python extractor/extract_events.py --use-checkpoint --advance-on-skip
  python extractor/extract_events.py --use-checkpoint --no-strict-validation
        """,
    )

    parser.add_argument("--week-start", type=str, help="Start of batch week (YYYY-MM-DD)")
    parser.add_argument("--week-end", type=str, help="End of batch week (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Validate only, don't write files")
    parser.add_argument("--force", action="store_true", help="Force re-run even if output exists")
    parser.add_argument("--local-only", action="store_true", help="Skip ADLS upload")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")
    parser.add_argument("--log-format", choices=["json", "text"], default="json")
    parser.add_argument("--check-api", action="store_true", help="Check API health and exit")

    # Strict policy toggle
    parser.add_argument("--no-strict-validation", action="store_true", help="Do not fail if reject rate exceeds threshold")

    # Airflow-friendly resume
    parser.add_argument("--use-checkpoint", action="store_true", help="Use state file to select next week to run")
    parser.add_argument("--state-path", type=str, default=None, help="Override checkpoint file path")
    parser.add_argument(
        "--advance-on-skip",
        action="store_true",
        help="If output exists and pipeline skips, still advance checkpoint to next week",
    )

    return parser.parse_args()


def main() -> int:
    """Main entry point for CLI execution."""
    args = parse_args()

    setup_logging(level=args.log_level, json_format=(args.log_format == "json"))

    try:
        config = get_config()

        # Optional: override checkpoint path
        if args.state_path:
            p = Path(args.state_path)
            config.state_dir = p.parent
            config.state_file_name = p.name

        # If using checkpoint, load next week from state (ONLY when week-start/week-end not provided)
        if args.use_checkpoint and (not args.week_start) and (not args.week_end):
            state = read_state(config)
            if state and state.get("next_week_start"):
                config.week_start = date.fromisoformat(state["next_week_start"])
                if state.get("next_week_end"):
                    config.week_end = date.fromisoformat(state["next_week_end"])
                else:
                    config.week_end = compute_week_end(config.week_start)
                logger.info(
                    "Loaded checkpoint week",
                    extra={"week_start": str(config.week_start), "week_end": str(config.week_end)},
                )
            else:
                logger.info(
                    "No checkpoint found; using config.env week range",
                    extra={"week_start": str(config.week_start), "week_end": str(config.week_end)},
                )

        # Override from CLI (explicit week always wins)
        if args.week_start:
            config.week_start = date.fromisoformat(args.week_start)
        if args.week_end:
            config.week_end = date.fromisoformat(args.week_end)

        # Health check mode
        if args.check_api:
            healthy = check_api_health(config)
            logger.info("API health check " + ("passed" if healthy else "failed"))
            return 0 if healthy else 1

        strict_validation = not args.no_strict_validation

        # Run pipeline (one week)
        metrics = run_pipeline(
            config=config,
            skip_if_exists=not args.force,
            upload_to_cloud=not args.local_only,
            dry_run=args.dry_run,
            strict_validation=strict_validation,
        )

        # Update checkpoint ONLY on SUCCESS / SUCCESS_WITH_REJECTS (and not dry-run)
        if metrics.success and (not args.dry_run) and args.use_checkpoint:
            was_skipped = (metrics.rows_fetched == 0) and (not args.force) and check_output_exists(config)

            # If skipped, only advance if requested
            if was_skipped and (not args.advance_on_skip):
                logger.info("Pipeline skipped and advance-on-skip not set; checkpoint not advanced")
                return 0

            # IMPORTANT: do not advance if run was "FAILED" (already ensured by metrics.success)
            # Also, for strict mode: this will only reach here if reject_rate <= threshold (because otherwise it raises)
            # For non-strict mode: we still only advance if reject_rate <= threshold (so reruns won’t skip bad weeks).
            if metrics.reject_rate_pct <= config.reject_threshold_pct:
                next_week_start = config.week_end + timedelta(days=1)
                next_week_end = compute_week_end(next_week_start)

                new_state = {
                    "last_successful_week_start": str(config.week_start),
                    "last_successful_week_end": str(config.week_end),
                    "next_week_start": str(next_week_start),
                    "next_week_end": str(next_week_end),
                    "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "last_run_correlation_id": metrics.correlation_id,
                    "last_output_file_path": metrics.output_file_path,
                    "last_rows_output": metrics.rows_output,
                    "last_run_status": metrics.run_status,
                    "last_reject_rate_pct": round(metrics.reject_rate_pct, 4),
                    "last_rejects_file_path": metrics.rejects_file_path,
                }
                write_state(config, new_state)
            else:
                logger.warning(
                    "Reject rate above threshold; checkpoint not advanced",
                    extra={"reject_rate_pct": metrics.reject_rate_pct, "threshold_pct": config.reject_threshold_pct},
                )

        return 0 if metrics.success else 1

    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except ExtractorError as e:
        logger.error(f"Extraction failed: {e}")
        return 1
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        return 130
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())

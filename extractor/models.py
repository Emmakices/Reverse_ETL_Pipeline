"""Data models for the extraction pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator


class EventRecord(BaseModel):
    """Schema for individual event records from the API."""

    event_time: datetime = Field(..., description="Timestamp of the event")
    event_type: str = Field(..., description="Type of event")

    @field_validator("event_time", mode="before")
    @classmethod
    def validate_event_time(cls, v: Any) -> datetime:
        if isinstance(v, datetime):
            return v
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                raise ValueError(f"Invalid event_time format: {v!r}")
        raise ValueError(f"event_time must be a datetime or ISO-8601 string, got {type(v).__name__}")

    # IMPORTANT: API returns numeric ids, so we store as strings (coerced)
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

    # coerce numeric IDs from API into strings
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
    def is_acceptable(self) -> bool:
        return self.success_rate >= 95.0


class RunStatus(str, Enum):
    SUCCESS = "SUCCESS"
    SUCCESS_WITH_REJECTS = "SUCCESS_WITH_REJECTS"
    FAILED = "FAILED"


@dataclass
class PipelineMetrics:
    """Metrics collected during pipeline execution."""

    correlation_id: str = ""
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: datetime | None = None
    duration_seconds: float = 0.0

    run_status: str = RunStatus.FAILED.value
    reject_rate_pct: float = 0.0
    reject_threshold_pct: float = 0.0

    rows_fetched: int = 0
    rows_validated: int = 0
    rows_output: int = 0
    rows_rejected: int = 0

    output_file_path: str = ""
    output_file_size_bytes: int = 0
    output_file_sha256: str = ""

    rejects_file_path: str = ""
    rejects_file_size_bytes: int = 0
    rejects_file_sha256: str = ""

    adls_uploaded: bool = False
    adls_path: str = ""

    rejects_adls_uploaded: bool = False
    rejects_adls_path: str = ""

    success: bool = False
    error_message: str = ""
    error_type: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "correlation_id": self.correlation_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": round(self.duration_seconds, 2),
            "run_status": self.run_status,
            "reject_rate_pct": round(self.reject_rate_pct, 4),
            "reject_threshold_pct": round(self.reject_threshold_pct, 4),
            "rows_fetched": self.rows_fetched,
            "rows_validated": self.rows_validated,
            "rows_output": self.rows_output,
            "rows_rejected": self.rows_rejected,
            "output_file_path": self.output_file_path,
            "output_file_size_bytes": self.output_file_size_bytes,
            "output_file_sha256": self.output_file_sha256,
            "rejects_file_path": self.rejects_file_path,
            "rejects_file_size_bytes": self.rejects_file_size_bytes,
            "rejects_file_sha256": self.rejects_file_sha256,
            "adls_uploaded": self.adls_uploaded,
            "adls_path": self.adls_path,
            "rejects_adls_uploaded": self.rejects_adls_uploaded,
            "rejects_adls_path": self.rejects_adls_path,
            "success": self.success,
            "error_message": self.error_message,
            "error_type": self.error_type,
        }

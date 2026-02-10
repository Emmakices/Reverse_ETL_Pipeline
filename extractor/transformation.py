"""Data validation, normalization, and quality checks."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from extractor.config import ExtractorConfig
from extractor.exceptions import DataQualityError, SchemaValidationError
from extractor.logging_utils import get_logger, log_operation
from extractor.models import EventRecord, ValidationResult

logger = get_logger(__name__)

OUTPUT_COLUMNS = [
    "event_time", "event_type", "product_id", "category_id", "category_code",
    "brand", "price", "user_id", "user_session",
    "batch_week_start", "batch_week_end", "ingested_at",
]


def validate_events(events: list[dict]) -> tuple[list[dict], list[dict], ValidationResult]:
    """
    Validate events against the schema.
    Returns: (valid_events, rejected_events, ValidationResult)
    """
    from pydantic import ValidationError as PydanticValidationError

    valid_events: list[dict] = []
    rejected_events: list[dict] = []
    validation_errors: list[dict] = []

    for idx, event in enumerate(events):
        try:
            validated = EventRecord.model_validate(event)
            valid_events.append(validated.model_dump())
        except PydanticValidationError as e:
            rejected_events.append({
                "row_index": idx,
                "error_count": len(e.errors()),
                "errors": e.errors(),
                "raw_event": event,
                "rejected_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            })

            validation_errors.append({
                "row_index": idx,
                "errors": e.errors(),
                "raw_data": {k: str(v)[:100] for k, v in event.items()},
            })

    result = ValidationResult(
        total_records=len(events),
        valid_records=len(valid_events),
        invalid_records=len(rejected_events),
        validation_errors=validation_errors[:100],
    )

    logger.info(
        "Schema validation complete",
        extra={
            "total_records": result.total_records,
            "valid_records": result.valid_records,
            "invalid_records": result.invalid_records,
            "success_rate": f"{result.success_rate:.2f}%",
        },
    )

    if rejected_events:
        for err in rejected_events[:5]:
            logger.warning(
                "Validation reject sample",
                extra={"row_index": err["row_index"], "error_count": err["error_count"]},
            )

    return valid_events, rejected_events, result


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


def normalize_rejects(rejected_events: list[dict], config: ExtractorConfig) -> pd.DataFrame:
    """Normalize rejects into a DataFrame, with week metadata."""
    with log_operation(logger, "normalize_rejects", reject_count=len(rejected_events)):
        if not rejected_events:
            return pd.DataFrame(columns=[
                "row_index", "error_count", "errors", "raw_event", "rejected_at",
                "batch_week_start", "batch_week_end",
            ])

        df = pd.DataFrame(rejected_events)
        df["batch_week_start"] = config.week_start.isoformat()
        df["batch_week_end"] = config.week_end.isoformat()
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

    # Match API semantics: event_time >= week_start AND event_time < week_end + 1 day
    week_start = pd.Timestamp(config.week_start, tz="UTC")
    week_end = pd.Timestamp(config.week_end, tz="UTC") + pd.Timedelta(days=1)

    if df["event_time"].notna().any():
        out_of_range = ((df["event_time"] < week_start) | (df["event_time"] >= week_end)).sum()
        if out_of_range > 0:
            checks["checks"]["event_time_range"] = {"passed": False, "out_of_range_count": int(out_of_range)}
            logger.warning("Events outside expected date range", extra={"count": int(out_of_range)})
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
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Full transformation pipeline: validate -> normalize(valid + rejects) -> quality check(valid)."""
    with log_operation(logger, "transform_pipeline", input_records=len(raw_events)):
        valid_events, rejected_events, validation_result = validate_events(raw_events)

        if strict_validation and not validation_result.is_acceptable:
            raise SchemaValidationError(
                f"Validation success rate too low: {validation_result.success_rate:.1f}%",
                details={"valid": validation_result.valid_records, "total": validation_result.total_records},
            )

        df_valid = normalize_events(valid_events, config)
        run_data_quality_checks(df_valid, config, min_records=min_records)

        df_rejects = normalize_rejects(rejected_events, config)
        return df_valid, df_rejects

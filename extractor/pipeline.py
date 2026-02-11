"""Pipeline orchestration: full pipeline and individual stage runners."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

import pandas as pd

from extractor.azure_sql import (
    db_finalize_pipeline_run,
    db_insert_pipeline_run,
    db_stage_end,
    db_stage_start,
)
from extractor.config import ExtractorConfig, get_config
from extractor.exceptions import DataQualityError, ExtractorError
from extractor.extraction import fetch_events
from extractor.logging_utils import CorrelationIdFilter, get_logger, log_operation
from extractor.models import PipelineMetrics, RunStatus
from extractor.storage import check_output_exists, ensure_dir, save_events
from extractor.transformation import transform_events

logger = get_logger(__name__)

# Run-scoped tag to prevent stage metadata collisions across concurrent runs.
# Set once per CLI invocation via set_run_tag(); defaults to "default".
_run_tag: str = "default"


def set_run_tag(tag: str) -> None:
    """Set a run-scoped tag used to namespace stage metadata files."""
    global _run_tag
    _run_tag = tag


def compute_reject_rate_pct(rows_rejected: int, rows_fetched: int) -> float:
    if rows_fetched <= 0:
        return 0.0
    return (rows_rejected / rows_fetched) * 100.0


def decide_run_status(reject_rate_pct: float, rows_rejected: int, threshold_pct: float) -> str:
    if rows_rejected <= 0:
        return RunStatus.SUCCESS.value
    if reject_rate_pct <= threshold_pct:
        return RunStatus.SUCCESS_WITH_REJECTS.value
    return RunStatus.FAILED.value


# ---------------------------------------------------------------------------
# Stage metadata helpers (for inter-stage data passing)
# ---------------------------------------------------------------------------

def _stage_meta_path(config: ExtractorConfig, stage_name: str) -> Path:
    """Path for a stage's metadata sidecar file, namespaced by run tag."""
    return config.local_output_dir / f"_stage_{stage_name}_{_run_tag}.json"


def _write_stage_meta(config: ExtractorConfig, stage_name: str, meta: dict) -> None:
    ensure_dir(config.local_output_dir)
    path = _stage_meta_path(config, stage_name)
    path.write_text(json.dumps(meta, default=str), encoding="utf-8")
    logger.info(f"Stage metadata written: {stage_name}", extra={"path": str(path)})


def _read_stage_meta(config: ExtractorConfig, stage_name: str) -> dict:
    path = _stage_meta_path(config, stage_name)
    if not path.exists():
        raise ExtractorError(f"Stage metadata not found: {path}. Run the '{stage_name}' stage first.")
    return json.loads(path.read_text(encoding="utf-8"))


def cleanup_stage_metadata(config: ExtractorConfig) -> None:
    """Remove stage metadata files from the output directory after a successful run."""
    output_dir = config.local_output_dir
    if not output_dir.exists():
        return
    for meta_file in output_dir.glob("_stage_*.json"):
        try:
            meta_file.unlink()
        except OSError:
            pass
    # Also clean up temporary intermediate files
    for temp_file in output_dir.glob("_raw_events.json"):
        try:
            temp_file.unlink()
        except OSError:
            pass
    for temp_file in output_dir.glob("_validated_*.parquet"):
        try:
            temp_file.unlink()
        except OSError:
            pass
    logger.debug("Stage metadata cleaned up", extra={"dir": str(output_dir)})


# ---------------------------------------------------------------------------
# Individual stage runners (for multi-task DAG)
# ---------------------------------------------------------------------------

def run_extract_stage(config: ExtractorConfig) -> dict:
    """Run only the extraction stage. Saves raw events as JSON for downstream stages."""
    with log_operation(logger, "extract_stage"):
        raw_events = fetch_events(config)
        # Save raw events to a temporary JSON file for the transform stage
        raw_path = config.local_output_dir / "_raw_events.json"
        ensure_dir(raw_path.parent)
        raw_path.write_text(json.dumps(raw_events, default=str), encoding="utf-8")
        meta = {"rows_fetched": len(raw_events), "raw_events_path": str(raw_path)}
        _write_stage_meta(config, "extract", meta)
        logger.info("Extract stage complete", extra=meta)
        return meta


def run_transform_stage(config: ExtractorConfig) -> dict:
    """Run only the transform stage. Reads raw events JSON, writes validated parquet."""
    with log_operation(logger, "transform_stage"):
        extract_meta = _read_stage_meta(config, "extract")
        raw_path = Path(extract_meta["raw_events_path"])
        raw_events = json.loads(raw_path.read_text(encoding="utf-8"))

        df_valid, df_rejects = transform_events(raw_events, config, strict_validation=True, min_records=1)

        rows_fetched = extract_meta["rows_fetched"]
        rows_rejected = 0 if df_rejects is None else len(df_rejects)
        reject_rate_pct = compute_reject_rate_pct(rows_rejected, rows_fetched)
        run_status = decide_run_status(reject_rate_pct, rows_rejected, float(config.reject_threshold_pct))

        # Save validated data as parquet for the load stage
        valid_path = config.local_output_dir / "_validated_events.parquet"
        ensure_dir(valid_path.parent)
        df_valid.to_parquet(valid_path, index=False, compression="snappy", engine="pyarrow")

        rejects_path = None
        if df_rejects is not None and not df_rejects.empty:
            rejects_path = str(config.local_output_dir / "_validated_rejects.parquet")
            df_rejects.to_parquet(rejects_path, index=False, compression="snappy", engine="pyarrow")

        meta = {
            "rows_fetched": rows_fetched,
            "rows_output": len(df_valid),
            "rows_rejected": rows_rejected,
            "reject_rate_pct": reject_rate_pct,
            "reject_threshold_pct": float(config.reject_threshold_pct),
            "run_status": run_status,
            "validated_path": str(valid_path),
            "rejects_path": rejects_path,
        }
        _write_stage_meta(config, "transform", meta)
        logger.info("Transform stage complete", extra=meta)
        return meta


def run_load_stage(config: ExtractorConfig, upload_to_cloud: bool = True) -> dict:
    """Run only the load stage. Reads validated parquet, writes final output + ADLS upload."""
    with log_operation(logger, "load_stage"):
        transform_meta = _read_stage_meta(config, "transform")
        df_valid = pd.read_parquet(transform_meta["validated_path"])
        df_rejects = pd.DataFrame()
        if transform_meta.get("rejects_path"):
            df_rejects = pd.read_parquet(transform_meta["rejects_path"])

        save_result = save_events(df_valid, df_rejects, config, upload_to_cloud=upload_to_cloud)

        meta = {
            "local": save_result.get("local"),
            "adls": save_result.get("adls"),
            "rejects_local": save_result.get("rejects_local"),
            "rejects_adls": save_result.get("rejects_adls"),
            "run_status": transform_meta["run_status"],
            "rows_output": transform_meta["rows_output"],
            "rows_rejected": transform_meta["rows_rejected"],
            "reject_rate_pct": transform_meta["reject_rate_pct"],
            "reject_threshold_pct": transform_meta["reject_threshold_pct"],
        }
        _write_stage_meta(config, "load", meta)
        logger.info("Load stage complete", extra=meta)
        return meta


def run_quality_gate_stage(config: ExtractorConfig) -> dict:
    """Run quality gate check. Fails if reject threshold is breached."""
    with log_operation(logger, "quality_gate_stage"):
        transform_meta = _read_stage_meta(config, "transform")

        run_status = transform_meta["run_status"]
        reject_rate_pct = transform_meta["reject_rate_pct"]
        threshold_pct = transform_meta["reject_threshold_pct"]

        if run_status == RunStatus.FAILED.value:
            msg = f"Reject rate {reject_rate_pct:.4f}% exceeds threshold {threshold_pct:.4f}%"
            logger.error("Quality gate FAILED", extra=transform_meta)
            raise DataQualityError(
                "Reject threshold exceeded",
                details={
                    "reject_rate_pct": reject_rate_pct,
                    "reject_threshold_pct": threshold_pct,
                    "rows_rejected": transform_meta["rows_rejected"],
                },
            )

        meta = {"quality_gate": "PASSED", "run_status": run_status}
        _write_stage_meta(config, "quality_gate", meta)
        logger.info("Quality gate PASSED", extra=meta)
        return meta


# ---------------------------------------------------------------------------
# Full pipeline (backward compatible)
# ---------------------------------------------------------------------------

def run_pipeline(
    config: ExtractorConfig | None = None,
    skip_if_exists: bool = True,
    upload_to_cloud: bool = True,
    strict_validation: bool = True,
    dry_run: bool = False,
) -> PipelineMetrics:
    """Run the complete extraction pipeline with DB-first checkpoint pattern."""
    start_time = perf_counter()
    metrics = PipelineMetrics(correlation_id=CorrelationIdFilter.generate_correlation_id())

    pipeline_name = "reverse_etl_events"
    run_id = None
    db_write_failed = False

    # DB run insert — fail fast if DB is unreachable (signals broader infra issues)
    try:
        run_id = db_insert_pipeline_run(config, pipeline_name, metrics)
    except Exception as e:
        logger.error(
            "DB run insert failed — cannot proceed without audit trail",
            extra={"error": str(e)},
        )
        raise ExtractorError(
            f"Pipeline aborted: unable to insert pipeline run to DB: {e}"
        ) from e

    try:
        if config is None:
            config = get_config()

        metrics.reject_threshold_pct = float(config.reject_threshold_pct)

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

            # Re-attempt ADLS upload if local file exists but cloud upload was requested
            if upload_to_cloud and config.adls_enabled:
                try:
                    from extractor.storage import upload_to_adls, upload_rejects_to_adls
                    logger.info("Re-attempting ADLS upload for existing local file")
                    adls_result = upload_to_adls(config.local_output_file, config)
                    metrics.adls_uploaded = True
                    metrics.adls_path = adls_result["path"]
                    if config.local_rejects_output_file.exists():
                        rejects_result = upload_rejects_to_adls(config.local_rejects_output_file, config)
                        metrics.rejects_adls_uploaded = True
                        metrics.rejects_adls_path = rejects_result["path"]
                except Exception as e:
                    logger.warning("ADLS re-upload failed on skip", extra={"error": str(e)})

            metrics.success = True
            metrics.run_status = RunStatus.SUCCESS.value
            metrics.output_file_path = str(config.local_output_file)
            return metrics

        # ============================================================
        # EXTRACT PHASE - with stage tracking
        # ============================================================
        stage_id = None
        try:
            if run_id:
                try:
                    stage_id = db_stage_start(config, run_id, "EXTRACT")
                except Exception as e:
                    logger.warning("DB stage start failed", extra={"error": str(e)})

            with log_operation(logger, "extract_phase"):
                raw_events = fetch_events(config)
                metrics.rows_fetched = len(raw_events)

            if run_id and stage_id:
                try:
                    db_stage_end(config, stage_id, "SUCCESS", row_count=metrics.rows_fetched)
                except Exception as e:
                    logger.warning("DB stage end failed", extra={"error": str(e)})

        except Exception as e:
            if run_id and stage_id:
                try:
                    db_stage_end(config, stage_id, "FAILED", row_count=None, error_message=str(e))
                except Exception as db_err:
                    logger.warning("DB stage end failed", extra={"error": str(db_err)})
            raise

        # ============================================================
        # TRANSFORM PHASE - with stage tracking
        # ============================================================
        stage_id = None
        try:
            if run_id:
                try:
                    stage_id = db_stage_start(config, run_id, "TRANSFORM")
                except Exception as e:
                    logger.warning("DB stage start failed", extra={"error": str(e)})

            with log_operation(logger, "transform_phase"):
                df_valid, df_rejects = transform_events(
                    raw_events,
                    config,
                    strict_validation=strict_validation,
                    min_records=0 if dry_run else 1,
                )
                metrics.rows_validated = len(df_valid)
                metrics.rows_output = len(df_valid)
                metrics.rows_rejected = 0 if df_rejects is None else len(df_rejects)

                metrics.reject_rate_pct = compute_reject_rate_pct(metrics.rows_rejected, metrics.rows_fetched)
                metrics.run_status = decide_run_status(
                    reject_rate_pct=metrics.reject_rate_pct,
                    rows_rejected=metrics.rows_rejected,
                    threshold_pct=float(config.reject_threshold_pct),
                )

                logger.info(
                    "Rejects summary",
                    extra={
                        "rows_fetched": metrics.rows_fetched,
                        "rows_rejected": metrics.rows_rejected,
                        "reject_rate_pct": round(metrics.reject_rate_pct, 4),
                        "reject_threshold_pct": config.reject_threshold_pct,
                        "run_status": metrics.run_status,
                    },
                )

            if run_id and stage_id:
                try:
                    db_stage_end(config, stage_id, "SUCCESS", row_count=metrics.rows_output)
                except Exception as e:
                    logger.warning("DB stage end failed", extra={"error": str(e)})

        except Exception as e:
            if run_id and stage_id:
                try:
                    db_stage_end(config, stage_id, "FAILED", row_count=None, error_message=str(e))
                except Exception as db_err:
                    logger.warning("DB stage end failed", extra={"error": str(db_err)})
            raise

        # Check if reject threshold was breached
        fail_due_to_rejects = (metrics.run_status == RunStatus.FAILED.value)

        # ============================================================
        # LOAD PHASE - with stage tracking (only if not dry_run)
        # ============================================================
        if not dry_run:
            stage_id = None
            try:
                if run_id:
                    try:
                        stage_id = db_stage_start(config, run_id, "LOAD")
                    except Exception as e:
                        logger.warning("DB stage start failed", extra={"error": str(e)})

                with log_operation(logger, "load_phase"):
                    save_result = save_events(df_valid, df_rejects, config, upload_to_cloud=upload_to_cloud)

                    if save_result["local"]:
                        metrics.output_file_path = save_result["local"]["path"]
                        metrics.output_file_size_bytes = save_result["local"]["size_bytes"]
                        metrics.output_file_sha256 = save_result["local"]["sha256"]

                    if save_result.get("rejects_local"):
                        metrics.rejects_file_path = save_result["rejects_local"]["path"]
                        metrics.rejects_file_size_bytes = save_result["rejects_local"]["size_bytes"]
                        metrics.rejects_file_sha256 = save_result["rejects_local"]["sha256"]

                    if save_result["adls"]:
                        metrics.adls_uploaded = True
                        metrics.adls_path = save_result["adls"]["path"]

                    if save_result.get("rejects_adls"):
                        metrics.rejects_adls_uploaded = True
                        metrics.rejects_adls_path = save_result["rejects_adls"]["path"]

                if run_id and stage_id:
                    try:
                        db_stage_end(config, stage_id, "SUCCESS", row_count=metrics.rows_output)
                    except Exception as e:
                        logger.warning("DB stage end failed", extra={"error": str(e)})

            except Exception as e:
                if run_id and stage_id:
                    try:
                        db_stage_end(config, stage_id, "FAILED", row_count=None, error_message=str(e))
                    except Exception as db_err:
                        logger.warning("DB stage end failed", extra={"error": str(db_err)})
                raise
        else:
            logger.info("Dry run - skipping file output")

        # ============================================================
        # QUALITY GATE - reject threshold breach check
        # ============================================================
        if fail_due_to_rejects:
            metrics.success = False
            metrics.error_type = "DataQualityError"
            metrics.error_message = (
                f"Reject rate {metrics.reject_rate_pct:.4f}% exceeds threshold {config.reject_threshold_pct:.4f}%"
            )

            # Record this failure as a separate stage for visibility
            if run_id:
                try:
                    gate_stage_id = db_stage_start(config, run_id, "QUALITY_GATE")
                    db_stage_end(
                        config,
                        gate_stage_id,
                        "FAILED",
                        row_count=metrics.rows_output,
                        error_message=f"Reject rate {metrics.reject_rate_pct:.6f}% exceeded threshold {config.reject_threshold_pct:.6f}%",
                    )
                except Exception as e:
                    logger.warning("DB quality gate stage tracking failed", extra={"error": str(e)})

            logger.error("Pipeline failed due to reject threshold breach", extra=metrics.to_dict())
            raise DataQualityError(
                "Reject threshold exceeded",
                details={
                    "reject_rate_pct": metrics.reject_rate_pct,
                    "reject_threshold_pct": config.reject_threshold_pct,
                    "rows_fetched": metrics.rows_fetched,
                    "rows_rejected": metrics.rows_rejected,
                    "rejects_file_path": metrics.rejects_file_path,
                    "rejects_adls_path": metrics.rejects_adls_path,
                },
            )

        metrics.success = True
        logger.info("Pipeline completed successfully", extra=metrics.to_dict())

        # Clean up temporary stage metadata files
        try:
            cleanup_stage_metadata(config)
        except Exception as e:
            logger.debug("Stage metadata cleanup failed (non-critical)", extra={"error": str(e)})

    except ExtractorError as e:
        metrics.success = False
        metrics.error_message = str(e)
        metrics.error_type = type(e).__name__
        metrics.run_status = RunStatus.FAILED.value
        logger.error(f"Pipeline failed: {e}", extra=metrics.to_dict())
        raise

    except Exception as e:
        metrics.success = False
        metrics.error_message = str(e)
        metrics.error_type = type(e).__name__
        metrics.run_status = RunStatus.FAILED.value
        logger.error(f"Unexpected error: {e}", extra=metrics.to_dict(), exc_info=True)
        raise ExtractorError(f"Unexpected error: {e}") from e

    finally:
        metrics.end_time = datetime.now(timezone.utc)
        metrics.duration_seconds = perf_counter() - start_time

        # Best-effort: Finalize DB run tracking
        if run_id:
            try:
                db_finalize_pipeline_run(config, run_id, metrics)
            except Exception as e:
                db_write_failed = True
                logger.error("DB run finalize failed", extra={"error": str(e), "run_id": run_id})

    return metrics

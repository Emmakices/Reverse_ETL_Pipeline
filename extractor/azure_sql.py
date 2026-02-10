"""Azure SQL integration: AAD auth, checkpoint, run/stage tracking, pending update replay."""

from __future__ import annotations

import uuid
from datetime import date

import pyodbc
from azure.identity import DefaultAzureCredential

from extractor.checkpoint import read_pending_db_updates, rewrite_pending_db_updates
from extractor.config import ExtractorConfig
from extractor.exceptions import AuthenticationError, CheckpointError, ConfigurationError
from extractor.logging_utils import get_logger
from extractor.models import PipelineMetrics

logger = get_logger(__name__)

SQL_COPT_SS_ACCESS_TOKEN = 1256  # ODBC attribute for AAD access token


def _get_sql_access_token_bytes() -> bytes:
    """
    Returns the AAD access token bytes in the format required by ODBC:
    4-byte little-endian length prefix + UTF-16LE token bytes.
    """
    try:
        cred = DefaultAzureCredential()
        token = cred.get_token("https://database.windows.net/.default").token
    except Exception as e:
        raise AuthenticationError(
            "Failed to get Azure SQL access token. Ensure 'az login' is configured "
            "or service principal credentials are set.",
            details={"error": str(e)},
        ) from e
    token_bytes = token.encode("utf-16-le")
    return (len(token_bytes)).to_bytes(4, "little") + token_bytes


def get_sql_connection(config: ExtractorConfig) -> pyodbc.Connection:
    """
    DSN-less connection to Azure SQL using AAD token auth.
    Requires: az login (works for your current setup)
    """
    if not config.azure_sql_server or not config.azure_sql_database:
        raise ConfigurationError(
            "AZURE_SQL_SERVER and AZURE_SQL_DATABASE must be set",
            details={"server": config.azure_sql_server, "database": config.azure_sql_database},
        )
    server = config.azure_sql_server
    database = config.azure_sql_database

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={server},1433;"
        f"DATABASE={database};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    token_bytes = _get_sql_access_token_bytes()
    return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_bytes})


def db_get_checkpoint(config: ExtractorConfig, pipeline_name: str) -> dict:
    """
    Read next week from dbo.pipeline_checkpoint.
    DB-FIRST PATTERN: This is REQUIRED - no fallback to JSON.
    Raises CheckpointError if unable to read from DB.
    """
    try:
        with get_sql_connection(config) as cn:
            cur = cn.cursor()
            cur.execute(
                """
                SELECT pipeline_name, last_successful_week_start, last_successful_week_end,
                       next_week_start, next_week_end
                FROM dbo.pipeline_checkpoint
                WHERE pipeline_name = ?
                """,
                pipeline_name,
            )
            row = cur.fetchone()
            if not row:
                logger.info("No checkpoint found in DB - will use config.env defaults")
                return None

            checkpoint = {
                "pipeline_name": row[0],
                "last_successful_week_start": row[1].isoformat() if row[1] else None,
                "last_successful_week_end": row[2].isoformat() if row[2] else None,
                "next_week_start": row[3].isoformat(),
                "next_week_end": row[4].isoformat(),
            }
            logger.info("Loaded checkpoint from DB", extra=checkpoint)
            return checkpoint

    except Exception as e:
        logger.error("FATAL: Unable to read checkpoint from DB", extra={"error": str(e)})
        raise CheckpointError(
            "Failed to read checkpoint from database - cannot proceed",
            details={"pipeline_name": pipeline_name, "error": str(e)}
        ) from e


def db_update_checkpoint(
    config: ExtractorConfig,
    pipeline_name: str,
    last_start: date,
    last_end: date,
    next_start: date,
    next_end: date,
) -> None:
    """
    Upsert dbo.pipeline_checkpoint.
    BEST EFFORT: Failures are logged but don't stop the pipeline.
    """
    try:
        with get_sql_connection(config) as cn:
            cur = cn.cursor()
            cur.execute(
                """
                MERGE dbo.pipeline_checkpoint AS target
                USING (SELECT ? AS pipeline_name) AS source
                ON target.pipeline_name = source.pipeline_name
                WHEN MATCHED THEN
                  UPDATE SET
                    last_successful_week_start = ?,
                    last_successful_week_end   = ?,
                    next_week_start            = ?,
                    next_week_end              = ?,
                    updated_at_utc             = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                  INSERT (pipeline_name, last_successful_week_start, last_successful_week_end, next_week_start, next_week_end)
                  VALUES (?, ?, ?, ?, ?);
                """,
                pipeline_name,
                last_start,
                last_end,
                next_start,
                next_end,
                pipeline_name,
                last_start,
                last_end,
                next_start,
                next_end,
            )
            cn.commit()
        logger.info("DB checkpoint updated successfully")
    except Exception as e:
        logger.error("DB checkpoint update failed", extra={"error": str(e), "pipeline_name": pipeline_name})
        raise


def db_insert_pipeline_run(config: ExtractorConfig, pipeline_name: str, metrics: PipelineMetrics) -> str:
    """
    Insert dbo.pipeline_run row; returns run_id (GUID string).
    BEST EFFORT: Failures are logged but don't stop the pipeline.
    """
    run_id = str(uuid.uuid4())
    try:
        with get_sql_connection(config) as cn:
            cur = cn.cursor()
            cur.execute(
                """
                INSERT INTO dbo.pipeline_run (
                  run_id, pipeline_name, batch_week_start, batch_week_end,
                  pipeline_start_time, status, correlation_id,
                  reject_threshold_pct, created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME())
                """,
                run_id,
                pipeline_name,
                config.week_start,
                config.week_end,
                metrics.start_time,
                "STARTED",
                metrics.correlation_id,
                metrics.reject_threshold_pct,
            )
            cn.commit()
        logger.info("Pipeline run inserted to DB", extra={"run_id": run_id})
    except Exception as e:
        logger.error("DB run insert failed", extra={"error": str(e), "run_id": run_id})
        raise

    return run_id


def db_finalize_pipeline_run(config: ExtractorConfig, run_id: str, metrics: PipelineMetrics) -> None:
    """
    Update dbo.pipeline_run with final metrics.
    BEST EFFORT: Failures are logged but don't stop the pipeline.
    """
    try:
        with get_sql_connection(config) as cn:
            cur = cn.cursor()
            cur.execute(
                """
                UPDATE dbo.pipeline_run
                SET
                  pipeline_end_time    = ?,
                  status               = ?,
                  rows_fetched         = ?,
                  rows_output          = ?,
                  rows_rejected        = ?,
                  reject_rate_pct      = ?,
                  reject_threshold_pct = ?,
                  output_path          = ?,
                  rejects_path         = ?,
                  error_type           = ?,
                  error_message        = ?
                WHERE run_id = ?
                """,
                metrics.end_time,
                metrics.run_status,
                metrics.rows_fetched,
                metrics.rows_output,
                metrics.rows_rejected,
                metrics.reject_rate_pct,
                metrics.reject_threshold_pct,
                metrics.output_file_path or None,
                metrics.rejects_file_path or None,
                metrics.error_type or None,
                metrics.error_message or None,
                run_id,
            )
            cn.commit()
        logger.info("Pipeline run finalized in DB", extra={"run_id": run_id})
    except Exception as e:
        logger.error("DB run finalize failed", extra={"error": str(e), "run_id": run_id})
        raise


def db_stage_start(config: ExtractorConfig, run_id: str, stage_name: str) -> int:
    """
    Insert dbo.pipeline_stage; returns stage_id.
    BEST EFFORT: Failures are logged but don't stop the pipeline.
    """
    try:
        with get_sql_connection(config) as cn:
            cur = cn.cursor()
            cur.execute(
                """
                INSERT INTO dbo.pipeline_stage (run_id, stage_name, stage_start_time, status)
                OUTPUT INSERTED.stage_id
                VALUES (?, ?, SYSUTCDATETIME(), ?)
                """,
                run_id,
                stage_name,
                "STARTED",
            )
            stage_id = cur.fetchone()[0]
            cn.commit()
        logger.info("Stage started in DB", extra={"run_id": run_id, "stage_name": stage_name, "stage_id": stage_id})
        return int(stage_id)
    except Exception as e:
        logger.error("DB stage start failed", extra={"error": str(e), "run_id": run_id, "stage_name": stage_name})
        raise


def db_stage_end(
    config: ExtractorConfig,
    stage_id: int,
    status: str,
    row_count: int | None = None,
    error_message: str | None = None,
) -> None:
    """
    Update dbo.pipeline_stage with completion status.
    BEST EFFORT: Failures are logged but don't stop the pipeline.
    """
    try:
        with get_sql_connection(config) as cn:
            cur = cn.cursor()
            cur.execute(
                """
                UPDATE dbo.pipeline_stage
                SET stage_end_time = SYSUTCDATETIME(),
                    status = ?,
                    row_count = ?,
                    error_message = ?
                WHERE stage_id = ?
                """,
                status,
                row_count,
                error_message,
                stage_id,
            )
            cn.commit()
        logger.info("Stage ended in DB", extra={"stage_id": stage_id, "status": status})
    except Exception as e:
        logger.error("DB stage end failed", extra={"error": str(e), "stage_id": stage_id})
        raise


def flush_pending_db_updates(config: ExtractorConfig) -> None:
    """
    Auto-replay pending DB updates from previous failed runs.
    This is called AFTER DB connection is established but BEFORE reading checkpoint.

    Production-safe:
    - If flush succeeds -> remove from pending file
    - If flush fails -> keep in pending file for next run
    - Pipeline continues regardless of flush outcome
    """
    pending = read_pending_db_updates(config)
    if not pending:
        logger.debug("No pending DB updates to flush")
        return

    logger.info("Found pending DB updates to replay", extra={"count": len(pending)})

    remaining: list[dict] = []
    flushed_count = 0

    for idx, item in enumerate(pending, 1):
        correlation_id = None
        try:
            run_payload = item.get("run")
            checkpoint_payload = item.get("checkpoint")
            failed_at_utc = item.get("failed_at_utc")

            if run_payload:
                correlation_id = run_payload.get("correlation_id")

            logger.info(
                f"Replaying pending update {idx}/{len(pending)}",
                extra={
                    "correlation_id": correlation_id,
                    "originally_failed_at": failed_at_utc,
                    "has_run_payload": run_payload is not None,
                    "has_checkpoint_payload": checkpoint_payload is not None,
                }
            )

            # Replay checkpoint update
            if checkpoint_payload:
                pipeline_name = checkpoint_payload.get("pipeline_name")
                last_start = date.fromisoformat(checkpoint_payload["last_successful_week_start"])
                last_end = date.fromisoformat(checkpoint_payload["last_successful_week_end"])
                next_start = date.fromisoformat(checkpoint_payload["next_week_start"])
                next_end = date.fromisoformat(checkpoint_payload["next_week_end"])

                db_update_checkpoint(
                    config,
                    pipeline_name=pipeline_name,
                    last_start=last_start,
                    last_end=last_end,
                    next_start=next_start,
                    next_end=next_end,
                )
                logger.info(
                    "Successfully replayed checkpoint update",
                    extra={"correlation_id": correlation_id}
                )

            flushed_count += 1

        except Exception as e:
            logger.error(
                "Failed to replay pending DB update; keeping for next run",
                extra={
                    "correlation_id": correlation_id,
                    "error": str(e),
                    "update_index": idx,
                }
            )
            remaining.append(item)

    # Rewrite file with only remaining (unflushed) updates
    rewrite_pending_db_updates(config, remaining)

    logger.info(
        "Pending DB updates flush complete",
        extra={
            "total_pending": len(pending),
            "successfully_flushed": flushed_count,
            "remaining": len(remaining),
        }
    )

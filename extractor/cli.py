"""CLI entry point for the extraction pipeline."""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from extractor.azure_sql import db_get_checkpoint, db_update_checkpoint, flush_pending_db_updates
from extractor.checkpoint import append_pending_db_update, compute_week_end, write_json_state
from extractor.config import get_config
from extractor.exceptions import CheckpointError, ConfigurationError, ExtractorError
from extractor.extraction import check_api_health
from extractor.logging_utils import get_logger, setup_logging
from extractor.models import RunStatus
from extractor.pipeline import (
    run_extract_stage,
    run_load_stage,
    run_pipeline,
    run_quality_gate_stage,
    run_transform_stage,
    set_run_tag,
)
from extractor.storage import check_output_exists

logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract weekly event data from API to data warehouse",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m extractor.cli
  python -m extractor.cli --week-start 2024-01-08 --week-end 2024-01-14
  python -m extractor.cli --use-checkpoint
  python -m extractor.cli --use-checkpoint --stage extract
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

    # Airflow-friendly resume
    parser.add_argument("--use-checkpoint", action="store_true", help="Use DB checkpoint to select next week to run")
    parser.add_argument("--state-path", type=str, default=None, help="Override checkpoint file path")
    parser.add_argument(
        "--advance-on-skip",
        action="store_true",
        help="If output exists and pipeline skips, still advance checkpoint to next week",
    )

    # Stage mode for multi-task DAG
    parser.add_argument(
        "--stage",
        choices=["extract", "transform", "load", "quality-gate", "salesforce-sync", "full"],
        default="full",
        help="Run a specific pipeline stage (for Airflow multi-task DAG)",
    )

    parsed = parser.parse_args()

    # Validate --week-start and --week-end are provided together
    if bool(parsed.week_start) != bool(parsed.week_end):
        parser.error("Both --week-start and --week-end must be provided together")

    return parsed


def main() -> int:
    """Main entry point for CLI execution."""
    args = parse_args()
    setup_logging(level=args.log_level, json_format=(args.log_format == "json"))

    # Set run tag for stage metadata namespacing (Airflow run_id or date-based)
    import os
    run_id = os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", f"{date.today().isoformat()}")
    set_run_tag(run_id.replace("/", "_").replace(":", "_")[:64])

    try:
        config = get_config()

        # Optional: override checkpoint path
        if args.state_path:
            p = Path(args.state_path)
            config.state_dir = p.parent
            config.state_file_name = p.name

        pipeline_name = "reverse_etl_events"

        # ============================================================
        # AUTO-REPLAY PENDING DB UPDATES
        # ============================================================
        try:
            flush_pending_db_updates(config)
        except Exception as e:
            logger.warning(
                "Failed to flush pending DB updates; will retry on next run",
                extra={"error": str(e)}
            )

        # ============================================================
        # DB-FIRST CHECKPOINT PATTERN
        # ============================================================
        if args.use_checkpoint and (not args.week_start) and (not args.week_end):
            checkpoint = db_get_checkpoint(config, pipeline_name)

            if checkpoint and checkpoint.get("next_week_start"):
                config.week_start = date.fromisoformat(checkpoint["next_week_start"])
                config.week_end = date.fromisoformat(checkpoint["next_week_end"])
                logger.info(
                    "Loaded checkpoint from DB",
                    extra={"week_start": str(config.week_start), "week_end": str(config.week_end)}
                )
            else:
                logger.info(
                    "No checkpoint found in DB; using config.env week range",
                    extra={"week_start": str(config.week_start), "week_end": str(config.week_end)}
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

        # ============================================================
        # STAGE-SPECIFIC EXECUTION (for multi-task DAG)
        # ============================================================
        if args.stage != "full":
            return _run_stage(args.stage, config, args)

        # ============================================================
        # FULL PIPELINE (backward compatible)
        # ============================================================
        metrics = run_pipeline(
            config=config,
            skip_if_exists=not args.force,
            upload_to_cloud=not args.local_only,
            dry_run=args.dry_run,
        )

        # ============================================================
        # POST-RUN: CHECKPOINT UPDATE (DB + JSON SYNC)
        # ============================================================
        if (not args.dry_run) and args.use_checkpoint:
            _update_checkpoint(config, metrics, args, pipeline_name)

        return 0 if metrics.success else 1

    except CheckpointError as e:
        logger.error(f"Checkpoint error: {e}")
        return 1
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


def _run_stage(stage: str, config, args) -> int:
    """Execute a single pipeline stage."""
    try:
        if stage == "extract":
            run_extract_stage(config)
        elif stage == "transform":
            run_transform_stage(config)
        elif stage == "load":
            run_load_stage(config, upload_to_cloud=not args.local_only)
        elif stage == "quality-gate":
            run_quality_gate_stage(config)
        elif stage == "salesforce-sync":
            from extractor.salesforce_sync import run_salesforce_sync
            result = run_salesforce_sync(config)
            logger.info("Salesforce sync result", extra=result)
            return 0 if result.get("error_count", 0) == 0 else 1
        return 0
    except ExtractorError as e:
        logger.error(f"Stage '{stage}' failed: {e}")
        return 1
    except Exception as e:
        logger.exception(f"Unexpected error in stage '{stage}': {e}")
        return 1


def _update_checkpoint(config, metrics, args, pipeline_name: str) -> None:
    """Post-run checkpoint update logic."""
    was_skipped = (metrics.rows_fetched == 0) and (not args.force) and check_output_exists(config)
    ok_to_advance = metrics.run_status in {RunStatus.SUCCESS.value, RunStatus.SUCCESS_WITH_REJECTS.value}

    if ok_to_advance and ((not was_skipped) or args.advance_on_skip):
        next_week_start = config.week_end + timedelta(days=1)
        next_week_end = compute_week_end(next_week_start)

        checkpoint_payload = {
            "pipeline_name": pipeline_name,
            "last_successful_week_start": str(config.week_start),
            "last_successful_week_end": str(config.week_end),
            "next_week_start": str(next_week_start),
            "next_week_end": str(next_week_end),
        }

        run_payload = {
            "correlation_id": metrics.correlation_id,
            "run_status": metrics.run_status,
            "reject_rate_pct": round(metrics.reject_rate_pct, 6),
            "reject_threshold_pct": round(metrics.reject_threshold_pct, 6),
            "output_file_path": metrics.output_file_path,
            "rows_output": metrics.rows_output,
            "rows_rejected": metrics.rows_rejected,
            "rejects_file_path": metrics.rejects_file_path,
            "rejects_adls_path": metrics.rejects_adls_path,
            "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        }

        db_write_failed = False

        # BEST EFFORT: Write to DB
        try:
            db_update_checkpoint(
                config,
                pipeline_name=pipeline_name,
                last_start=config.week_start,
                last_end=config.week_end,
                next_start=next_week_start,
                next_end=next_week_end,
            )
            logger.info(
                "DB checkpoint updated",
                extra={
                    "pipeline_name": pipeline_name,
                    "next_week_start": str(next_week_start),
                    "next_week_end": str(next_week_end)
                }
            )
        except Exception as e:
            db_write_failed = True
            logger.error("DB WRITE FAILED after extraction", extra={"error": str(e)})

        # ALWAYS: Sync JSON (mirror/backup)
        json_state = {
            **checkpoint_payload,
            "last_run": run_payload,
        }
        write_json_state(config, json_state)

        # IF DB WRITE FAILED: Save to pending updates file
        if db_write_failed:
            append_pending_db_update(config, {
                "checkpoint": checkpoint_payload,
                "run": run_payload,
                "failed_at_utc": datetime.now(timezone.utc).isoformat(),
                "reason": "db_checkpoint_update_failed",
            })

    else:
        if not ok_to_advance:
            logger.warning(
                "Checkpoint not advanced because run_status is FAILED",
                extra={
                    "run_status": metrics.run_status,
                    "reject_rate_pct": round(metrics.reject_rate_pct, 4),
                    "reject_threshold_pct": round(metrics.reject_threshold_pct, 4),
                },
            )
        else:
            logger.info("Pipeline skipped and advance-on-skip not set; checkpoint not advanced")


if __name__ == "__main__":
    sys.exit(main())

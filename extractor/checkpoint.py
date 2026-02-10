"""JSON-based checkpoint and pending DB update management."""

from __future__ import annotations

import json
from datetime import date, timedelta

from extractor.config import ExtractorConfig
from extractor.logging_utils import get_logger
from extractor.storage import ensure_dir

logger = get_logger(__name__)


def compute_week_end(week_start: date, days: int = 6) -> date:
    return week_start + timedelta(days=days)


def read_json_state(config: ExtractorConfig) -> dict | None:
    """Read state from JSON file (backup/mirror only)."""
    path = config.state_file
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception as e:
        logger.warning("Failed to read JSON state file", extra={"path": str(path), "error": str(e)})
        return None


def write_json_state(config: ExtractorConfig, state: dict) -> None:
    """Write state to JSON file (always sync as mirror/backup)."""
    try:
        ensure_dir(config.state_dir)
        config.state_file.write_text(json.dumps(state, indent=2), encoding="utf-8")
        logger.info("JSON state synchronized", extra={"state_path": str(config.state_file)})
    except Exception as e:
        logger.error("Failed to write JSON state file", extra={"path": str(config.state_file), "error": str(e)})


def append_pending_db_update(config: ExtractorConfig, payload: dict) -> None:
    """Append a failed DB update to pending file (JSONL format - one update per line)."""
    try:
        ensure_dir(config.pending_updates_file.parent)
        with config.pending_updates_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload) + "\n")
        logger.warning(
            "DB update saved to pending file for replay on next run",
            extra={"pending_file": str(config.pending_updates_file)}
        )
    except Exception as e:
        logger.error("Failed to save pending DB update", extra={"error": str(e)})


def read_pending_db_updates(config: ExtractorConfig) -> list[dict]:
    """Read all pending DB updates from JSONL file."""
    if not config.pending_updates_file.exists():
        return []

    updates: list[dict] = []
    with config.pending_updates_file.open("r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                updates.append(json.loads(line))
            except Exception as e:
                # Skip bad lines instead of breaking everything
                logger.warning(
                    "Skipping malformed pending update",
                    extra={"line_num": line_num, "error": str(e)}
                )
                continue
    return updates


def rewrite_pending_db_updates(config: ExtractorConfig, remaining: list[dict]) -> None:
    """Rewrite pending updates file with only remaining (unflushed) updates.

    Uses write-to-temp + atomic rename to avoid data loss on crash.
    """
    ensure_dir(config.pending_updates_file.parent)
    temp_file = config.pending_updates_file.parent / f"{config.pending_updates_file.name}.tmp"
    try:
        with temp_file.open("w", encoding="utf-8") as f:
            for item in remaining:
                f.write(json.dumps(item) + "\n")
        temp_file.replace(config.pending_updates_file)
    except Exception:
        if temp_file.exists():
            temp_file.unlink()
        raise

"""Tests for extractor.cli — argument parsing, main return codes, stage dispatch, checkpoint update."""

from __future__ import annotations

import sys
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from extractor.cli import _run_stage, _update_checkpoint, main, parse_args
from extractor.exceptions import CheckpointError, ConfigurationError, DataQualityError, ExtractorError
from extractor.models import PipelineMetrics, RunStatus


# ---------------------------------------------------------------------------
# parse_args tests
# ---------------------------------------------------------------------------

class TestParseArgs:
    def test_defaults(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["cli"])
        args = parse_args()
        assert args.stage == "full"
        assert args.log_level == "INFO"
        assert args.log_format == "json"
        assert args.dry_run is False
        assert args.force is False
        assert args.use_checkpoint is False

    def test_week_dates(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["cli", "--week-start", "2024-01-08", "--week-end", "2024-01-14"])
        args = parse_args()
        assert args.week_start == "2024-01-08"
        assert args.week_end == "2024-01-14"

    def test_week_start_without_end_errors(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["cli", "--week-start", "2024-01-08"])
        with pytest.raises(SystemExit):
            parse_args()

    def test_week_end_without_start_errors(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["cli", "--week-end", "2024-01-14"])
        with pytest.raises(SystemExit):
            parse_args()

    def test_stage_choices(self, monkeypatch):
        for stage in ("extract", "transform", "load", "quality-gate", "salesforce-sync", "full"):
            monkeypatch.setattr(sys, "argv", ["cli", "--stage", stage])
            args = parse_args()
            assert args.stage == stage

    def test_invalid_stage_errors(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["cli", "--stage", "invalid"])
        with pytest.raises(SystemExit):
            parse_args()

    def test_all_flags(self, monkeypatch):
        monkeypatch.setattr(sys, "argv", [
            "cli", "--dry-run", "--force", "--local-only",
            "--use-checkpoint", "--advance-on-skip",
            "--log-level", "DEBUG", "--log-format", "text",
        ])
        args = parse_args()
        assert args.dry_run is True
        assert args.force is True
        assert args.local_only is True
        assert args.use_checkpoint is True
        assert args.advance_on_skip is True
        assert args.log_level == "DEBUG"
        assert args.log_format == "text"


# ---------------------------------------------------------------------------
# main return codes
# ---------------------------------------------------------------------------

class TestMainReturnCodes:
    @patch("extractor.cli.parse_args")
    @patch("extractor.cli.setup_logging")
    @patch("extractor.cli.get_config")
    @patch("extractor.cli.flush_pending_db_updates")
    @patch("extractor.cli.run_pipeline")
    @patch("extractor.cli.set_run_tag")
    def test_success_returns_0(self, mock_tag, mock_pipeline, mock_flush, mock_config, mock_log, mock_args):
        mock_args.return_value = MagicMock(
            log_level="INFO", log_format="json", week_start=None, week_end=None,
            use_checkpoint=False, state_path=None, check_api=False, stage="full",
            force=False, local_only=False, dry_run=False,
        )
        mock_config.return_value = MagicMock(week_start=date(2024, 1, 8), week_end=date(2024, 1, 14))
        metrics = PipelineMetrics()
        metrics.success = True
        mock_pipeline.return_value = metrics
        assert main() == 0

    @patch("extractor.cli.parse_args")
    @patch("extractor.cli.setup_logging")
    @patch("extractor.cli.get_config")
    def test_config_error_returns_1(self, mock_config, mock_log, mock_args):
        mock_args.return_value = MagicMock(
            log_level="INFO", log_format="json", state_path=None,
        )
        mock_config.side_effect = ConfigurationError("bad config")
        assert main() == 1

    @patch("extractor.cli.parse_args")
    @patch("extractor.cli.setup_logging")
    @patch("extractor.cli.get_config")
    @patch("extractor.cli.flush_pending_db_updates")
    @patch("extractor.cli.db_get_checkpoint")
    @patch("extractor.cli.set_run_tag")
    def test_checkpoint_error_returns_1(self, mock_tag, mock_ckpt, mock_flush, mock_config, mock_log, mock_args):
        mock_args.return_value = MagicMock(
            log_level="INFO", log_format="json", week_start=None, week_end=None,
            use_checkpoint=True, state_path=None, check_api=False, stage="full",
        )
        mock_config.return_value = MagicMock(week_start=date(2024, 1, 8), week_end=date(2024, 1, 14))
        mock_ckpt.side_effect = CheckpointError("DB down")
        assert main() == 1

    @patch("extractor.cli.parse_args")
    @patch("extractor.cli.setup_logging")
    @patch("extractor.cli.get_config")
    @patch("extractor.cli.flush_pending_db_updates")
    @patch("extractor.cli.run_pipeline")
    @patch("extractor.cli.set_run_tag")
    def test_pipeline_failure_returns_1(self, mock_tag, mock_pipeline, mock_flush, mock_config, mock_log, mock_args):
        mock_args.return_value = MagicMock(
            log_level="INFO", log_format="json", week_start=None, week_end=None,
            use_checkpoint=False, state_path=None, check_api=False, stage="full",
            force=False, local_only=False, dry_run=False,
        )
        mock_config.return_value = MagicMock(week_start=date(2024, 1, 8), week_end=date(2024, 1, 14))
        mock_pipeline.side_effect = DataQualityError("threshold exceeded")
        assert main() == 1


# ---------------------------------------------------------------------------
# _run_stage tests
# ---------------------------------------------------------------------------

class TestRunStage:
    def _make_args(self, **kwargs):
        defaults = {"local_only": False}
        defaults.update(kwargs)
        return MagicMock(**defaults)

    @patch("extractor.cli.run_extract_stage")
    def test_extract_stage(self, mock_extract):
        mock_extract.return_value = {"rows_fetched": 10}
        assert _run_stage("extract", MagicMock(), self._make_args()) == 0
        mock_extract.assert_called_once()

    @patch("extractor.cli.run_transform_stage")
    def test_transform_stage(self, mock_transform):
        mock_transform.return_value = {"rows_output": 10}
        assert _run_stage("transform", MagicMock(), self._make_args()) == 0

    @patch("extractor.cli.run_load_stage")
    def test_load_stage(self, mock_load):
        mock_load.return_value = {}
        assert _run_stage("load", MagicMock(), self._make_args()) == 0

    @patch("extractor.cli.run_quality_gate_stage")
    def test_quality_gate_stage(self, mock_qg):
        mock_qg.return_value = {"quality_gate": "PASSED"}
        assert _run_stage("quality-gate", MagicMock(), self._make_args()) == 0

    @patch("extractor.cli.run_quality_gate_stage")
    def test_quality_gate_failure_returns_1(self, mock_qg):
        mock_qg.side_effect = DataQualityError("threshold exceeded")
        assert _run_stage("quality-gate", MagicMock(), self._make_args()) == 1

    @patch("extractor.cli.run_extract_stage")
    def test_unexpected_error_returns_1(self, mock_extract):
        mock_extract.side_effect = RuntimeError("unexpected")
        assert _run_stage("extract", MagicMock(), self._make_args()) == 1


# ---------------------------------------------------------------------------
# _update_checkpoint tests
# ---------------------------------------------------------------------------

class TestUpdateCheckpoint:
    def _make_metrics(self, success=True, status="SUCCESS"):
        m = PipelineMetrics(correlation_id="test-corr-id")
        m.success = success
        m.run_status = status
        m.rows_fetched = 10
        m.rows_output = 10
        m.rows_rejected = 0
        m.reject_rate_pct = 0.0
        m.reject_threshold_pct = 5.0
        m.output_file_path = "/tmp/output.parquet"
        return m

    def _make_args(self, force=False, advance_on_skip=False):
        return MagicMock(force=force, advance_on_skip=advance_on_skip)

    @patch("extractor.cli.write_json_state")
    @patch("extractor.cli.db_update_checkpoint")
    @patch("extractor.cli.check_output_exists", return_value=False)
    def test_success_advances_checkpoint(self, mock_exists, mock_db, mock_json, sample_config):
        metrics = self._make_metrics()
        _update_checkpoint(sample_config, metrics, self._make_args(), "test_pipeline")
        mock_db.assert_called_once()
        mock_json.assert_called_once()
        # Verify next week computed correctly
        call_kwargs = mock_db.call_args
        assert call_kwargs[1]["next_start"] == sample_config.week_end + timedelta(days=1)

    @patch("extractor.cli.append_pending_db_update")
    @patch("extractor.cli.write_json_state")
    @patch("extractor.cli.db_update_checkpoint")
    @patch("extractor.cli.check_output_exists", return_value=False)
    def test_db_failure_queues_pending(self, mock_exists, mock_db, mock_json, mock_pending, sample_config):
        mock_db.side_effect = Exception("DB connection failed")
        metrics = self._make_metrics()
        _update_checkpoint(sample_config, metrics, self._make_args(), "test_pipeline")
        mock_pending.assert_called_once()
        mock_json.assert_called_once()

    @patch("extractor.cli.write_json_state")
    @patch("extractor.cli.db_update_checkpoint")
    @patch("extractor.cli.check_output_exists", return_value=False)
    def test_failed_status_does_not_advance(self, mock_exists, mock_db, mock_json, sample_config):
        metrics = self._make_metrics(success=False, status="FAILED")
        _update_checkpoint(sample_config, metrics, self._make_args(), "test_pipeline")
        mock_db.assert_not_called()
        mock_json.assert_not_called()

    @patch("extractor.cli.write_json_state")
    @patch("extractor.cli.db_update_checkpoint")
    @patch("extractor.cli.check_output_exists", return_value=True)
    def test_skipped_without_advance_flag_does_not_advance(self, mock_exists, mock_db, mock_json, sample_config):
        metrics = self._make_metrics()
        metrics.rows_fetched = 0  # Skipped
        _update_checkpoint(sample_config, metrics, self._make_args(advance_on_skip=False), "test_pipeline")
        mock_db.assert_not_called()

    @patch("extractor.cli.write_json_state")
    @patch("extractor.cli.db_update_checkpoint")
    @patch("extractor.cli.check_output_exists", return_value=True)
    def test_skipped_with_advance_flag_advances(self, mock_exists, mock_db, mock_json, sample_config):
        metrics = self._make_metrics()
        metrics.rows_fetched = 0  # Skipped
        _update_checkpoint(sample_config, metrics, self._make_args(advance_on_skip=True), "test_pipeline")
        mock_db.assert_called_once()

    @patch("extractor.cli.write_json_state")
    @patch("extractor.cli.db_update_checkpoint")
    @patch("extractor.cli.check_output_exists", return_value=False)
    def test_year_boundary_crossing(self, mock_exists, mock_db, mock_json, sample_config):
        sample_config.week_start = date(2024, 12, 23)
        sample_config.week_end = date(2024, 12, 29)
        metrics = self._make_metrics()
        _update_checkpoint(sample_config, metrics, self._make_args(), "test_pipeline")
        call_kwargs = mock_db.call_args
        assert call_kwargs[1]["next_start"] == date(2024, 12, 30)
        assert call_kwargs[1]["next_end"] == date(2025, 1, 5)

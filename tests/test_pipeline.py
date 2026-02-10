"""Tests for extractor.pipeline — run_pipeline with mocked dependencies."""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from extractor.exceptions import DataQualityError, ExtractorError
from extractor.models import RunStatus
from extractor.pipeline import (
    _read_stage_meta,
    _write_stage_meta,
    compute_reject_rate_pct,
    decide_run_status,
    run_pipeline,
    set_run_tag,
)


class TestComputeRejectRatePct:
    def test_zero_fetched(self):
        assert compute_reject_rate_pct(5, 0) == 0.0

    def test_no_rejects(self):
        assert compute_reject_rate_pct(0, 1000) == 0.0

    def test_some_rejects(self):
        assert compute_reject_rate_pct(50, 1000) == 5.0


class TestDecideRunStatus:
    def test_success_no_rejects(self):
        assert decide_run_status(0.0, 0, 5.0) == RunStatus.SUCCESS.value

    def test_success_with_rejects_under_threshold(self):
        assert decide_run_status(2.0, 20, 5.0) == RunStatus.SUCCESS_WITH_REJECTS.value

    def test_failed_over_threshold(self):
        assert decide_run_status(6.0, 60, 5.0) == RunStatus.FAILED.value

    def test_at_threshold(self):
        assert decide_run_status(5.0, 50, 5.0) == RunStatus.SUCCESS_WITH_REJECTS.value


@patch("extractor.pipeline.db_finalize_pipeline_run")
@patch("extractor.pipeline.db_stage_end")
@patch("extractor.pipeline.db_stage_start")
@patch("extractor.pipeline.db_insert_pipeline_run", return_value="test-run-id")
@patch("extractor.pipeline.save_events")
@patch("extractor.pipeline.transform_events")
@patch("extractor.pipeline.fetch_events")
class TestRunPipeline:
    def test_successful_run(
        self, mock_fetch, mock_transform, mock_save,
        mock_db_insert, mock_db_stage_start, mock_db_stage_end, mock_db_finalize,
        sample_config,
    ):
        mock_fetch.return_value = [{"event_type": "v", "user_id": "1", "event_time": "2024-01-10T00:00:00"}]
        mock_transform.return_value = (
            pd.DataFrame({"user_id": ["1"]}),
            pd.DataFrame(),
        )
        mock_save.return_value = {
            "local": {"path": "/tmp/events.parquet", "size_bytes": 100, "sha256": "abc"},
            "adls": None,
            "rejects_local": None,
            "rejects_adls": None,
        }

        metrics = run_pipeline(config=sample_config, skip_if_exists=False, upload_to_cloud=False)
        assert metrics.success is True
        assert metrics.run_status == RunStatus.SUCCESS.value
        assert metrics.rows_fetched == 1

    def test_dry_run_skips_output(
        self, mock_fetch, mock_transform, mock_save,
        mock_db_insert, mock_db_stage_start, mock_db_stage_end, mock_db_finalize,
        sample_config,
    ):
        mock_fetch.return_value = [{"event_type": "v", "user_id": "1", "event_time": "2024-01-10T00:00:00"}]
        mock_transform.return_value = (
            pd.DataFrame({"user_id": ["1"]}),
            pd.DataFrame(),
        )

        metrics = run_pipeline(config=sample_config, dry_run=True, skip_if_exists=False)
        assert metrics.success is True
        mock_save.assert_not_called()

    def test_skip_if_exists(
        self, mock_fetch, mock_transform, mock_save,
        mock_db_insert, mock_db_stage_start, mock_db_stage_end, mock_db_finalize,
        sample_config,
    ):
        # Create the output file so skip_if_exists triggers
        output_file = sample_config.local_output_file
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text("existing")

        metrics = run_pipeline(config=sample_config, skip_if_exists=True)
        assert metrics.success is True
        mock_fetch.assert_not_called()

    def test_reject_threshold_breach_raises(
        self, mock_fetch, mock_transform, mock_save,
        mock_db_insert, mock_db_stage_start, mock_db_stage_end, mock_db_finalize,
        sample_config,
    ):
        sample_config.reject_threshold_pct = 1.0  # Very low threshold
        mock_fetch.return_value = [{"id": i} for i in range(100)]

        # 10 out of 100 rejected = 10%, exceeds 1% threshold
        df_valid = pd.DataFrame({"user_id": [str(i) for i in range(90)]})
        df_rejects = pd.DataFrame({"row_index": list(range(10))})
        mock_transform.return_value = (df_valid, df_rejects)
        mock_save.return_value = {
            "local": {"path": "/tmp/events.parquet", "size_bytes": 100, "sha256": "abc"},
            "adls": None,
            "rejects_local": {"path": "/tmp/rejects.parquet", "size_bytes": 50, "sha256": "def"},
            "rejects_adls": None,
        }

        with pytest.raises(DataQualityError):
            run_pipeline(config=sample_config, skip_if_exists=False, upload_to_cloud=False)

    def test_db_insert_failure_aborts_pipeline(
        self, mock_fetch, mock_transform, mock_save,
        mock_db_insert, mock_db_stage_start, mock_db_stage_end, mock_db_finalize,
        sample_config,
    ):
        mock_db_insert.side_effect = Exception("DB connection refused")

        with pytest.raises(ExtractorError, match="unable to insert pipeline run"):
            run_pipeline(config=sample_config, skip_if_exists=False)
        mock_fetch.assert_not_called()


class TestStageMetadata:
    def test_write_and_read_roundtrip(self, sample_config):
        set_run_tag("test-run")
        meta = {"rows_fetched": 42, "raw_events_path": "/tmp/raw.json"}
        _write_stage_meta(sample_config, "extract", meta)
        loaded = _read_stage_meta(sample_config, "extract")
        assert loaded["rows_fetched"] == 42
        assert loaded["raw_events_path"] == "/tmp/raw.json"

    def test_read_missing_raises(self, sample_config):
        set_run_tag("nonexistent-run")
        with pytest.raises(ExtractorError, match="Stage metadata not found"):
            _read_stage_meta(sample_config, "extract")

    def test_different_run_tags_isolated(self, sample_config):
        set_run_tag("run-a")
        _write_stage_meta(sample_config, "extract", {"tag": "a"})

        set_run_tag("run-b")
        _write_stage_meta(sample_config, "extract", {"tag": "b"})

        set_run_tag("run-a")
        assert _read_stage_meta(sample_config, "extract")["tag"] == "a"

        set_run_tag("run-b")
        assert _read_stage_meta(sample_config, "extract")["tag"] == "b"

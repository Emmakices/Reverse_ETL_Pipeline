"""Tests for extractor.transformation — validate, normalize, quality checks."""

import pandas as pd
import pytest

from extractor.exceptions import DataQualityError
from extractor.transformation import (
    OUTPUT_COLUMNS,
    normalize_events,
    normalize_rejects,
    run_data_quality_checks,
    transform_events,
    validate_events,
)


class TestValidateEvents:
    def test_valid_events_pass(self, sample_raw_events):
        valid, rejected, result = validate_events(sample_raw_events)
        assert len(valid) == 3
        assert len(rejected) == 0
        assert result.success_rate == 100.0

    def test_invalid_events_rejected(self, sample_invalid_events):
        valid, rejected, result = validate_events(sample_invalid_events)
        assert len(rejected) > 0
        assert result.invalid_records > 0

    def test_mixed_events(self, sample_raw_events, sample_invalid_events):
        mixed = sample_raw_events + sample_invalid_events
        valid, rejected, result = validate_events(mixed)
        assert len(valid) == 3
        assert len(rejected) == len(sample_invalid_events)
        assert result.total_records == len(mixed)

    def test_rejected_event_contains_metadata(self, sample_invalid_events):
        _, rejected, _ = validate_events(sample_invalid_events)
        for rej in rejected:
            assert "row_index" in rej
            assert "error_count" in rej
            assert "errors" in rej
            assert "rejected_at" in rej


class TestNormalizeEvents:
    def test_output_has_correct_columns(self, sample_raw_events, sample_config):
        valid, _, _ = validate_events(sample_raw_events)
        df = normalize_events(valid, sample_config)
        assert list(df.columns) == OUTPUT_COLUMNS

    def test_batch_metadata_added(self, sample_raw_events, sample_config):
        valid, _, _ = validate_events(sample_raw_events)
        df = normalize_events(valid, sample_config)
        assert df["batch_week_start"].iloc[0] == str(sample_config.week_start)
        assert df["batch_week_end"].iloc[0] == str(sample_config.week_end)
        assert df["ingested_at"].notna().all()

    def test_empty_input_returns_empty_df(self, sample_config):
        df = normalize_events([], sample_config)
        assert len(df) == 0
        assert list(df.columns) == OUTPUT_COLUMNS


class TestNormalizeRejects:
    def test_empty_rejects(self, sample_config):
        df = normalize_rejects([], sample_config)
        assert len(df) == 0

    def test_rejects_have_batch_metadata(self, sample_invalid_events, sample_config):
        _, rejected, _ = validate_events(sample_invalid_events)
        df = normalize_rejects(rejected, sample_config)
        assert len(df) > 0
        assert "batch_week_start" in df.columns


class TestRunDataQualityChecks:
    def test_passes_with_good_data(self, sample_raw_events, sample_config):
        valid, _, _ = validate_events(sample_raw_events)
        df = normalize_events(valid, sample_config)
        checks = run_data_quality_checks(df, sample_config, min_records=1)
        assert checks["passed"] is True

    def test_fails_on_empty_df_with_min_records(self, sample_config):
        df = pd.DataFrame(columns=OUTPUT_COLUMNS)
        checks = run_data_quality_checks(df, sample_config, min_records=1)
        assert checks["passed"] is False
        assert checks["checks"]["min_records"]["passed"] is False

    def test_passes_on_empty_df_with_min_zero(self, sample_config):
        df = pd.DataFrame(columns=OUTPUT_COLUMNS)
        checks = run_data_quality_checks(df, sample_config, min_records=0)
        assert checks["passed"] is True


class TestTransformEvents:
    def test_end_to_end(self, sample_raw_events, sample_config):
        df_valid, df_rejects = transform_events(sample_raw_events, sample_config, min_records=1)
        assert len(df_valid) == 3
        assert list(df_valid.columns) == OUTPUT_COLUMNS

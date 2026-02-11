"""Tests for extractor.azure_sql — connection, checkpoint, run/stage tracking, pending replay."""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest

from extractor.azure_sql import (
    db_get_checkpoint,
    db_update_checkpoint,
    db_insert_pipeline_run,
    db_finalize_pipeline_run,
    db_stage_start,
    db_stage_end,
    flush_pending_db_updates,
    get_sql_connection,
)
from extractor.exceptions import AuthenticationError, CheckpointError, ConfigurationError
from extractor.models import PipelineMetrics


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sql_config(tmp_path):
    """Config with Azure SQL server/database set."""
    from extractor.config import ExtractorConfig
    return ExtractorConfig(
        api_base_url="https://test-api.example.com",
        api_key="test-key",
        week_start=date(2024, 1, 8),
        week_end=date(2024, 1, 14),
        storage_account=None,
        container=None,
        local_data_dir=tmp_path / "data" / "raw_events",
        local_rejects_dir=tmp_path / "data" / "raw_events_rejects",
        state_dir=tmp_path / "state",
        azure_sql_server="test.database.windows.net",
        azure_sql_database="TestDB",
    )


@pytest.fixture
def mock_connection():
    """Create a mock pyodbc connection with cursor."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    return conn, cursor


# ---------------------------------------------------------------------------
# get_sql_connection tests
# ---------------------------------------------------------------------------

class TestGetSqlConnection:
    def test_missing_server_raises(self, tmp_path):
        from extractor.config import ExtractorConfig
        config = ExtractorConfig(
            api_base_url="https://test.com", api_key="k",
            week_start=date(2024, 1, 8), week_end=date(2024, 1, 14),
            storage_account=None, container=None,
            local_data_dir=tmp_path / "d", local_rejects_dir=tmp_path / "r",
            state_dir=tmp_path / "s",
            azure_sql_server=None, azure_sql_database=None,
        )
        with pytest.raises(ConfigurationError, match="AZURE_SQL_SERVER"):
            get_sql_connection(config)

    @patch("extractor.azure_sql._get_sql_access_token_bytes", return_value=b"\x00\x00\x00\x00")
    @patch("extractor.azure_sql.pyodbc.connect")
    def test_successful_connection(self, mock_connect, mock_token, sql_config):
        mock_connect.return_value = MagicMock()
        conn = get_sql_connection(sql_config)
        mock_connect.assert_called_once()
        assert conn is not None

    @patch("extractor.azure_sql._get_sql_access_token_bytes")
    def test_auth_failure_raises(self, mock_token, sql_config):
        mock_token.side_effect = AuthenticationError("auth failed")
        with pytest.raises(AuthenticationError):
            get_sql_connection(sql_config)


# ---------------------------------------------------------------------------
# db_get_checkpoint tests
# ---------------------------------------------------------------------------

class TestDbGetCheckpoint:
    @patch("extractor.azure_sql.get_sql_connection")
    def test_found(self, mock_conn_fn, sql_config, mock_connection):
        conn, cursor = mock_connection
        mock_conn_fn.return_value = conn
        cursor.fetchone.return_value = (
            "test_pipeline",
            date(2024, 1, 1),
            date(2024, 1, 7),
            date(2024, 1, 8),
            date(2024, 1, 14),
            datetime(2024, 1, 7, 12, 0, tzinfo=timezone.utc),
        )
        result = db_get_checkpoint(sql_config, "test_pipeline")
        assert result["next_week_start"] == "2024-01-08"
        assert result["next_week_end"] == "2024-01-14"
        assert result["updated_at_utc"] is not None

    @patch("extractor.azure_sql.get_sql_connection")
    def test_not_found_returns_none(self, mock_conn_fn, sql_config, mock_connection):
        conn, cursor = mock_connection
        mock_conn_fn.return_value = conn
        cursor.fetchone.return_value = None
        result = db_get_checkpoint(sql_config, "test_pipeline")
        assert result is None

    @patch("extractor.azure_sql.get_sql_connection")
    def test_connection_error_raises_checkpoint_error(self, mock_conn_fn, sql_config):
        mock_conn_fn.side_effect = Exception("connection refused")
        with pytest.raises(CheckpointError, match="Failed to read checkpoint"):
            db_get_checkpoint(sql_config, "test_pipeline")

    @patch("extractor.azure_sql.get_sql_connection")
    def test_null_last_dates(self, mock_conn_fn, sql_config, mock_connection):
        conn, cursor = mock_connection
        mock_conn_fn.return_value = conn
        cursor.fetchone.return_value = (
            "test_pipeline", None, None, date(2024, 1, 8), date(2024, 1, 14), None,
        )
        result = db_get_checkpoint(sql_config, "test_pipeline")
        assert result["last_successful_week_start"] is None
        assert result["last_successful_week_end"] is None
        assert result["next_week_start"] == "2024-01-08"


# ---------------------------------------------------------------------------
# db_update_checkpoint tests
# ---------------------------------------------------------------------------

class TestDbUpdateCheckpoint:
    @patch("extractor.azure_sql.get_sql_connection")
    def test_successful_upsert(self, mock_conn_fn, sql_config, mock_connection):
        conn, cursor = mock_connection
        mock_conn_fn.return_value = conn
        db_update_checkpoint(
            sql_config,
            pipeline_name="test",
            last_start=date(2024, 1, 8),
            last_end=date(2024, 1, 14),
            next_start=date(2024, 1, 15),
            next_end=date(2024, 1, 21),
        )
        cursor.execute.assert_called_once()
        conn.commit.assert_called_once()

    @patch("extractor.azure_sql.get_sql_connection")
    def test_failure_raises(self, mock_conn_fn, sql_config):
        mock_conn_fn.side_effect = Exception("DB down")
        with pytest.raises(Exception, match="DB down"):
            db_update_checkpoint(
                sql_config, pipeline_name="test",
                last_start=date(2024, 1, 8), last_end=date(2024, 1, 14),
                next_start=date(2024, 1, 15), next_end=date(2024, 1, 21),
            )


# ---------------------------------------------------------------------------
# db_insert_pipeline_run tests
# ---------------------------------------------------------------------------

class TestDbInsertPipelineRun:
    @patch("extractor.azure_sql.get_sql_connection")
    def test_returns_run_id(self, mock_conn_fn, sql_config, mock_connection):
        conn, cursor = mock_connection
        mock_conn_fn.return_value = conn
        metrics = PipelineMetrics(correlation_id="corr-123")
        run_id = db_insert_pipeline_run(sql_config, "test_pipeline", metrics)
        assert isinstance(run_id, str)
        assert len(run_id) == 36  # UUID format
        cursor.execute.assert_called_once()
        conn.commit.assert_called_once()

    @patch("extractor.azure_sql.get_sql_connection")
    def test_failure_raises(self, mock_conn_fn, sql_config):
        mock_conn_fn.side_effect = Exception("DB down")
        metrics = PipelineMetrics()
        with pytest.raises(Exception):
            db_insert_pipeline_run(sql_config, "test_pipeline", metrics)


# ---------------------------------------------------------------------------
# db_finalize_pipeline_run tests
# ---------------------------------------------------------------------------

class TestDbFinalizePipelineRun:
    @patch("extractor.azure_sql.get_sql_connection")
    def test_successful_finalize(self, mock_conn_fn, sql_config, mock_connection):
        conn, cursor = mock_connection
        mock_conn_fn.return_value = conn
        metrics = PipelineMetrics(correlation_id="corr-123")
        metrics.run_status = "SUCCESS"
        metrics.rows_fetched = 100
        metrics.rows_output = 98
        metrics.rows_rejected = 2
        metrics.end_time = datetime.now(timezone.utc)
        db_finalize_pipeline_run(sql_config, "run-id-123", metrics)
        cursor.execute.assert_called_once()
        conn.commit.assert_called_once()

    @patch("extractor.azure_sql.get_sql_connection")
    def test_finalize_with_error(self, mock_conn_fn, sql_config, mock_connection):
        conn, cursor = mock_connection
        mock_conn_fn.return_value = conn
        metrics = PipelineMetrics()
        metrics.run_status = "FAILED"
        metrics.error_type = "DataQualityError"
        metrics.error_message = "threshold exceeded"
        metrics.end_time = datetime.now(timezone.utc)
        db_finalize_pipeline_run(sql_config, "run-id-123", metrics)
        cursor.execute.assert_called_once()


# ---------------------------------------------------------------------------
# db_stage_start / db_stage_end tests
# ---------------------------------------------------------------------------

class TestDbStageTracking:
    @patch("extractor.azure_sql.get_sql_connection")
    def test_stage_start_returns_id(self, mock_conn_fn, sql_config, mock_connection):
        conn, cursor = mock_connection
        mock_conn_fn.return_value = conn
        cursor.fetchone.return_value = (42,)
        stage_id = db_stage_start(sql_config, "run-id", "EXTRACT")
        assert stage_id == 42
        cursor.execute.assert_called_once()
        conn.commit.assert_called_once()

    @patch("extractor.azure_sql.get_sql_connection")
    def test_stage_end_success(self, mock_conn_fn, sql_config, mock_connection):
        conn, cursor = mock_connection
        mock_conn_fn.return_value = conn
        db_stage_end(sql_config, 42, "SUCCESS", row_count=100)
        cursor.execute.assert_called_once()
        conn.commit.assert_called_once()

    @patch("extractor.azure_sql.get_sql_connection")
    def test_stage_end_failed_with_error(self, mock_conn_fn, sql_config, mock_connection):
        conn, cursor = mock_connection
        mock_conn_fn.return_value = conn
        db_stage_end(sql_config, 42, "FAILED", row_count=None, error_message="timeout")
        cursor.execute.assert_called_once()
        # Verify error_message passed
        call_args = cursor.execute.call_args[0]
        assert "timeout" in call_args  # error_message is in positional args

    @patch("extractor.azure_sql.get_sql_connection")
    def test_stage_start_failure_raises(self, mock_conn_fn, sql_config):
        mock_conn_fn.side_effect = Exception("DB down")
        with pytest.raises(Exception):
            db_stage_start(sql_config, "run-id", "EXTRACT")


# ---------------------------------------------------------------------------
# flush_pending_db_updates tests
# ---------------------------------------------------------------------------

class TestFlushPendingDbUpdates:
    @patch("extractor.azure_sql.rewrite_pending_db_updates")
    @patch("extractor.azure_sql.read_pending_db_updates")
    def test_no_pending_is_noop(self, mock_read, mock_rewrite, sql_config):
        mock_read.return_value = []
        flush_pending_db_updates(sql_config)
        mock_rewrite.assert_not_called()

    @patch("extractor.azure_sql.rewrite_pending_db_updates")
    @patch("extractor.azure_sql.read_pending_db_updates")
    @patch("extractor.azure_sql.db_update_checkpoint")
    def test_successful_replay(self, mock_db_update, mock_read, mock_rewrite, sql_config):
        mock_read.return_value = [{
            "checkpoint": {
                "pipeline_name": "test",
                "last_successful_week_start": "2024-01-01",
                "last_successful_week_end": "2024-01-07",
                "next_week_start": "2024-01-08",
                "next_week_end": "2024-01-14",
            },
            "run": {"correlation_id": "corr-1"},
            "failed_at_utc": "2024-01-08T00:00:00",
        }]
        flush_pending_db_updates(sql_config)
        mock_db_update.assert_called_once()
        mock_rewrite.assert_called_once_with(sql_config, [])  # Empty — all flushed

    @patch("extractor.azure_sql.rewrite_pending_db_updates")
    @patch("extractor.azure_sql.read_pending_db_updates")
    @patch("extractor.azure_sql.db_update_checkpoint")
    def test_partial_replay_keeps_failures(self, mock_db_update, mock_read, mock_rewrite, sql_config):
        item1 = {
            "checkpoint": {
                "pipeline_name": "test",
                "last_successful_week_start": "2024-01-01",
                "last_successful_week_end": "2024-01-07",
                "next_week_start": "2024-01-08",
                "next_week_end": "2024-01-14",
            },
            "run": {"correlation_id": "corr-1"},
        }
        item2 = {
            "checkpoint": {
                "pipeline_name": "test",
                "last_successful_week_start": "2024-01-08",
                "last_successful_week_end": "2024-01-14",
                "next_week_start": "2024-01-15",
                "next_week_end": "2024-01-21",
            },
            "run": {"correlation_id": "corr-2"},
        }
        mock_read.return_value = [item1, item2]
        # First succeeds, second fails
        mock_db_update.side_effect = [None, Exception("DB still down")]
        flush_pending_db_updates(sql_config)
        # Should keep the failed item
        mock_rewrite.assert_called_once_with(sql_config, [item2])

    @patch("extractor.azure_sql.rewrite_pending_db_updates")
    @patch("extractor.azure_sql.read_pending_db_updates")
    @patch("extractor.azure_sql.db_update_checkpoint")
    def test_replay_without_checkpoint_payload(self, mock_db_update, mock_read, mock_rewrite, sql_config):
        """Items with only run payload (no checkpoint) should still be flushed."""
        mock_read.return_value = [{
            "run": {"correlation_id": "corr-1"},
            "failed_at_utc": "2024-01-08T00:00:00",
        }]
        flush_pending_db_updates(sql_config)
        mock_db_update.assert_not_called()  # No checkpoint to replay
        mock_rewrite.assert_called_once_with(sql_config, [])  # Still removed from pending

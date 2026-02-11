"""Tests for extractor.salesforce_sync — Salesforce sync module."""

import os
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from extractor.config import ExtractorConfig
from extractor.exceptions import ConfigurationError
from extractor.salesforce_sync import SalesforceConfig, fetch_export_data, push_to_salesforce, run_salesforce_sync


@pytest.fixture
def sample_config(tmp_path):
    return ExtractorConfig(
        api_base_url="https://test-api.example.com",
        api_key="test-key-123",
        week_start=date(2024, 1, 8),
        week_end=date(2024, 1, 14),
        storage_account=None,
        container=None,
        azure_sql_server="test.database.windows.net",
        azure_sql_database="TestDB",
        local_data_dir=tmp_path / "data" / "raw_events",
        local_rejects_dir=tmp_path / "data" / "raw_events_rejects",
        state_dir=tmp_path / "state",
    )


@pytest.fixture
def sf_env(monkeypatch):
    """Set Salesforce environment variables for testing."""
    monkeypatch.setenv("SF_USERNAME", "test@example.com")
    monkeypatch.setenv("SF_PASSWORD", "password123")
    monkeypatch.setenv("SF_SECURITY_TOKEN", "token456")


class TestSalesforceConfig:
    def test_valid_config(self, sf_env):
        cfg = SalesforceConfig()
        assert cfg.username == "test@example.com"
        assert cfg.password == "password123"
        assert cfg.security_token == "token456"
        assert cfg.domain == "login"
        assert cfg.object_name == "Customer_Weekly_Metric__c"

    def test_custom_domain(self, sf_env, monkeypatch):
        monkeypatch.setenv("SF_DOMAIN", "test")
        cfg = SalesforceConfig()
        assert cfg.domain == "test"

    def test_missing_username_raises(self, monkeypatch):
        monkeypatch.setenv("SF_PASSWORD", "pw")
        monkeypatch.setenv("SF_SECURITY_TOKEN", "tok")
        monkeypatch.delenv("SF_USERNAME", raising=False)
        with pytest.raises(ConfigurationError, match="SF_USERNAME"):
            SalesforceConfig()

    def test_missing_all_credentials_raises(self, monkeypatch):
        monkeypatch.delenv("SF_USERNAME", raising=False)
        monkeypatch.delenv("SF_PASSWORD", raising=False)
        monkeypatch.delenv("SF_SECURITY_TOKEN", raising=False)
        with pytest.raises(ConfigurationError, match="SF_USERNAME.*SF_PASSWORD.*SF_SECURITY_TOKEN"):
            SalesforceConfig()


class TestFetchExportData:
    @patch("extractor.salesforce_sync.get_sql_connection")
    def test_fetches_rows(self, mock_conn, sample_config):
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("external_customer_id",), ("external_signal_id",),
            ("batch_week_start",), ("batch_week_end",),
            ("total_spend_week",), ("activity_count_week",),
            ("last_activity_time",), ("customer_tier",),
        ]
        mock_cursor.fetchall.return_value = [
            ("C001", "S001", date(2024, 1, 8), date(2024, 1, 14), 100.0, 5, None, "gold"),
        ]
        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_cm)
        mock_cm.__exit__ = MagicMock(return_value=False)
        mock_cm.cursor.return_value = mock_cursor
        mock_conn.return_value = mock_cm

        rows = fetch_export_data(sample_config)
        assert len(rows) == 1
        assert rows[0]["external_customer_id"] == "C001"
        assert rows[0]["customer_tier"] == "gold"

    @patch("extractor.salesforce_sync.get_sql_connection")
    def test_empty_result(self, mock_conn, sample_config):
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("external_customer_id",), ("external_signal_id",),
            ("batch_week_start",), ("batch_week_end",),
            ("total_spend_week",), ("activity_count_week",),
            ("last_activity_time",), ("customer_tier",),
        ]
        mock_cursor.fetchall.return_value = []
        mock_cm = MagicMock()
        mock_cm.__enter__ = MagicMock(return_value=mock_cm)
        mock_cm.__exit__ = MagicMock(return_value=False)
        mock_cm.cursor.return_value = mock_cursor
        mock_conn.return_value = mock_cm

        rows = fetch_export_data(sample_config)
        assert rows == []


class TestPushToSalesforce:
    _SAMPLE_RECORD = {
        "external_customer_id": "C001",
        "external_signal_id": "S001",
        "batch_week_start": "2024-01-08",
        "batch_week_end": "2024-01-14",
        "total_spend_week": 100.0,
        "activity_count_week": 5,
        "last_activity_time": None,
        "customer_tier": "gold",
    }

    def test_successful_upsert(self, sf_env):
        sf_config = SalesforceConfig()
        with patch("extractor.salesforce_sync.Salesforce") as mock_sf_cls:
            mock_sf = MagicMock()
            # sf.bulk.__getattr__(object_name) returns a MagicMock; configure its upsert
            mock_bulk_obj = MagicMock()
            mock_bulk_obj.upsert.return_value = [{"success": True, "id": "a01XX"}]
            type(mock_sf.bulk).__getattr__ = MagicMock(return_value=mock_bulk_obj)
            mock_sf_cls.return_value = mock_sf

            result = push_to_salesforce([self._SAMPLE_RECORD], sf_config)
            assert result["total_records"] == 1
            assert result["success_count"] == 1
            assert result["error_count"] == 0

    def test_upsert_with_errors_above_threshold_raises(self, sf_env):
        """100% error rate (1/1) exceeds 10% threshold -> raises ExtractorError."""
        sf_config = SalesforceConfig()
        with patch("extractor.salesforce_sync.Salesforce") as mock_sf_cls:
            mock_sf = MagicMock()
            mock_bulk_obj = MagicMock()
            mock_bulk_obj.upsert.return_value = [{"success": False, "errors": ["FIELD_ERROR"]}]
            type(mock_sf.bulk).__getattr__ = MagicMock(return_value=mock_bulk_obj)
            mock_sf_cls.return_value = mock_sf

            from extractor.exceptions import ExtractorError
            with pytest.raises(ExtractorError, match="error rate too high"):
                push_to_salesforce([self._SAMPLE_RECORD], sf_config)

    def test_upsert_with_errors_below_threshold_returns(self, sf_env):
        """1/20 = 5% error rate, below 10% threshold -> returns summary."""
        sf_config = SalesforceConfig()
        with patch("extractor.salesforce_sync.Salesforce") as mock_sf_cls:
            mock_sf = MagicMock()
            mock_bulk_obj = MagicMock()
            # 19 successes + 1 failure = 5% error rate
            mock_bulk_obj.upsert.return_value = (
                [{"success": True, "id": f"a{i:03d}"} for i in range(19)]
                + [{"success": False, "errors": ["FIELD_ERROR"]}]
            )
            type(mock_sf.bulk).__getattr__ = MagicMock(return_value=mock_bulk_obj)
            mock_sf_cls.return_value = mock_sf

            records = [self._SAMPLE_RECORD] * 20
            result = push_to_salesforce(records, sf_config)
            assert result["error_count"] == 1
            assert result["success_count"] == 19
            assert "sample_errors" in result


class TestRunSalesforceSync:
    @patch("extractor.salesforce_sync.push_to_salesforce")
    @patch("extractor.salesforce_sync.fetch_export_data")
    def test_no_records_skips_push(self, mock_fetch, mock_push, sample_config, sf_env):
        mock_fetch.return_value = []
        result = run_salesforce_sync(sample_config)
        assert result["total_records"] == 0
        mock_push.assert_not_called()

    @patch("extractor.salesforce_sync.push_to_salesforce")
    @patch("extractor.salesforce_sync.fetch_export_data")
    def test_with_records_calls_push(self, mock_fetch, mock_push, sample_config, sf_env):
        mock_fetch.return_value = [{"external_customer_id": "C001"}]
        mock_push.return_value = {"total_records": 1, "success_count": 1, "error_count": 0}

        result = run_salesforce_sync(sample_config)
        assert result["total_records"] == 1
        mock_push.assert_called_once()

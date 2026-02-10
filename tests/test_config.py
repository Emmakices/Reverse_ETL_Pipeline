"""Tests for extractor.config — configuration validation."""

from datetime import date

import pytest

from extractor.config import ExtractorConfig


def _cfg(**overrides):
    """Create an ExtractorConfig without reading config.env file."""
    defaults = {
        "api_base_url": "https://api.example.com",
        "api_key": "key",
        "week_start": date(2024, 1, 8),
        "week_end": date(2024, 1, 14),
    }
    return ExtractorConfig(**{**defaults, **overrides}, _env_file=None)


class TestApiUrlValidation:
    def test_valid_https_url(self):
        cfg = _cfg(api_base_url="https://api.example.com")
        assert cfg.api_base_url == "https://api.example.com"

    def test_valid_http_url(self):
        cfg = _cfg(api_base_url="http://localhost:8000/")
        assert cfg.api_base_url == "http://localhost:8000"  # trailing slash stripped

    def test_invalid_url_rejected(self):
        with pytest.raises(Exception, match="must start with http"):
            _cfg(api_base_url="ftp://not-http.example.com")


class TestLogLevelValidation:
    def test_valid_log_level(self):
        cfg = _cfg(log_level="debug")
        assert cfg.log_level == "DEBUG"  # uppercased

    def test_invalid_log_level_rejected(self):
        with pytest.raises(Exception, match="LOG_LEVEL"):
            _cfg(log_level="TRACE")


class TestDateRangeValidation:
    def test_valid_date_range(self):
        cfg = _cfg()
        assert cfg.week_start < cfg.week_end

    def test_same_day_range_allowed(self):
        cfg = _cfg(week_start=date(2024, 1, 8), week_end=date(2024, 1, 8))
        assert cfg.week_start == cfg.week_end

    def test_end_before_start_rejected(self):
        with pytest.raises(Exception, match="WEEK_END"):
            _cfg(week_start=date(2024, 1, 14), week_end=date(2024, 1, 8))


class TestAzureConfigValidation:
    def test_both_set(self):
        cfg = _cfg(storage_account="myaccount", container="mycontainer")
        assert cfg.adls_enabled is True

    def test_neither_set(self):
        cfg = _cfg(storage_account=None, container=None)
        assert cfg.adls_enabled is False

    def test_only_account_rejected(self):
        with pytest.raises(Exception, match="STORAGE_ACCOUNT and CONTAINER"):
            _cfg(storage_account="myaccount", container=None)

    def test_only_container_rejected(self):
        with pytest.raises(Exception, match="STORAGE_ACCOUNT and CONTAINER"):
            _cfg(storage_account=None, container="mycontainer")


class TestAzureSqlDefaults:
    def test_azure_sql_defaults_to_none(self):
        cfg = _cfg()
        assert cfg.azure_sql_server is None
        assert cfg.azure_sql_database is None

    def test_azure_sql_explicit_values(self):
        cfg = _cfg(
            azure_sql_server="myserver.database.windows.net",
            azure_sql_database="MyDB",
        )
        assert cfg.azure_sql_server == "myserver.database.windows.net"
        assert cfg.azure_sql_database == "MyDB"

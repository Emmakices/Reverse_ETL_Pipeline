"""Tests for extractor.logging_utils — JSON formatter, correlation ID, setup_logging, log_operation."""

import json
import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from extractor.logging_utils import (
    CorrelationIdFilter,
    JSONFormatter,
    get_logger,
    log_operation,
    setup_logging,
)


class TestJSONFormatter:
    def test_basic_format(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hello world", args=(), exc_info=None,
        )
        output = formatter.format(record)
        data = json.loads(output)
        assert data["level"] == "INFO"
        assert data["message"] == "hello world"
        assert "timestamp" in data

    def test_extra_fields_included(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="msg", args=(), exc_info=None,
        )
        record.custom_key = "custom_value"
        output = formatter.format(record)
        data = json.loads(output)
        assert data["custom_key"] == "custom_value"

    def test_correlation_id_included(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="msg", args=(), exc_info=None,
        )
        record.correlation_id = "corr-123"
        output = formatter.format(record)
        data = json.loads(output)
        assert data["correlation_id"] == "corr-123"

    def test_datetime_extra_serialized(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="msg", args=(), exc_info=None,
        )
        record.my_time = datetime(2024, 1, 10, 12, 0, tzinfo=timezone.utc)
        output = formatter.format(record)
        data = json.loads(output)
        assert "2024-01-10" in data["my_time"]

    def test_object_extra_str_fallback(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="msg", args=(), exc_info=None,
        )
        record.my_obj = MagicMock()
        output = formatter.format(record)
        data = json.loads(output)
        assert isinstance(data["my_obj"], str)

    def test_non_serializable_extra(self):
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="msg", args=(), exc_info=None,
        )
        record.bad_val = {1, 2, 3}  # sets are not JSON serializable
        output = formatter.format(record)
        data = json.loads(output)
        assert isinstance(data["bad_val"], str)

    def test_exception_info_included(self):
        formatter = JSONFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys
            exc_info = sys.exc_info()
        record = logging.LogRecord(
            name="test", level=logging.ERROR, pathname="", lineno=0,
            msg="error happened", args=(), exc_info=exc_info,
        )
        output = formatter.format(record)
        data = json.loads(output)
        assert "exception" in data
        assert "ValueError" in data["exception"]


class TestCorrelationIdFilter:
    def test_set_and_filter(self):
        CorrelationIdFilter.set_correlation_id("test-id-123")
        f = CorrelationIdFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="msg", args=(), exc_info=None,
        )
        result = f.filter(record)
        assert result is True
        assert record.correlation_id == "test-id-123"

    def test_generate_correlation_id(self):
        cid = CorrelationIdFilter.generate_correlation_id()
        assert len(cid) == 36  # UUID format
        f = CorrelationIdFilter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="msg", args=(), exc_info=None,
        )
        f.filter(record)
        assert record.correlation_id == cid


class TestSetupLogging:
    def test_json_format(self):
        setup_logging(level="DEBUG", json_format=True)
        root = logging.getLogger()
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, JSONFormatter)

    def test_text_format(self):
        setup_logging(level="WARNING", json_format=False)
        root = logging.getLogger()
        assert len(root.handlers) == 1
        assert not isinstance(root.handlers[0].formatter, JSONFormatter)
        assert root.level == logging.WARNING

    def test_azure_logger_suppressed(self):
        setup_logging()
        assert logging.getLogger("azure").level == logging.WARNING
        assert logging.getLogger("urllib3").level == logging.WARNING


class TestGetLogger:
    def test_returns_logger(self):
        lg = get_logger("test.module")
        assert isinstance(lg, logging.Logger)
        assert lg.name == "test.module"


class TestLogOperation:
    def test_success_path(self):
        lg = get_logger("test.op")
        with log_operation(lg, "test_op", key="val"):
            pass  # no error

    def test_failure_path(self):
        lg = get_logger("test.op")
        with pytest.raises(RuntimeError, match="boom"):
            with log_operation(lg, "test_op"):
                raise RuntimeError("boom")

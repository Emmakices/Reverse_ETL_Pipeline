"""Tests for extractor.models — EventRecord validation, ValidationResult, RunStatus, PipelineMetrics."""

from decimal import Decimal

import pytest
from pydantic import ValidationError as PydanticValidationError

from extractor.models import EventRecord, PipelineMetrics, RunStatus, ValidationResult


class TestEventRecord:
    def test_valid_event(self):
        event = EventRecord.model_validate({
            "event_time": "2024-01-10T14:30:00",
            "event_type": "purchase",
            "user_id": "12345",
            "price": 599.99,
        })
        assert event.user_id == "12345"
        assert event.event_type == "purchase"
        assert event.price == Decimal("599.99")

    def test_event_type_lowercased(self):
        event = EventRecord.model_validate({
            "event_time": "2024-01-10T14:30:00",
            "event_type": "PURCHASE",
            "user_id": "12345",
        })
        assert event.event_type == "purchase"

    def test_numeric_user_id_coerced_to_string(self):
        event = EventRecord.model_validate({
            "event_time": "2024-01-10T14:30:00",
            "event_type": "view",
            "user_id": 67890,
        })
        assert event.user_id == "67890"
        assert isinstance(event.user_id, str)

    def test_empty_user_id_rejected(self):
        with pytest.raises(PydanticValidationError):
            EventRecord.model_validate({
                "event_time": "2024-01-10T14:30:00",
                "event_type": "view",
                "user_id": "",
            })

    def test_missing_event_time_rejected(self):
        with pytest.raises(PydanticValidationError):
            EventRecord.model_validate({
                "event_type": "view",
                "user_id": "123",
            })

    def test_price_string_coerced_to_decimal(self):
        event = EventRecord.model_validate({
            "event_time": "2024-01-10T14:30:00",
            "event_type": "view",
            "user_id": "123",
            "price": "99.99",
        })
        assert event.price == Decimal("99.99")

    def test_price_none_accepted(self):
        event = EventRecord.model_validate({
            "event_time": "2024-01-10T14:30:00",
            "event_type": "view",
            "user_id": "123",
            "price": None,
        })
        assert event.price is None

    def test_optional_fields_default_to_none(self):
        event = EventRecord.model_validate({
            "event_time": "2024-01-10T14:30:00",
            "event_type": "view",
            "user_id": "123",
        })
        assert event.product_id is None
        assert event.category_id is None
        assert event.brand is None
        assert event.user_session is None

    def test_extra_fields_ignored(self):
        event = EventRecord.model_validate({
            "event_time": "2024-01-10T14:30:00",
            "event_type": "view",
            "user_id": "123",
            "unknown_field": "should be ignored",
        })
        assert not hasattr(event, "unknown_field")


class TestValidationResult:
    def test_success_rate_100(self):
        result = ValidationResult(total_records=100, valid_records=100, invalid_records=0)
        assert result.success_rate == 100.0
        assert result.is_acceptable is True

    def test_success_rate_below_threshold(self):
        result = ValidationResult(total_records=100, valid_records=90, invalid_records=10)
        assert result.success_rate == 90.0
        assert result.is_acceptable is False

    def test_success_rate_at_threshold(self):
        result = ValidationResult(total_records=100, valid_records=95, invalid_records=5)
        assert result.success_rate == 95.0
        assert result.is_acceptable is True

    def test_empty_records(self):
        result = ValidationResult(total_records=0, valid_records=0, invalid_records=0)
        assert result.success_rate == 0.0


class TestRunStatus:
    def test_enum_values(self):
        assert RunStatus.SUCCESS.value == "SUCCESS"
        assert RunStatus.SUCCESS_WITH_REJECTS.value == "SUCCESS_WITH_REJECTS"
        assert RunStatus.FAILED.value == "FAILED"


class TestPipelineMetrics:
    def test_to_dict_contains_all_keys(self):
        metrics = PipelineMetrics(correlation_id="test-123")
        d = metrics.to_dict()
        assert d["correlation_id"] == "test-123"
        assert "rows_fetched" in d
        assert "run_status" in d
        assert "success" in d

    def test_default_values(self):
        metrics = PipelineMetrics()
        assert metrics.success is False
        assert metrics.rows_fetched == 0
        assert metrics.run_status == "FAILED"

"""Shared test fixtures for the extraction pipeline."""

import pytest
from datetime import date
from unittest.mock import MagicMock

from extractor.config import ExtractorConfig


@pytest.fixture
def sample_config(tmp_path):
    """Minimal ExtractorConfig for testing (no real Azure/API)."""
    return ExtractorConfig(
        api_base_url="https://test-api.example.com",
        api_key="test-key-123",
        week_start=date(2024, 1, 8),
        week_end=date(2024, 1, 14),
        storage_account=None,
        container=None,
        local_data_dir=tmp_path / "data" / "raw_events",
        local_rejects_dir=tmp_path / "data" / "raw_events_rejects",
        state_dir=tmp_path / "state",
    )


@pytest.fixture
def sample_raw_events():
    """Sample raw event dicts as returned by the API."""
    return [
        {
            "event_time": "2024-01-10T14:30:00",
            "event_type": "purchase",
            "user_id": "12345",
            "product_id": "P001",
            "category_id": "C001",
            "category_code": "electronics.phone",
            "brand": "Samsung",
            "price": 599.99,
            "user_session": "sess-abc-123",
        },
        {
            "event_time": "2024-01-11T09:15:00",
            "event_type": "View",
            "user_id": 67890,
            "product_id": "P002",
            "category_id": "C002",
            "category_code": "clothing.shoes",
            "brand": "Nike",
            "price": "129.50",
            "user_session": "sess-def-456",
        },
        {
            "event_time": "2024-01-12T18:00:00",
            "event_type": "CART",
            "user_id": "11111",
            "product_id": None,
            "category_id": None,
            "category_code": None,
            "brand": None,
            "price": None,
            "user_session": "sess-ghi-789",
        },
    ]


@pytest.fixture
def sample_invalid_events():
    """Events that should fail validation."""
    return [
        {"event_time": "not-a-date", "event_type": "view", "user_id": "123"},
        {"event_type": "view", "user_id": "456"},  # missing event_time
        {"event_time": "2024-01-10T14:30:00", "event_type": "view", "user_id": ""},  # empty user_id
    ]

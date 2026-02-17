"""Tests for extractor.checkpoint — JSON state read/write, pending updates."""

from datetime import date

from extractor.checkpoint import (
    append_pending_db_update,
    compute_week_end,
    read_json_state,
    read_pending_db_updates,
    rewrite_pending_db_updates,
    write_json_state,
)


class TestComputeWeekEnd:
    def test_default_6_days(self):
        assert compute_week_end(date(2024, 1, 8)) == date(2024, 1, 14)

    def test_custom_days(self):
        assert compute_week_end(date(2024, 1, 8), days=13) == date(2024, 1, 21)


class TestJsonState:
    def test_write_then_read_roundtrip(self, sample_config):
        state = {"last_successful_week_start": "2024-01-08", "next_week_start": "2024-01-15"}
        write_json_state(sample_config, state)
        loaded = read_json_state(sample_config)
        assert loaded == state

    def test_read_nonexistent_returns_none(self, sample_config):
        assert read_json_state(sample_config) is None


class TestPendingDbUpdates:
    def test_append_and_read_roundtrip(self, sample_config):
        payload1 = {"checkpoint": {"pipeline_name": "test"}, "failed_at_utc": "2024-01-10T00:00:00"}
        payload2 = {"checkpoint": {"pipeline_name": "test2"}, "failed_at_utc": "2024-01-11T00:00:00"}

        append_pending_db_update(sample_config, payload1)
        append_pending_db_update(sample_config, payload2)

        updates = read_pending_db_updates(sample_config)
        assert len(updates) == 2
        assert updates[0]["checkpoint"]["pipeline_name"] == "test"
        assert updates[1]["checkpoint"]["pipeline_name"] == "test2"

    def test_read_empty_returns_empty_list(self, sample_config):
        assert read_pending_db_updates(sample_config) == []

    def test_rewrite_replaces_content(self, sample_config):
        append_pending_db_update(sample_config, {"a": 1})
        append_pending_db_update(sample_config, {"b": 2})
        append_pending_db_update(sample_config, {"c": 3})

        remaining = [{"b": 2}]
        rewrite_pending_db_updates(sample_config, remaining)

        updates = read_pending_db_updates(sample_config)
        assert len(updates) == 1
        assert updates[0]["b"] == 2


class TestJsonStateEdgeCases:
    def test_read_corrupt_json_returns_none(self, sample_config):
        """Corrupt JSON file should return None, not raise."""
        sample_config.state_dir.mkdir(parents=True, exist_ok=True)
        sample_config.state_file.write_text("{bad json!!!}", encoding="utf-8")
        result = read_json_state(sample_config)
        assert result is None

    def test_write_json_state_creates_dir(self, sample_config):
        """write_json_state should create state_dir if needed."""
        state = {"key": "value"}
        write_json_state(sample_config, state)
        loaded = read_json_state(sample_config)
        assert loaded == state


class TestPendingDbUpdateEdgeCases:
    def test_read_with_malformed_lines(self, sample_config):
        """Malformed JSONL lines should be skipped."""
        sample_config.state_dir.mkdir(parents=True, exist_ok=True)
        content = '{"good": 1}\nnot valid json\n{"good": 2}\n\n'
        sample_config.pending_updates_file.write_text(content, encoding="utf-8")
        updates = read_pending_db_updates(sample_config)
        assert len(updates) == 2
        assert updates[0]["good"] == 1
        assert updates[1]["good"] == 2

    def test_read_with_blank_lines(self, sample_config):
        """Blank lines in JSONL should be skipped."""
        sample_config.state_dir.mkdir(parents=True, exist_ok=True)
        content = '\n\n{"a": 1}\n\n'
        sample_config.pending_updates_file.write_text(content, encoding="utf-8")
        updates = read_pending_db_updates(sample_config)
        assert len(updates) == 1

    def test_rewrite_empty_list(self, sample_config):
        """Rewrite with empty list should create empty file."""
        append_pending_db_update(sample_config, {"x": 1})
        rewrite_pending_db_updates(sample_config, [])
        updates = read_pending_db_updates(sample_config)
        assert updates == []

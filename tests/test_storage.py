"""Tests for extractor.storage — parquet write/read, SHA256."""

import pandas as pd
import pytest

from extractor.storage import check_output_exists, ensure_dir, sha256_file, write_parquet, write_rejects_parquet


class TestSha256File:
    def test_consistent_hash(self, tmp_path):
        path = tmp_path / "test.txt"
        path.write_text("hello world")
        hash1 = sha256_file(path)
        hash2 = sha256_file(path)
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hex digest length


class TestEnsureDir:
    def test_creates_nested_dirs(self, tmp_path):
        target = tmp_path / "a" / "b" / "c"
        assert not target.exists()
        ensure_dir(target)
        assert target.exists()

    def test_idempotent(self, tmp_path):
        target = tmp_path / "existing"
        target.mkdir()
        ensure_dir(target)  # should not raise
        assert target.exists()


class TestWriteParquet:
    def test_writes_valid_parquet(self, tmp_path):
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        output_path = tmp_path / "output" / "test.parquet"
        metadata = write_parquet(df, output_path)

        assert output_path.exists()
        assert metadata["row_count"] == 3
        assert metadata["size_bytes"] > 0
        assert len(metadata["sha256"]) == 64

        # Verify readable
        df_read = pd.read_parquet(output_path)
        assert len(df_read) == 3

    def test_empty_df(self, tmp_path):
        df = pd.DataFrame({"col1": [], "col2": []})
        output_path = tmp_path / "empty.parquet"
        metadata = write_parquet(df, output_path)
        assert metadata["row_count"] == 0


class TestWriteRejectsParquet:
    def test_skips_empty_df(self, tmp_path):
        df = pd.DataFrame()
        result = write_rejects_parquet(df, tmp_path / "rejects.parquet")
        assert result is None

    def test_skips_none_df(self, tmp_path):
        result = write_rejects_parquet(None, tmp_path / "rejects.parquet")
        assert result is None

    def test_writes_non_empty_df(self, tmp_path):
        df = pd.DataFrame({"error": ["bad data"], "row_index": [0]})
        output_path = tmp_path / "rejects.parquet"
        result = write_rejects_parquet(df, output_path)
        assert result is not None
        assert result["row_count"] == 1


class TestCheckOutputExists:
    def test_returns_false_when_missing(self, sample_config):
        assert check_output_exists(sample_config) is False

    def test_returns_true_when_exists(self, sample_config):
        output_file = sample_config.local_output_file
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text("test")
        assert check_output_exists(sample_config) is True

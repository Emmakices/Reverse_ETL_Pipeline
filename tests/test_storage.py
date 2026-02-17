"""Tests for extractor.storage — parquet write/read, SHA256, ADLS upload, save_events."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from extractor.exceptions import AuthenticationError, AzureStorageError, LocalStorageError
from extractor.storage import (
    check_output_exists,
    ensure_dir,
    get_adls_credential,
    save_events,
    sha256_file,
    upload_rejects_to_adls,
    upload_to_adls,
    write_parquet,
    write_rejects_parquet,
)


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


class TestGetAdlsCredential:
    @patch("extractor.storage.DefaultAzureCredential")
    def test_returns_credential(self, mock_cred_cls):
        mock_cred = MagicMock()
        mock_cred_cls.return_value = mock_cred
        result = get_adls_credential()
        assert result is mock_cred

    @patch("extractor.storage.DefaultAzureCredential")
    def test_auth_failure_raises(self, mock_cred_cls):
        mock_cred_cls.side_effect = Exception("no creds")
        with pytest.raises(AuthenticationError, match="Azure authentication failed"):
            get_adls_credential()


class TestUploadToAdls:
    def _make_adls_config(self, tmp_path):
        from datetime import date
        from extractor.config import ExtractorConfig
        return ExtractorConfig(
            api_base_url="https://test.com", api_key="k",
            week_start=date(2024, 1, 8), week_end=date(2024, 1, 14),
            storage_account="teststorage", container="testcontainer",
            local_data_dir=tmp_path / "data" / "raw_events",
            local_rejects_dir=tmp_path / "data" / "raw_events_rejects",
            state_dir=tmp_path / "state",
        )

    def test_raises_when_adls_not_configured(self, sample_config, tmp_path):
        local_file = tmp_path / "test.parquet"
        local_file.write_text("data")
        with pytest.raises(AzureStorageError, match="not configured"):
            upload_to_adls(local_file, sample_config)

    @patch("extractor.storage.DataLakeServiceClient")
    @patch("extractor.storage.get_adls_credential")
    def test_successful_upload(self, mock_cred, mock_dls, tmp_path):
        config = self._make_adls_config(tmp_path)
        local_file = tmp_path / "test.parquet"
        local_file.write_bytes(b"parquet data here")

        mock_file_client = MagicMock()
        mock_dls.return_value.get_file_system_client.return_value.get_file_client.return_value = mock_file_client

        result = upload_to_adls(local_file, config)
        assert result["storage_account"] == "teststorage"
        assert result["container"] == "testcontainer"
        mock_file_client.upload_data.assert_called_once()

    @patch("extractor.storage.DataLakeServiceClient")
    @patch("extractor.storage.get_adls_credential")
    def test_auth_error_raises(self, mock_cred, mock_dls, tmp_path):
        from azure.core.exceptions import ClientAuthenticationError
        config = self._make_adls_config(tmp_path)
        local_file = tmp_path / "test.parquet"
        local_file.write_bytes(b"data")

        mock_dls.return_value.get_file_system_client.return_value.get_file_client.side_effect = \
            ClientAuthenticationError("auth failed")

        with pytest.raises(AuthenticationError):
            upload_to_adls(local_file, config)

    @patch("extractor.storage.DataLakeServiceClient")
    @patch("extractor.storage.get_adls_credential")
    def test_azure_error_raises(self, mock_cred, mock_dls, tmp_path):
        from azure.core.exceptions import AzureError
        config = self._make_adls_config(tmp_path)
        local_file = tmp_path / "test.parquet"
        local_file.write_bytes(b"data")

        mock_dls.return_value.get_file_system_client.return_value.get_file_client.side_effect = \
            AzureError("service error")

        with pytest.raises(AzureStorageError):
            upload_to_adls(local_file, config)


class TestUploadRejectsToAdls:
    def test_raises_when_adls_not_configured(self, sample_config, tmp_path):
        local_file = tmp_path / "rejects.parquet"
        local_file.write_text("data")
        with pytest.raises(AzureStorageError, match="not configured"):
            upload_rejects_to_adls(local_file, sample_config)

    @patch("extractor.storage.DataLakeServiceClient")
    @patch("extractor.storage.get_adls_credential")
    def test_successful_upload(self, mock_cred, mock_dls, tmp_path):
        from datetime import date
        from extractor.config import ExtractorConfig
        config = ExtractorConfig(
            api_base_url="https://test.com", api_key="k",
            week_start=date(2024, 1, 8), week_end=date(2024, 1, 14),
            storage_account="teststorage", container="testcontainer",
            local_data_dir=tmp_path / "data" / "raw_events",
            local_rejects_dir=tmp_path / "data" / "raw_events_rejects",
            state_dir=tmp_path / "state",
        )
        local_file = tmp_path / "rejects.parquet"
        local_file.write_bytes(b"reject data")

        mock_file_client = MagicMock()
        mock_dls.return_value.get_file_system_client.return_value.get_file_client.return_value = mock_file_client

        result = upload_rejects_to_adls(local_file, config)
        assert result["storage_account"] == "teststorage"
        mock_file_client.upload_data.assert_called_once()


class TestWriteParquetError:
    @patch("extractor.storage.ensure_dir")
    def test_permission_error_raises_local_storage_error(self, mock_ensure, tmp_path):
        df = pd.DataFrame({"col": [1]})
        output_path = tmp_path / "no_perms" / "test.parquet"
        mock_ensure.return_value = None  # skip dir creation
        # Simulate PermissionError on to_parquet
        with patch.object(df, "to_parquet", side_effect=PermissionError("denied")):
            with pytest.raises(LocalStorageError, match="Failed to write parquet"):
                write_parquet(df, output_path)


class TestSaveEvents:
    def test_local_only(self, sample_config):
        df_valid = pd.DataFrame({"user_id": ["1", "2"], "event_type": ["view", "cart"]})
        df_rejects = pd.DataFrame()
        result = save_events(df_valid, df_rejects, sample_config, upload_to_cloud=False)
        assert result["local"] is not None
        assert result["adls"] is None
        assert result["rejects_local"] is None  # empty rejects

    def test_local_with_rejects(self, sample_config):
        df_valid = pd.DataFrame({"user_id": ["1"]})
        df_rejects = pd.DataFrame({"error": ["bad"], "row_index": [0]})
        result = save_events(df_valid, df_rejects, sample_config, upload_to_cloud=False)
        assert result["local"] is not None
        assert result["rejects_local"] is not None
        assert result["rejects_local"]["row_count"] == 1

    def test_cloud_upload_skipped_when_not_configured(self, sample_config):
        df_valid = pd.DataFrame({"user_id": ["1"]})
        df_rejects = pd.DataFrame()
        result = save_events(df_valid, df_rejects, sample_config, upload_to_cloud=True)
        # ADLS not configured (storage_account=None), so adls should be None
        assert result["adls"] is None

    @patch("extractor.storage.upload_rejects_to_adls")
    @patch("extractor.storage.upload_to_adls")
    def test_cloud_upload_with_adls_enabled(self, mock_upload, mock_upload_rejects, tmp_path):
        from datetime import date
        from extractor.config import ExtractorConfig
        config = ExtractorConfig(
            api_base_url="https://test.com", api_key="k",
            week_start=date(2024, 1, 8), week_end=date(2024, 1, 14),
            storage_account="teststorage", container="testcontainer",
            local_data_dir=tmp_path / "data" / "raw_events",
            local_rejects_dir=tmp_path / "data" / "raw_events_rejects",
            state_dir=tmp_path / "state",
        )
        mock_upload.return_value = {"storage_account": "teststorage", "container": "testcontainer", "path": "p", "size_bytes": 10}
        mock_upload_rejects.return_value = {"storage_account": "teststorage", "container": "testcontainer", "path": "r", "size_bytes": 5}

        df_valid = pd.DataFrame({"user_id": ["1"]})
        df_rejects = pd.DataFrame({"error": ["bad"], "row_index": [0]})
        result = save_events(df_valid, df_rejects, config, upload_to_cloud=True)
        assert result["adls"] is not None
        assert result["rejects_adls"] is not None
        mock_upload.assert_called_once()
        mock_upload_rejects.assert_called_once()

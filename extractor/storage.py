"""Local and Azure ADLS storage operations."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pandas as pd
from azure.core.exceptions import AzureError, ClientAuthenticationError, ServiceRequestError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from extractor.config import ExtractorConfig
from extractor.exceptions import AuthenticationError, AzureStorageError, LocalStorageError
from extractor.logging_utils import get_logger, log_operation

logger = get_logger(__name__)


def sha256_file(path: Path) -> str:
    """Calculate SHA256 hash of a file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def ensure_dir(path: Path) -> None:
    """Create directory if it doesn't exist."""
    path.mkdir(parents=True, exist_ok=True)


def check_output_exists(config: ExtractorConfig) -> bool:
    """Check if output file already exists."""
    return config.local_output_file.exists()


def write_parquet(df: pd.DataFrame, output_path: Path, compression: str = "snappy") -> dict:
    """Write DataFrame to parquet file."""
    with log_operation(logger, "write_parquet", output_path=str(output_path), row_count=len(df)):
        try:
            ensure_dir(output_path.parent)
            df.to_parquet(output_path, index=False, compression=compression, engine="pyarrow")

            file_size = output_path.stat().st_size
            file_hash = sha256_file(output_path)

            metadata = {
                "path": str(output_path),
                "size_bytes": file_size,
                "size_mb": round(file_size / (1024 * 1024), 2),
                "sha256": file_hash,
                "row_count": len(df),
            }

            logger.info("Parquet file written successfully", extra=metadata)
            return metadata

        except (PermissionError, OSError) as e:
            raise LocalStorageError(f"Failed to write parquet file: {e}") from e


def write_rejects_parquet(df_rejects: pd.DataFrame, output_path: Path) -> dict | None:
    """Write rejects to parquet if any rejects exist."""
    if df_rejects is None or df_rejects.empty:
        logger.info("No rejects to write")
        return None
    return write_parquet(df_rejects, output_path)


def get_adls_credential():
    """Get Azure credential for ADLS access."""
    try:
        return DefaultAzureCredential(exclude_interactive_browser_credential=False)
    except Exception as e:
        raise AuthenticationError(
            "Azure authentication failed. Run 'az login' or configure service principal."
        ) from e


def upload_to_adls(local_file: Path, config: ExtractorConfig) -> dict:
    """Upload file to Azure Data Lake Storage Gen2."""
    if not config.adls_enabled:
        raise AzureStorageError("ADLS upload not configured")

    adls_path = config.adls_output_path

    with log_operation(logger, "upload_adls", storage_account=config.storage_account, path=adls_path):
        try:
            credential = get_adls_credential()
            account_url = f"https://{config.storage_account}.dfs.core.windows.net"
            service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
            fs_client = service_client.get_file_system_client(file_system=config.container)
            file_client = fs_client.get_file_client(adls_path)

            file_size = local_file.stat().st_size
            with local_file.open("rb") as f:
                file_client.upload_data(f, overwrite=True, length=file_size)

            metadata = {
                "storage_account": config.storage_account,
                "container": config.container,
                "path": adls_path,
                "size_bytes": file_size,
            }

            logger.info("File uploaded to ADLS successfully", extra=metadata)
            return metadata

        except ClientAuthenticationError as e:
            raise AuthenticationError(
                "Azure auth failed. Ensure 'Storage Blob Data Contributor' role is assigned."
            ) from e
        except (ServiceRequestError, AzureError) as e:
            raise AzureStorageError(f"Azure storage operation failed: {e}") from e


def upload_rejects_to_adls(local_file: Path, config: ExtractorConfig) -> dict:
    """Upload rejects file to ADLS."""
    if not config.adls_enabled:
        raise AzureStorageError("ADLS upload not configured")

    adls_path = config.adls_rejects_output_path

    with log_operation(logger, "upload_adls_rejects", storage_account=config.storage_account, path=adls_path):
        try:
            credential = get_adls_credential()
            account_url = f"https://{config.storage_account}.dfs.core.windows.net"
            service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
            fs_client = service_client.get_file_system_client(file_system=config.container)
            file_client = fs_client.get_file_client(adls_path)

            file_size = local_file.stat().st_size
            with local_file.open("rb") as f:
                file_client.upload_data(f, overwrite=True, length=file_size)

            metadata = {
                "storage_account": config.storage_account,
                "container": config.container,
                "path": adls_path,
                "size_bytes": file_size,
            }

            logger.info("Rejects uploaded to ADLS successfully", extra=metadata)
            return metadata

        except ClientAuthenticationError as e:
            raise AuthenticationError(
                "Azure auth failed. Ensure 'Storage Blob Data Contributor' role is assigned."
            ) from e
        except (ServiceRequestError, AzureError) as e:
            raise AzureStorageError(f"Azure storage operation failed: {e}") from e


def save_events(
    df_valid: pd.DataFrame,
    df_rejects: pd.DataFrame,
    config: ExtractorConfig,
    upload_to_cloud: bool = True
) -> dict:
    """Save valid events + rejects to local storage and optionally upload both to ADLS."""
    result = {"local": None, "adls": None, "rejects_local": None, "rejects_adls": None}

    # Valid
    result["local"] = write_parquet(df_valid, config.local_output_file)

    # Rejects
    ensure_dir(config.local_rejects_output_file.parent)
    result["rejects_local"] = write_rejects_parquet(df_rejects, config.local_rejects_output_file)

    if upload_to_cloud and config.adls_enabled:
        # Upload valid
        result["adls"] = upload_to_adls(config.local_output_file, config)

        # Upload rejects if any
        if result["rejects_local"]:
            result["rejects_adls"] = upload_rejects_to_adls(config.local_rejects_output_file, config)
        else:
            logger.info("Rejects upload skipped - no rejects")

    elif upload_to_cloud and not config.adls_enabled:
        logger.info("ADLS upload skipped - not configured")

    return result

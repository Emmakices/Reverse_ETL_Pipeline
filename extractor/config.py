"""Configuration management for the extraction pipeline."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from extractor.exceptions import ConfigurationError


class ExtractorConfig(BaseSettings):
    """Configuration for the event extraction pipeline."""

    model_config = SettingsConfigDict(
        env_file="config.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Required fields
    api_base_url: str = Field(..., description="Base URL of the events API")
    api_key: str = Field(..., description="API key for authentication")
    week_start: date = Field(..., description="Start date of batch week")
    week_end: date = Field(..., description="End date of batch week")

    # Azure storage (optional)
    storage_account: str | None = Field(default=None)
    container: str | None = Field(default=None)
    adls_base_path: str = Field(default="reverse_etl/raw_events")

    # Rejects output paths
    adls_rejects_base_path: str = Field(default="reverse_etl/raw_events_rejects")

    # Azure SQL (required when USE_DB_STATE=true)
    azure_sql_server: str | None = Field(default=None)
    azure_sql_database: str | None = Field(default=None)

    # Local storage
    local_data_dir: Path = Field(default=Path("data/raw_events"))

    # Local rejects dir
    local_rejects_dir: Path = Field(default=Path("data/raw_events_rejects"))

    # Checkpoint/state (for Airflow resume)
    state_dir: Path = Field(default=Path("state"))
    state_file_name: str = Field(default="extractor_state.json")
    pending_updates_file_name: str = Field(default="pending_db_updates.jsonl")

    # API settings
    api_timeout_seconds: int = Field(default=60, ge=1, le=300)
    api_max_retries: int = Field(default=3, ge=1, le=10)
    api_page_size: int = Field(default=1000, ge=1, le=1000, description="Records per API page (max 1000)")
    api_rate_limit_delay: float = Field(default=2.0, ge=0, description="Delay between API requests in seconds")

    # Reject threshold percent (allowed rejects %)
    reject_threshold_pct: float = Field(default=5.0, ge=0.0, le=100.0)

    # Logging
    log_level: str = Field(default="INFO")

    @field_validator("api_base_url")
    @classmethod
    def validate_api_url(cls, v: str) -> str:
        v = v.rstrip("/")
        if not v.startswith(("http://", "https://")):
            raise ValueError("API_BASE_URL must start with http:// or https://")
        return v

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v = v.upper()
        if v not in valid_levels:
            raise ValueError(f"LOG_LEVEL must be one of {valid_levels}")
        return v

    @model_validator(mode="after")
    def validate_date_range(self) -> "ExtractorConfig":
        if self.week_end < self.week_start:
            raise ValueError(f"WEEK_END ({self.week_end}) must be >= WEEK_START ({self.week_start})")
        return self

    @model_validator(mode="after")
    def validate_azure_config(self) -> "ExtractorConfig":
        if (self.storage_account is None) != (self.container is None):
            raise ValueError("Both STORAGE_ACCOUNT and CONTAINER must be set together")
        return self

    @property
    def adls_enabled(self) -> bool:
        return self.storage_account is not None and self.container is not None

    @property
    def local_output_dir(self) -> Path:
        return self.local_data_dir / f"week_start={self.week_start}" / f"week_end={self.week_end}"

    @property
    def local_output_file(self) -> Path:
        return self.local_output_dir / "events.parquet"

    @property
    def adls_output_path(self) -> str:
        return f"{self.adls_base_path}/week_start={self.week_start}/week_end={self.week_end}/events.parquet"

    # Rejects local + ADLS paths
    @property
    def local_rejects_output_dir(self) -> Path:
        return self.local_rejects_dir / f"week_start={self.week_start}" / f"week_end={self.week_end}"

    @property
    def local_rejects_output_file(self) -> Path:
        return self.local_rejects_output_dir / "rejects.parquet"

    @property
    def adls_rejects_output_path(self) -> str:
        return f"{self.adls_rejects_base_path}/week_start={self.week_start}/week_end={self.week_end}/rejects.parquet"

    @property
    def state_file(self) -> Path:
        return self.state_dir / self.state_file_name

    @property
    def pending_updates_file(self) -> Path:
        return self.state_dir / self.pending_updates_file_name


def get_config() -> ExtractorConfig:
    """Load configuration from environment."""
    try:
        load_dotenv("config.env")
        return ExtractorConfig()
    except Exception as e:
        raise ConfigurationError(f"Failed to load configuration: {e}") from e

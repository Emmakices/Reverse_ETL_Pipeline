"""Custom exception hierarchy for the extraction pipeline."""

from __future__ import annotations


class ExtractorError(Exception):
    """Base exception for all extractor-related errors."""

    def __init__(self, message: str, details: dict | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class ConfigurationError(ExtractorError):
    """Raised when configuration is missing or invalid."""
    pass


class APIError(ExtractorError):
    """Base exception for API-related errors."""
    pass


class APIConnectionError(APIError):
    """Raised when unable to connect to the API."""
    pass


class APITimeoutError(APIError):
    """Raised when API request times out."""
    pass


class APIResponseError(APIError):
    """Raised when API returns an unexpected response."""
    pass


class ValidationError(ExtractorError):
    """Raised when data validation fails."""
    pass


class SchemaValidationError(ValidationError):
    """Raised when data doesn't match expected schema."""
    pass


class DataQualityError(ValidationError):
    """Raised when data quality checks fail."""
    pass


class StorageError(ExtractorError):
    """Base exception for storage-related errors."""
    pass


class LocalStorageError(StorageError):
    """Raised when local file operations fail."""
    pass


class AzureStorageError(StorageError):
    """Raised when Azure storage operations fail."""
    pass


class AuthenticationError(ExtractorError):
    """Raised when authentication fails."""
    pass


class CheckpointError(ExtractorError):
    """Raised when checkpoint operations fail."""
    pass

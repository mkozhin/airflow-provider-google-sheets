"""Custom exceptions for the Google Sheets provider."""


class GoogleSheetsProviderError(Exception):
    """Base exception for all Google Sheets provider errors."""


class GoogleSheetsAuthError(GoogleSheetsProviderError):
    """Raised when authentication with Google API fails."""


class GoogleSheetsAPIError(GoogleSheetsProviderError):
    """Raised when a Google Sheets API call fails.

    Wraps ``googleapiclient.errors.HttpError`` with a human-readable message.
    """

    def __init__(self, message: str, status_code: int | None = None, original_error: Exception | None = None):
        self.status_code = status_code
        self.original_error = original_error
        super().__init__(message)


class GoogleSheetsSchemaError(GoogleSheetsProviderError):
    """Raised when data does not match the expected schema."""


class GoogleSheetsDataError(GoogleSheetsProviderError):
    """Raised when data conversion or processing fails."""

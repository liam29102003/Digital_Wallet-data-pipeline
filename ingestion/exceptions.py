"""Custom exception hierarchy used across all ingestion pipelines.

Keeping these distinct (instead of raising bare Exception / ValueError)
lets main.py catch and report failures per-pipeline without masking bugs,
and gives Airflow (later) clean signals for retries vs. hard failures.
"""


class IngestionError(Exception):
    """Base class for all ingestion-related errors."""


class SourceConnectionError(IngestionError):
    """Raised when a source system (DB, API, filesystem) can't be reached."""


class SchemaValidationError(IngestionError):
    """Raised when extracted data is missing required columns."""


class EmptyDatasetError(IngestionError):
    """Raised when a source returns zero rows and that is not expected."""


class MalformedSourceDataError(IngestionError):
    """Raised when source data can't be parsed (bad CSV, bad JSON, etc.)."""


class ApiResponseError(IngestionError):
    """Raised when an API response is malformed or signals failure."""


class BronzeWriteError(IngestionError):
    """Raised when writing to the Databricks Bronze layer fails."""

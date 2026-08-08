"""Centralized, environment-driven configuration.

No credentials are hardcoded anywhere in this project. Everything here is
read from environment variables (populated from `.env` via python-dotenv).
Each source system gets its own small, typed dataclass so downstream
modules only import the piece of config they need.
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv

# Load .env once, as early as possible, without overriding real env vars
# that may already be set (e.g. injected by Airflow/CI later).
load_dotenv(override=False)


def _get_env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return value or ""


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    return int(raw) if raw else default


@dataclass(frozen=True)
class PostgresConfig:
    host: str = field(default_factory=lambda: _get_env("POSTGRES_HOST", "localhost"))
    port: int = field(default_factory=lambda: _get_int_env("POSTGRES_PORT", 5432))
    database: str = field(default_factory=lambda: _get_env("POSTGRES_DB", required=True))
    user: str = field(default_factory=lambda: _get_env("POSTGRES_USER", required=True))
    password: str = field(default_factory=lambda: _get_env("POSTGRES_PASSWORD", required=True))
    schema: str = field(default_factory=lambda: _get_env("POSTGRES_SCHEMA", "public"))
    connect_timeout: int = field(
        default_factory=lambda: _get_int_env("POSTGRES_CONNECT_TIMEOUT_SECONDS", 10)
    )
    transactions_chunk_size: int = field(
        default_factory=lambda: _get_int_env("POSTGRES_TRANSACTIONS_CHUNK_SIZE", 250_000)
    )

    # Testing/backfill override: when set, extraction is bounded to this
    # explicit window instead of "everything after the stored watermark".
    # Mirrors ApiConfig.fixed_date_from / fixed_date_to. Leave unset for
    # normal incremental runs — this is a manual escape hatch for
    # pulling a small, known slice (e.g. one day) for local testing.
    transactions_date_from: str = field(
        default_factory=lambda: _get_env("POSTGRES_TRANSACTIONS_DATE_FROM", "")
    )
    transactions_date_to: str = field(
        default_factory=lambda: _get_env("POSTGRES_TRANSACTIONS_DATE_TO", "")
    )

@dataclass(frozen=True)
class ApiConfig:
    base_url: str = field(default_factory=lambda: _get_env("API_BASE_URL", required=True))
    
    transactions_endpoint: str = field(
        default_factory=lambda: _get_env("API_TRANSACTIONS_ENDPOINT", "transactions")
    )
    timeout_seconds: int = field(default_factory=lambda: _get_int_env("API_TIMEOUT_SECONDS", 30))
    max_retries: int = field(default_factory=lambda: _get_int_env("API_MAX_RETRIES", 3))
    retry_backoff_seconds: int = field(
        default_factory=lambda: _get_int_env("API_RETRY_BACKOFF_SECONDS", 2)
    )
    page_size: int = field(default_factory=lambda: _get_int_env("API_PAGE_SIZE", 200))
    auth_token: str = field(default_factory=lambda: _get_env("API_AUTH_TOKEN", ""))

    fixed_date_from: str = field(default_factory=lambda: _get_env("API_TRANSACTIONS_DATE_FROM", ""))
    fixed_date_to: str = field(default_factory=lambda: _get_env("API_TRANSACTIONS_DATE_TO", ""))

    @property
    def transactions_url(self) -> str:
        return f"{self.base_url.rstrip('/')}/{self.transactions_endpoint.lstrip('/')}"


@dataclass(frozen=True)
class CsvConfig:
    input_dir: Path = field(
        default_factory=lambda: Path(_get_env("CSV_INPUT_DIR", "./datasets")).resolve()
    )


@dataclass(frozen=True)
class DatabricksConfig:
    server_hostname: str = field(
        default_factory=lambda: _get_env("DATABRICKS_SERVER_HOSTNAME", required=True)
    )
    http_path: str = field(default_factory=lambda: _get_env("DATABRICKS_HTTP_PATH", required=True))
    token: str = field(default_factory=lambda: _get_env("DATABRICKS_TOKEN", required=True))
    cluster_id: str = field(default_factory=lambda: _get_env("DATABRICKS_CLUSTER_ID", ""))
    catalog: str = field(default_factory=lambda: _get_env("BRONZE_CATALOG", "main"))
    bronze_schema: str = field(default_factory=lambda: _get_env("BRONZE_SCHEMA", "bronze"))


@dataclass(frozen=True)
class RuntimeConfig:
    log_level: str = field(default_factory=lambda: _get_env("LOG_LEVEL", "INFO"))
    log_dir: Path = field(default_factory=lambda: Path(_get_env("LOG_DIR", "./logs")).resolve())
    state_dir: Path = field(default_factory=lambda: Path(_get_env("STATE_DIR", "./state")).resolve())
    batch_id: str = field(
        default_factory=lambda: _get_env("BATCH_ID") or f"batch_{uuid.uuid4().hex[:12]}"
    )


# ---------------------------------------------------------------------------
# Source system labels — must match the values required in Bronze metadata.
# ---------------------------------------------------------------------------
class SourceSystem:
    POSTGRES = "postgres"
    CSV = "csv"
    API = "api"


# ---------------------------------------------------------------------------
# Table ownership + required-column contracts.
# See README.md for the assumption behind this mapping.
# ---------------------------------------------------------------------------
CSV_TABLE_FILES: Dict[str, str] = {
    "branches": "branches.csv",
    "merchants": "merchants.csv",
    "devices": "devices.csv",
    "payment_methods": "payment_methods.csv",
}

POSTGRES_INCREMENTAL_TABLES: Dict[str, str] = {
    # table_name -> watermark column
    "customers": "updated_at",
    "wallet_accounts": "updated_at",
}

POSTGRES_TRANSACTIONS_TABLE = "transactions"
POSTGRES_TRANSACTIONS_WATERMARK_COLUMN = "transaction_timestamp"

# ingestion/config.py
REQUIRED_COLUMNS: Dict[str, List[str]] = {
    "branches": ["branch_id", "branch_name", "city", "country", "region", "created_at"],
    "merchants": ["merchant_id", "merchant_name", "merchant_category", "city", "country", "merchant_rating", "joined_date"],
    "devices": ["device_id", "device_type", "operating_system"],
    "payment_methods": ["payment_method_id", "payment_method", "provider"],
    "customers": ["customer_id", "updated_at"],
    "wallet_accounts": ["wallet_id", "customer_id", "wallet_type", "wallet_status", "currency", "current_balance", "created_at", "updated_at"],
    "transactions": [
        "transaction_id", "wallet_id", "merchant_id", "payment_method_id", "device_id",
        "transaction_timestamp", "amount", "transaction_fee", "cashback", "loyalty_points",
        "status", "transaction_type", "location_city", "currency", "fraud_flag",
    ],
}

# Bronze table name == source table name for every table in this project.
BRONZE_TABLES: List[str] = [
    "branches",
    "merchants",
    "devices",
    "payment_methods",
    "customers",
    "wallet_accounts",
    "transactions",
]


def get_postgres_config() -> PostgresConfig:
    return PostgresConfig()


def get_api_config() -> ApiConfig:
    return ApiConfig()


def get_csv_config() -> CsvConfig:
    return CsvConfig()


def get_databricks_config() -> DatabricksConfig:
    return DatabricksConfig()


def get_runtime_config() -> RuntimeConfig:
    return RuntimeConfig()

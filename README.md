![CI](https://github.com/liam29102003/Digital_Wallet-data-pipeline/actions/workflows/ci.yml/badge.svg)

# Wallet Data Platform — Bronze Ingestion Layer

Standalone Python ingestion pipelines that extract data from three source
systems and land it, as-is, into the **Bronze** layer of a Databricks Delta
Lakehouse. No orchestrator (Airflow) yet — this phase focuses on clean,
independently runnable ingestion modules that will slot into Airflow tasks
later with zero rewrite (each `run_*()` function in `main.py` is already a
1:1 candidate for a `PythonOperator`).

## Source → Bronze mapping

| Source system | Tables | Load strategy | Bronze target |
|---|---|---|---|
| CSV (local files) | `branches`, `merchants`, `devices`, `payment_methods` | Full load (small reference data, no `updated_at`) | `bronze.branches`, `bronze.merchants`, `bronze.devices`, `bronze.payment_methods` |
| PostgreSQL (`Digital_Money`) | `customers`, `wallet_accounts` | Incremental via `updated_at` watermark | `bronze.customers`, `bronze.wallet_accounts` |
| Node.js API | `transactions` | Incremental via `transaction_time`, paginated, retried | `bronze.transactions` |

> **Assumption / spec conflict called out:** the brief's "Source Systems"
> section lists `devices` and `payment_methods` under CSV, but the
> "Requirements → PostgreSQL" section also mentions full extraction for
> them under Postgres. This implementation treats CSV as the owner of all
> four small reference tables (matches "small reference datasets, no
> `updated_at`") and keeps PostgreSQL scoped to the two operational tables
> that actually have `updated_at`. `transactions` isn't listed under any
> source table list but is clearly the API's payload (`transaction_time`
> incremental key), so it's mapped there. Adjust `ingestion/config.py` →
> `REQUIRED_COLUMNS` / `TABLE_SOURCE_MAP` if your real systems differ.

## Project structure

```text
wallet-data-platform/
├── ingestion/
│   ├── __init__.py
│   ├── config.py            # env-driven settings (dataclasses), no hardcoded secrets
│   ├── logger.py             # centralized logging (console + rotating file)
│   ├── exceptions.py         # custom exception hierarchy
│   ├── utils.py               # metadata stamping, validation, watermark manager
│   ├── databricks_writer.py  # Bronze Delta writer (Databricks Connect / Spark)
│   ├── postgres_ingestion.py
│   ├── csv_ingestion.py
│   ├── api_ingestion.py
│   └── main.py                # orchestrates CSV -> Postgres -> API, in order
│
├── datasets/                  # local CSV drop zone (branches.csv, merchants.csv, ...)
├── api/                        # placeholder for a mock/sample Node.js API (not required to run)
├── dbt/                         # placeholder for the future Silver/Gold dbt project
├── state/                       # local watermark store (JSON) - swap for a real state
│                                 # store (Delta table / Airflow Variable) later
├── logs/                        # rotating log files land here
├── tests/                       # unit test placeholders
├── .env.example
├── requirements.txt
└── README.md
```

## Why this design is Airflow-ready later

- Every pipeline is a **class** with a single `run() -> IngestionResult`
  entrypoint and no global state — becomes a `PythonOperator` (or a Task
  Group) with no internal changes.
- `batch_id` is generated once per run and threaded through every module
  as a parameter, exactly how you'd pass it via `dag_run.run_id` in Airflow.
- Watermarks are read/written through a small `WatermarkStore` interface
  in `utils.py` — the local JSON file is an implementation detail you can
  swap for a Delta table, Airflow `Variable`, or XCom without touching the
  ingestion classes.
- Config is 100% environment-driven (`.env` + `os.environ`), so the same
  code runs unchanged inside an Airflow worker with env vars / connections
  injected differently.
- Writing to Bronze is isolated in `databricks_writer.py` — ingestion
  classes never talk to Databricks directly, they just return pandas
  DataFrames.

## Databricks connectivity

`databricks_writer.py` uses **Databricks Connect** (`databricks.connect`)
to get a remote Spark session against your Databricks Free Edition
workspace/cluster, converts the pandas DataFrame to a Spark DataFrame, and
does:

```python
df.write.format("delta").mode("append").option("mergeSchema", "true") \
  .saveAsTable(f"{bronze_schema}.{table_name}")
```

The Bronze schema is created if missing (`CREATE SCHEMA IF NOT EXISTS`).
If your Free Edition workspace doesn't expose a cluster compatible with
Databricks Connect, the same class can be swapped to use
`databricks-sql-connector` against a SQL Warehouse instead — the ingestion
modules don't care, they only call `BronzeWriter.write_table(...)`.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate         # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env              # fill in real credentials
```

Drop your reference CSVs into `datasets/`:
`branches.csv`, `merchants.csv`, `devices.csv`, `payment_methods.csv`.

## Run

```bash
python ingestion/main.py
```

This runs, **in order**: CSV → PostgreSQL → API, logs progress/timing for
each stage, writes all seven Bronze Delta tables, and exits non-zero if
any pipeline failed (while still attempting the remaining ones), so it's
safe to schedule directly with cron today and with Airflow tomorrow.

## Bronze metadata columns

Every Bronze table gets, appended by `utils.add_ingestion_metadata`:

- `_ingested_at` — UTC timestamp of the write
- `source_system` — `postgres` | `csv` | `api`
- `batch_id` — one UUID per `main.py` execution, shared across all tables
  written in that run

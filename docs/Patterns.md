# Design Patterns & Architecture

This document explains some of the software engineering patterns and architecture principles I used in this data engineering project.

Some of these patterns are common in software engineering, but they are also used in data engineering with different names, such as watermarking and Medallion architecture.

The purpose of this document is to explain where these patterns are used in the project and why I used them.

---

## Resilience / Distributed Systems Patterns

### Two-phase commit

**Where:** `ingestion/utils.py` — `WatermarkStore.begin()` / `.commit()`

For each incremental pipeline, I use a watermark to remember how much data has already been ingested.

Before writing data to Bronze, `begin()` stores the new watermark as a pending value.

After the Bronze write is successful, `commit()` changes it to the committed watermark.

This is similar to a two-phase commit used in databases. The idea is to separate the **prepare** step from the **commit** step.

This helps prevent an unclear state if the pipeline crashes between the data write and the watermark update.

More details are explained in `DECISIONS.md` under the two-phase watermark commit decision.

---

### Retry with exponential backoff

**Where:**
`ingestion/api_ingestion.py` — `@retry` on `_fetch_page`

`ingestion/postgres_ingestion.py` and `postgres_transactions_ingestion.py` — `@retry` on `_connect`

These retries use the `tenacity` library.

I use retries for temporary fsils such as connection failures and timeouts.

The retry is only used for specific errors, such as:

* `Timeout`
* `ConnectionError`
* `psycopg2.OperationalError`

But the pipeline will fail immediately for errors that are anot applied  retrying, such as bad JSON or HTTP 4xx errors
This prevents the pipeline from wasting time retrying a problem that needs to be fixed instead.

---

### Fallback chain

**Where:** `ingestion/main.py` — `run_transactions_pipeline()`

For transactions, PostgreSQL is the primary source and the API is the backup source.

The pipeline will try PostgreSQL ingestion first.

If PostgreSQL fails, it will automatically try the API.

The main idea is to keep the pipeline working even when the primary source has a problem.

---

### Idempotent writes / Merge

**Where:** `dbt/wallet_dbt/models/gold/facts/fact_transactions.sql`

The fact table uses:

`incremental_strategy='merge'`

with:

`transaction_id`

as the key.

This makes it safer to run the pipeline with same data again.

For example, if a transaction was already loaded and the pipeline processes it again, the existing record is updated instead of creating a duplicate.

This is important for the 3-day lookback window because some transactions are intentionally processed again.

The merge makes the pipline reprocessing safe.

---

### Compensating action

**Where:** `ingestion/api_ingestion.py` / `postgres_transactions_ingestion.py`

The main function is `_reconcile_pending_write()`, which calls:

`writer.delete_batch()`

Sometimes a pipeline can crash after `begin()` but before `commit()`.

In this case, there may be an incomplete Bronze batch from the previous run.

When the next run starts, it finds this pending batch and removes it using the `batch_id`.

This is similar to a compensating transaction in a Saga pattern.

Instead of leaving the incomplete data, the pipeline tries to undo the previous partial write.

---

### Best-effort / Fire-and-forget

**Where:** `ingestion/metrics_writer.py` — `MetricsWriter.log()`

The `log()` function is designed so that an error in writing metrics does not stop the main pipeline.

The function catches errors and does not raise them again.

I made this decision because metrics and logging are useful, but they should not become a reason for the actual data pipeline to fail.

There is also a test for this behavior in:

`tests/test_metrics_writer.py::TestLog::test_write_failure_is_swallowed_not_raised`

---

### Known gap: Circuit breaker

**Where:** `ingestion/circuit_breaker.py`

The circuit breaker file currently exists, but it is empty and not implemented yet.

The current PostgreSQL → API fallback provides part of the same goal because the pipeline does not completely stop when PostgreSQL fails.

However, a real circuit breaker would also remember repeated failures.

For example, if PostgreSQL keeps failing, the pipeline could stop trying PostgreSQL for a certain amount of time and use the API instead.

A proper circuit breaker normally has three states:

* **Closed** — requests work normally.
* **Open** — requests are stopped because there are too many failures.
* **Half-open** — the system tries again to check if the source has recovered.

This is currently a known gap in the project.

I included it here instead of pretending it is already implemented.

---

## Structural / OOP Patterns

### Dependency Injection

**Where:** In the constructors of the ingestion classes:

* `CsvIngestion`
* `PostgresIngestion`
* `ApiIngestion`
* `PostgresTransactionsIngestion`

Instead of creating dependencies inside each class, I pass them into the constructor.

For example, the class receives the:

* `writer`
* `watermark_store`
* `config`

This makes the classes easier to test.

In the tests, I can replace these dependencies with `MagicMock()` instead of connecting to a real PostgreSQL or Databricks system.

This is one reason the test suite can run without needing the actual infrastructure.

---

### Repository pattern

**Where:**

`ingestion/utils.py` — `WatermarkStore`

`ingestion/databricks_writer.py` — `BronzeWriter`

These classes hide the actual way data is stored.

For example, the ingestion classes do not need to know that the watermark is currently stored in a JSON file.

They only use methods such as:

* `get()`
* `begin()`
* `commit()`

Similarly, Bronze ingestion uses methods such as:

* `write_table()`
* `delete_batch()`

This makes it easier to change the storage later.

For example, the watermark could eventually be moved from a JSON file to a Delta table without changing all the ingestion classes.

---

### Adapter pattern

**Where:** `ingestion/databricks_writer.py` — `BronzeWriter`

`BronzeWriter` hides the Databricks Connect and Spark details from the rest of the application.

The other parts of the code only need to call:

`write_table(df, table_name)`

They do not need to know how Spark or Databricks is being used internally.

This makes the rest of the code simpler and less dependent on a specific storage technology.

---

### Result object pattern

**Where:**

* `ApiIngestionResult`
* `CsvIngestionResult`
* `PostgresIngestionResult`
* `PostgresTransactionsIngestionResult`

These result objects are returned by the different `run()` methods.

Instead of always raising an exception when something does not completely succeed, the pipeline can return information about what happened.

For example, if 3 out of 4 CSV tables were successfully loaded, the result can contain:

* the number of rows loaded for each table
* which tables failed

This makes partial success something that the application can inspect.

Exceptions are mainly used for serious problems that prevented the ingestion from being performed.

---

### Factory functions

**Where:** `ingestion/config.py`

Examples include:

* `get_postgres_config()`
* `get_api_config()`
* `get_csv_config()`
* `get_databricks_config()`
* `get_runtime_config()`

These functions create the different configuration objects.

One benefit is that the tests can create configuration objects directly with test values instead of always reading environment variables.

---

### Value objects / Immutable config

**Where:** `ingestion/config.py`

The configuration classes use:

`@dataclass(frozen=True)`

This means that after a configuration object is created, its values cannot be changed.

This helps prevent one part of the pipeline from accidentally changing configuration that another part of the pipeline is using.

---

### Custom exception hierarchy

**Where:** `ingestion/exceptions.py`

I created a base exception called:

`IngestionError`

and several more specific exceptions:

* `SourceConnectionError`
* `SchemaValidationError`
* `EmptyDatasetError`
* `MalformedSourceDataError`
* `ApiResponseError`
* `BronzeWriteError`

This allows the application to know what type of problem happened.

For example, a connection problem can be different from a schema validation problem.

The main orchestration layer can catch errors generally, while the logs and tests can still identify the more specific error type.

---

## Architectural Principles

### Layered architecture / Separation of concerns

**Where:** The overall project structure

The project separates different responsibilities into different layers.

For example:

* `ingestion/` contains the ingestion logic.
* `databricks_writer.py` handles storage.
* `airflow/dags/` handles orchestration.

The Airflow DAG calls the same `run_*_pipeline()` functions that can also be run locally.

This means the ingestion code does not depend directly on Airflow.

One benefit is that I can change the orchestration tool later without needing to rewrite the ingestion logic.

---

### Twelve-factor configuration

**Where:**

`ingestion/config.py`

`.env.example`

The configuration values are stored in environment variables instead of being hardcoded in the Python code.

This includes things like database connection information and other runtime configuration.

Because of this, the same application can run locally using a `.env` file or inside Airflow using environment variables.

---

### Single Responsibility Principle

**Where:** Different modules such as:

* `config.py`
* `logger.py`
* `exceptions.py`
* `utils.py`

Each ingestion source also has its own class.

I tried to make each module responsible for one main thing.

For example:

* `WatermarkStore` handles watermark management.
* `BronzeWriter` handles writing to Bronze.
* Configuration classes handle configuration.

This makes the code easier to understand and maintain.

---

## Data Engineering Patterns

Some data engineering concepts used in this project are also based on common software engineering ideas.

| Data engineering term                        | What I understand it as                                                                                 |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| **ELT**                                      | Data is loaded first and transformation is done later in the warehouse/lakehouse using dbt.             |
| **Medallion architecture**                   | Data is processed through different layers: Bronze → Silver → Gold. Each layer has a different purpose. |
| **Watermarking / Incremental checkpointing** | The pipeline remembers where it stopped so the next run does not need to process everything again.      |
| **SCD Type 1 vs Type 2**                     | Type 1 keeps the latest value, while Type 2 keeps historical versions when the history is important.    |
| **Star schema**                              | A data model where fact tables connect to dimension tables, making analytical queries easier.           |

---

## Patterns I can explain in an interview

If I am asked what design patterns I used in this project, these are the main ones I would explain.

### 1. Two-phase commit for watermarks

This is one of the more important decisions in the project.

I separated the watermark process into `begin()` and `commit()` so that a pipeline crash does not easily create an incorrect watermark state.

The tests in `tests/test_watermark_store.py` also verify this behavior.

### 2. Dependency injection

I used dependency injection in the ingestion classes.

This made the testing easier because I can replace real dependencies with mocks.

The whole test suite can therefore run without needing a live database or Databricks connection.

### 3. Idempotent merge

The Gold fact table uses `merge` based on `transaction_id`.

This allows the pipeline to safely process the same transaction more than once.

It is especially useful for the 3-day lookback window.

### 4. Fallback source

PostgreSQL is the primary transaction source and the API is the fallback.

If PostgreSQL fails, the pipeline can continue using the API instead of completely stopping.

### 5. Honest limitation — Circuit Breaker

A real circuit breaker is not implemented yet.

There is an empty `circuit_breaker.py` file, but I did not want to say that the project has a circuit breaker when it actually does not.

The current implementation only has the fallback behavior.

I think it is better to clearly mention this as a future improvement rather than claiming something that is not implemented.

# Engineering Decisions

These are some notes about the important decisions I made while building this data pipeline.

---

## 1. Two-phase watermark commit for incremental loads

**Context.**
For incremental pipelines, such as Postgres customers, wallets, and transactions, I use a watermark to remember the latest timestamp that was already ingested. This helps the next pipeline run only extract new records.

At first, a simple approach could be to write the data into Bronze and then update the watermark. But there may be a problem if the pipeline crashes between these  steps.

For example, if the watermark is updated before the Bronze write is completely successful, the next run may think the data was already loaded. This can cause some data to be skipped.

**Decision.**
I decided to split the watermark update into two steps: `begin()` and `commit()`.

`begin()` records the new watermark as a pending value before the Bronze write starts. It also stores the `batch_id` of the current run.

After the Bronze write is successfully completed, `commit()` changes the pending watermark into the committed watermark.

If the pipeline crashes before the commit, the next run can detect the pending watermark. It then removes the incomplete Bronze batch using the `batch_id` and removes the pending watermark before starting a new extraction.

**Alternatives considered.**

* **Write first, then update the watermark** — This is simpler, but if the process crashes between the two steps, the pipeline can have inconsistent progress.
* **Use idempotent upsert with a business key** — This can help prevent duplicates, but it can still have a problem if the pipeline thinks a write was successful when it was actually interrupted.

**Trade-off accepted.**
This approach adds one extra read/write operation to the watermark store for each incremental run. It also makes the `WatermarkStore` JSON structure more complicated than a simple `{key: value}` map.

However, the watermark is read only and written once per run, so this extra work is a small thing compared with the benefit of avoiding data loss. 

---

## 2. Point-in-time joins for SCD2 dimensions in `fact_transactions`

**Context.**
The `dim_customers`, `dim_wallet`, and `dim_merchant` tables use SCD Type 2. This means they can have multiple versions of the same customer, wallet, or merchant rows over time.

If I join the fact table only using the natural key, the transaction could be connected to the current dimension record instead of the record that was valid when the transaction actually happened.

For example, if a customer's risk tier changed after a transaction, a historical transaction could incorrectly show the customer's current risk tier.

**Decision.**
I decided to use a point-in-time join.

The transaction is joined to the dimension version where:

`transaction_timestamp >= dbt_valid_from`

and

`transaction_timestamp < dbt_valid_to`

This means the transaction uses the dimension record that was valid at the time when the transaction happened.

I also added the `no_overlapping_scd2_windows` test to the snapshots. This checks that two versions of the same record do not have overlapping validity periods.

If the validity periods overlap, the join could return more than one dimension record for the same transaction. The test helps catch this before it affects `fact_transactions`.

**Alternatives considered.**

* **Join only to `is_current = true` records** — This is much easier, but it is not correct for historical analysis.
* **Copy all dimension attributes directly into the fact table** — This removes the need for the range join, but it makes the fact table more dependent on the dimension logic.

**Trade-off accepted.**
A range join is more expensive than a normal equality join. It also depends on the SCD2 validity periods being correct.

Because of this, I added tests instead of assuming the data will always be correct.

I also added `assert_fact_transactions_no_fanout.sql` to check that the final fact table still has exactly one row for each `transaction_id`.

---

## 3. 3-day lookback window for the incremental fact load

**Context.**
At first, `fact_transactions` could be rebuilt completely on every run. This is not a good approach when the fact table becomes large because the pipeline has to process the whole history every run.

So I changed it to use:

`materialized='incremental'`

and

`incremental_strategy='merge'`

A simple incremental filter could be:

`transaction_timestamp > max(transaction_timestamp already loaded)`

But this assumes that transactions always arrive in the correct timestamp order.

In reality, a transaction can arrive late because of API pagination, network delays, or clock differences between systems.

For example, a transaction from yesterday could arrive today even though some newer transactions have already been loaded.

**Decision.**
I decided to scan the last 3 days of `stg_transactions` on every incremental run.

The pipeline uses `transaction_id` for the merge. So if a transaction was already loaded, processing it again will not create another row. It will simply merge with the existing record.

This adds a little extra processing, but it helps prevent late-arriving transactions from being permanently missed.

**Alternatives considered.**

* **Only use `> max(timestamp)`** — This is faster, but a late-arriving transaction can be missed.
* **Full rebuild every time** — This is safer, but becomes more expensive as the table grows.
* **Use `_ingested_at` instead of `transaction_timestamp`** — This can solve the late-arriving problem, but then the meaning of "new transaction" becomes based on when the data arrived rather than when the transaction happened.

**Trade-off accepted.**

The disadvantage is that each run processes some records that were already processed. However, the amount is limited to the last 3 days.

---

## 4. Primary and fallback source for transactions

**Context.**
Transactions dataset can come from two sources:

1. PostgreSQL as the primary source
2. Transactions API as the backup source

PostgreSQL transactions are extracted in chunks, while the API uses pagination.

Since both sources can work independently, I wanted the pipeline to continue working if the main source face a temporary failure.

**Decision.**
I decided to use PostgreSQL first.

If `run_postgres_transactions_pipeline` fails, the pipeline automatically uses the API pipeline in the same Airflow run.

I made this logic visible in the Airflow DAG instead of hiding everything inside a Python function.

The `api_transactions_task` uses:

`TriggerRule.ALL_FAILED`

This means it only runs when the PostgreSQL transaction task fails.

Then, the `transactions_complete` task uses:

`TriggerRule.ONE_SUCCESS`

So the DAG can be successful if either PostgreSQL or the API successfully loads the transactions.

**Alternatives considered.**

* **Only use PostgreSQL database and stop the pipeline when it fails** — This is simpler, but a temporary PostgreSQL failure would stop the whole pipeline.
* **Always run both sources and remove duplicates later** — This gives redundancy, but it creates unnecessary load because both sources would be used even when PostgreSQL is working normally.

**Trade-off accepted.**
The two sources have separate watermarks.

For example, the PostgreSQL source has its own watermark and the API source has its own watermark.

If PostgreSQL fails and the API is used, the PostgreSQL watermark is not changed. On the next run, the pipeline will try PostgreSQL again from the last successful point.

This is intentional because each watermark represents the progress of its own source.

One disadvantage is that if PostgreSQL stays unavailable for a long time, the API may process some of the same data again. However, the `merge` logic in the Gold layer can handle these duplicates.

---



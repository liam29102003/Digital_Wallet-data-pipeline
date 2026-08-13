# Data Quality & Testing

This document explains what I tested in the pipeline, what severity I used for each test, and why I chose these tests.

The main purpose is to have one place where I can understand the data quality checks without looking through every YAML file.

---

## Severity model

I use two levels for tests:

* `error` — the test fails the build when the condition is not correct.
* `warn` — the test gives a warning but does not fail the build.

The rules I follow are:

* **`error`** — I use this when the condition is very important, such as a primary key or a table grain. I also use it when I already confirmed the condition with real data. If this test fails, it usually means something is wrong.
* **`warn`** — I use this when I expect the condition to be true, but I have not fully confirmed it with enough real data. I also use it when there can be a small legitimate edge case.

My goal is to move tests from `warn` to `error` when I have enough information to confirm the rule.

A `warn` should not stay there forever without checking. Each warning should have a reason and, if possible, a plan to make it stricter later.

---

## Bronze layer — Source contracts

| Source table                                   | What's tested                                                           | Severity     | Why                                                                                               |
| ---------------------------------------------- | ----------------------------------------------------------------------- | ------------ | ------------------------------------------------------------------------------------------------- |
| `customers`, `wallet_accounts`, `transactions` | `unique` + `not_null` on natural key                                    | error        | These keys are used by many downstream joins, so they must be valid.                              |
| `customers`, `wallet_accounts`, `transactions` | `not_null` on watermark column (`updated_at` / `transaction_timestamp`) | error        | A null watermark can cause problems with incremental extraction.                                  |
| `merchants`, `devices`, `payment_methods`      | `not_null` on natural key                                               | error        | These are reference tables, so a null key means the source data is not correct.                   |
| `customers`, `wallet_accounts`                 | Freshness: warning after 24h, error after 48h                           | warn / error | These tables should be updated regularly. Old data can mean the Postgres ingestion has a problem. |
| `transactions`                                 | Freshness: warning after 6h, error after 24h                            | warn / error | Transactions are the main event data, so I use a shorter freshness threshold.                     |

---

## Silver layer — Staging contracts

| Model                                                                                                             | What's tested                                                   | Severity | Why                                                                                                                 |
| ----------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------- |
| `stg_merchants`, `stg_devices`, `stg_payment_methods`, `stg_customers`, `stg_wallet_accounts`, `stg_transactions` | `unique` + `not_null` on natural key                            | error    | These tests make sure each staging table has the expected grain and no duplicate keys.                              |
| `stg_wallet_accounts.customer_id` → `stg_customers`                                                               | `relationships`                                                 | error    | A wallet without a matching customer can cause problems when building the fact table.                               |
| `stg_transactions.wallet_id` / `merchant_id` / `payment_method_id` / `device_id`                                  | `relationships` to staging tables                               | error    | These are the keys used by `fact_transactions`, so they need to have matching records.                              |
| `stg_customers.risk_level`                                                                                        | `accepted_values` (`Low`, `Medium`, `High`)                     | **warn** | This list is based on a limited sample. I have not fully confirmed that there are no other possible values.         |
| `stg_wallet_accounts.wallet_status`                                                                               | `accepted_values` (`Active`, `Inactive`, `Suspended`, `Closed`) | **warn** | Same reason. The list is based on the current sample data.                                                          |
| `stg_customers.kyc_status`                                                                                        | `accepted_values` (`Verified`, `Failed`, `Pending`, `Expired`)  | error    | I checked this against real data, so I made the test stricter.                                                      |
| `stg_transactions.status`                                                                                         | `accepted_values` (`Success`, `Failed`, `Pending`, `Reversed`)  | error    | The values were confirmed with real data, so this is an error if something else appears.                            |
| `stg_transactions.fraud_flag`                                                                                     | `not_null`                                                      | error    | The value is converted to a boolean during transformation. A null means something went wrong during the conversion. |
| `stg_transactions.transaction_timestamp`                                                                          | `not_null`                                                      | error    | This field is important for the incremental fact load and for joining dimensions based on time.                     |

### Why `risk_level` and `wallet_status` are warnings

The `risk_level` and `wallet_status` tests use `warn` intentionally.

The accepted values were created from the sample data I had. I have not checked enough data to be completely sure that these are all possible values.

For `kyc_status` and transaction `status`, I had enough data to confirm the values, so I changed those tests from `warn` to `error`.

The same should be done for `risk_level` and `wallet_status when there is enough data to confirm their complete list of values.

---

## Snapshot layer — SCD2 checks

| Snapshot                                                               | Test                          | Severity | Why                                                                                                                       |
| ---------------------------------------------------------------------- | ----------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------- |
| `customers_snapshot`, `wallet_accounts_snapshot`, `merchants_snapshot` | `not_null` on natural key     | error    | The natural key is important for maintaining the correct grain of the snapshots.                                          |
| `customers_snapshot`, `wallet_accounts_snapshot`, `merchants_snapshot` | `no_overlapping_scd2_windows` | error    | The fact table uses point-in-time joins, so two versions of the same record should not have overlapping validity periods. |

`no_overlapping_scd2_windows` is a custom generic test that I created for this project.

It is located at:

`macros/generic_tests/test_no_overlapping_scd2_windows.sql`

It is not a standard built-in dbt test.

The test checks the different versions of the same natural key and fails when their validity periods overlap.

This is important because overlapping SCD2 records could cause a transaction to match more than one dimension version.

---

## Gold layer — Dimensions

| Model                                         | Test                                                       | Severity | Why                                                                                      |
| --------------------------------------------- | ---------------------------------------------------------- | -------- | ---------------------------------------------------------------------------------------- |
| Every `dim_*`                                 | `unique` + `not_null` on surrogate key                     | error    | The surrogate key is used to join the fact table, so it needs to be unique and not null. |
| `dim_customers`, `dim_wallet`, `dim_merchant` | `not_null` + `at_most_one_current_version` on `is_current` | error    | There should not be more than one current version of the same entity.                    |
| `dim_customers.kyc_status`                    | `accepted_values`                                          | error    | The values were already confirmed in the Silver layer.                                   |
| `dim_wallet.wallet_status`                    | `accepted_values`                                          | **warn** | This still has the same uncertainty as `stg_wallet_accounts.wallet_status`.              |

`at_most_one_current_version` is another custom generic test:

`macros/generic_tests/test_at_most_one_current_version.sql`

The normal `is_current: not_null` test is not enough for this case.

For example, if a customer accidentally has two current rows, both rows can still have `is_current = true`, so the normal not-null test will pass.

The custom test makes sure there is no more than one current version.

---

## Gold layer — Fact table

| Column                                    | Test                         | Severity | Why                                                                                                                             |
| ----------------------------------------- | ---------------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------- |
| `transaction_id`                          | `unique` + `not_null`        | error    | This is the grain of the fact table. There should be one row for each transaction.                                              |
| `device_sk`, `payment_method_sk`          | `not_null` + `relationships` | error    | These are Type 1 dimensions and use direct key joins, so a missing relationship means something is wrong.                       |
| `wallet_sk`, `customer_sk`, `merchant_sk` | `not_null` + `relationships` | **warn** | These use point-in-time joins with SCD2 history. A null can sometimes happen around the edges of the dimension validity period. |
| `transaction_timestamp`                   | `not_null`                   | error    | This is needed for the point-in-time dimension joins.                                                                           |
| `status`                                  | `accepted_values`            | error    | The allowed values were already confirmed in the staging layer.                                                                 |

### Why Type 1 and Type 2 foreign keys have different severity

There is a reason why `device_sk` and `payment_method_sk` use `error`, while `wallet_sk`, `customer_sk`, and `merchant_sk` use `warn`.

For Type 1 dimensions, the join is a direct equality join. If there is no matching record, it normally means the data is broken.

For SCD2 dimensions, the join also depends on the transaction timestamp and the validity period of the dimension record.

There can be a small edge case where a transaction falls outside the available SCD2 history. In that case, the foreign key can be null even though the source data itself is not necessarily broken.

Because of this, I use `warn` for these columns so the issue can be investigated without automatically failing the entire build.

---

## Singular tests — Whole-table checks

I also created some tests that check the whole fact table instead of only checking individual columns.

| Test                                                     | What it checks                                                                                                                | Why it exists                                                                                          |
| -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `assert_fact_transactions_matches_staging_row_count.sql` | Checks that the row count in `fact_transactions` matches the number of distinct `transaction_id` values in `stg_transactions` | Helps detect missing or duplicated transactions during the fact table build.                           |
| `assert_fact_transactions_no_fanout.sql`                 | Checks that one `transaction_id` does not appear more than once in `fact_transactions`                                        | Protects against duplicate rows caused by problems with the SCD2 joins or future changes to the joins. |

These tests are useful because normal column-level tests may not detect every problem with the overall shape of the fact table.

---

-- Singular test: fact_transactions must have exactly one row per
-- transaction_id — no fan-out.
--
-- The three point-in-time range joins in fact_transactions.sql
-- (wallet_history, customer_history, merchant_history) each rely on
-- every SCD2 dimension having non-overlapping [dbt_valid_from,
-- dbt_valid_to) windows per natural key. That invariant is now guarded
-- directly on the snapshots themselves (see the
-- no_overlapping_scd2_windows generic test on branches_snapshot /
-- merchants_snapshot / customers_snapshot / wallet_accounts_snapshot).
--
-- This test checks the JOINED OUTPUT instead of the inputs: if those
-- windows were ever violated anyway (a hand-edited snapshot, a future
-- change to check_cols/updated_at that reintroduces overlap, or a new
-- join added to this model without the same care), a single transaction
-- would silently fan out into more than one row here. Nothing else in
-- the current test suite would catch that — the existing not_null /
-- relationships tests on wallet_sk / customer_sk / merchant_sk only
-- confirm the FK values are valid, not that exactly one row exists per
-- transaction.
--
-- dbt singular tests pass when the query returns ZERO rows.

select
    transaction_id,
    count(*) as row_count
from {{ ref('fact_transactions') }}
group by transaction_id
having count(*) > 1
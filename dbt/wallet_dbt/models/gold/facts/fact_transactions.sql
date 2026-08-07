-- One row per transaction. Joins to Type 2 dimensions use effective-date
-- range joins against transaction_timestamp so each transaction resolves
-- to the dimension version that was actually current at the time the
-- transaction happened — not whatever the dimension looks like today.
-- Type 1 dimensions (device, payment_method) join directly on natural key
-- since they carry no history.
--
-- dim_branch is intentionally NOT joined here: no branch_id exists on
-- transactions or wallet_accounts in the current source data. See Gold
-- layer documentation for the recommended fix.
--
-- INCREMENTAL STRATEGY
-- ---------------------------------------------------------------------
-- This is the platform's highest-volume table and was previously a full
-- rebuild on every run — every historical transaction was re-read and
-- re-joined against every SCD2 dimension on every `dbt run`, even though
-- only a small slice of rows are actually new each run. That cost grows
-- unbounded with history and doesn't reflect what changed.
--
-- Incremental + merge fixes this: only transactions newer than what's
-- already in this table are extracted, transformed, and joined. The
-- point-in-time dimension joins are still correct under this scheme,
-- because each *new* transaction is independently joined against the
-- full dimension history to find its correct effective version — we're
-- reducing how many transactions get processed, not changing how a
-- given transaction resolves its dimension keys.
--
-- LOOKBACK WINDOW: transaction_timestamp is the API's sort/watermark
-- key, but pages can still land with a small amount of clock skew or
-- out-of-order arrival between the source system and Bronze landing
-- time. A naive `> max(transaction_timestamp) in this table` filter
-- would permanently miss any transaction that arrives late with an
-- earlier timestamp than one already loaded. A 3-day lookback re-scans
-- a small, bounded, recent slice of source data on every run (cheap)
-- and lets `merge` on transaction_id de-duplicate anything reprocessed,
-- which is far safer than a razor-thin real-time cutoff.
--
-- FULL REFRESH: run `dbt run --select fact_transactions --full-refresh`
-- to force a full rebuild (e.g. after a backfill, a logic change to the
-- dimension joins, or the first-ever run before any incremental state
-- exists).

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with transactions as (

    select * from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    where transaction_timestamp >= (
        select coalesce(max(transaction_timestamp), '1900-01-01') - interval '3 days'
        from {{ this }}
    )
    {% endif %}

),

wallet_history as (

    select
        dbt_scd_id as wallet_sk,
        wallet_id,
        customer_id,
        dbt_valid_from,
        coalesce(dbt_valid_to, timestamp('9999-12-31')) as dbt_valid_to
    from {{ ref('wallet_accounts_snapshot') }}

),

customer_history as (

    select
        dbt_scd_id as customer_sk,
        customer_id,
        dbt_valid_from,
        coalesce(dbt_valid_to, timestamp('9999-12-31')) as dbt_valid_to
    from {{ ref('customers_snapshot') }}

),

merchant_history as (

    select
        dbt_scd_id as merchant_sk,
        merchant_id,
        dbt_valid_from,
        coalesce(dbt_valid_to, timestamp('9999-12-31')) as dbt_valid_to
    from {{ ref('merchants_snapshot') }}

),

device as (

    select
        device_id as device_sk,
        device_id
    from {{ ref('stg_devices') }}

),

payment_method as (

    select
        payment_method_id as payment_method_sk,
        payment_method_id
    from {{ ref('stg_payment_methods') }}

),

final as (

    select
        t.transaction_id,

        -- dimension surrogate keys
        w.wallet_sk,
        c.customer_sk,
        m.merchant_sk,
        d.device_sk,
        p.payment_method_sk,

        -- degenerate / descriptive attributes
        t.transaction_timestamp,
        t.status,
        t.transaction_type,
        t.location_city,
        t.currency,
        t.fraud_flag,

        -- measures
        t.amount,
        t.transaction_fee,
        t.cashback,
        t.loyalty_points

    from transactions t
    left join wallet_history w
        on t.wallet_id = w.wallet_id
        and t.transaction_timestamp >= w.dbt_valid_from
        and t.transaction_timestamp <  w.dbt_valid_to
    left join customer_history c
        on w.customer_id = c.customer_id
        and t.transaction_timestamp >= c.dbt_valid_from
        and t.transaction_timestamp <  c.dbt_valid_to
    left join merchant_history m
        on t.merchant_id = m.merchant_id
        and t.transaction_timestamp >= m.dbt_valid_from
        and t.transaction_timestamp <  m.dbt_valid_to
    left join device d
        on t.device_id = d.device_id
    left join payment_method p
        on t.payment_method_id = p.payment_method_id

)

select * from final
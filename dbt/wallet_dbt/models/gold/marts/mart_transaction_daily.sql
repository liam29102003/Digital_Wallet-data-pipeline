-- Gold-layer daily aggregate mart for the Executive Payment Overview
-- Power BI dashboard.
--
-- GRAIN: one row per (transaction_date, currency, transaction_type, status).
-- City and merchant category are deliberately excluded from this grain —
-- those cuts are analyzed directly against fact_transactions + dimensions,
-- not pre-aggregated here.
--
-- MEASURES ARE ADDITIVE ONLY. avg_transaction_amount and fraud_rate are
-- intentionally NOT stored — both are ratios, and pre-computing a ratio
-- at this grain would make it non-summable if Power BI later rolls this
-- mart up further (e.g. to a weekly or currency-only view). Power BI
-- computes them at query time from the additive measures:
--     average transaction value = total_amount / transaction_count
--     fraud rate                = fraud_transaction_count / transaction_count
--
-- MATERIALIZATION: table (full rebuild), not incremental — see model
-- config below and the project README / capstone notes for the
-- reasoning tied to fact_transactions' own incremental/merge behavior.

with source as (

    select
        transaction_id,
        transaction_timestamp,
        currency,
        transaction_type,
        status,
        fraud_flag,
        amount,
        transaction_fee,
        cashback,
        loyalty_points
    from {{ ref('fact_transactions') }}

),

aggregated as (

    select
        cast(transaction_timestamp as date) as transaction_date,
        currency,
        transaction_type,
        status,

        count(transaction_id) as transaction_count,
        coalesce(sum(amount), 0) as total_amount,
        coalesce(sum(transaction_fee), 0) as total_transaction_fee,
        coalesce(sum(cashback), 0) as total_cashback,
        coalesce(sum(loyalty_points), 0) as total_loyalty_points,

        count(case when fraud_flag then transaction_id end) as fraud_transaction_count,
        coalesce(sum(case when fraud_flag then amount else 0 end), 0) as fraud_amount

    from source
    group by
        cast(transaction_timestamp as date),
        currency,
        transaction_type,
        status

)

select
    transaction_date,
    currency,
    transaction_type,
    status,
    transaction_count,
    total_amount,
    total_transaction_fee,
    total_cashback,
    total_loyalty_points,
    fraud_transaction_count,
    fraud_amount
from aggregated
-- Daily transaction summary by merchant category, city, and currency.
-- Grain: one row per (transaction_date, merchant_category, location_city, currency).
-- Rebuilt as a full table on every run — the aggregation is cheap relative
-- to fact_transactions' own incremental merge, so there's no need for
-- this mart to track its own incremental state separately.

select
    date_trunc('day', t.transaction_timestamp) as transaction_date,
    m.merchant_category,
    t.location_city,
    t.currency,

    count(*) as transaction_count,
    sum(t.amount) as total_amount,
    sum(t.transaction_fee) as total_fees,
    sum(t.cashback) as total_cashback,
    sum(t.loyalty_points) as total_loyalty_points,

    sum(case when t.fraud_flag then 1 else 0 end) as fraud_count,
    sum(case when t.status = 'Success' then 1 else 0 end) as successful_count,
    round(
        sum(case when t.status = 'Success' then 1 else 0 end) / nullif(count(*), 0),
        4
    ) as success_rate

from {{ ref('fact_transactions') }} t
left join {{ ref('dim_merchant') }} m
    on t.merchant_sk = m.merchant_sk

group by 1, 2, 3, 4
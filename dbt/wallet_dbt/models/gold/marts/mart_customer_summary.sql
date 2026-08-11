-- Lifetime customer summary. Grain: one row per CURRENT customer
-- (customer_id), with metrics aggregated across every transaction that
-- customer has ever made — regardless of which historical dim_customers
-- version (customer_sk) was active at the time of each transaction.
--
-- Why the two-step join: fact_transactions.customer_sk is versioned
-- (SCD2) — it points at whichever customer_sk was current AT THE TIME
-- of the transaction, not necessarily today's. Joining fact directly to
-- dim_customers WHERE is_current would silently drop any transaction
-- made under an older version of the customer's record (e.g. before a
-- risk_level or kyc_status change). Mapping every transaction to the
-- stable customer_id first, then aggregating, avoids that undercount.

with customer_facts as (

    select
        c.customer_id,
        t.transaction_id,
        t.amount,
        t.transaction_timestamp,
        t.fraud_flag,
        t.status
    from {{ ref('fact_transactions') }} t
    inner join {{ ref('dim_customers') }} c
        on t.customer_sk = c.customer_sk

),

aggregated as (

    select
        customer_id,
        count(transaction_id) as lifetime_transaction_count,
        sum(amount) as lifetime_spend,
        avg(amount) as avg_transaction_amount,
        min(transaction_timestamp) as first_transaction_at,
        max(transaction_timestamp) as last_transaction_at,
        sum(case when fraud_flag then 1 else 0 end) as fraud_transaction_count,
        sum(case when status = 'Success' then 1 else 0 end) as successful_transaction_count
    from customer_facts
    group by customer_id

)

select
    cur.customer_id,
    cur.first_name,
    cur.last_name,
    cur.risk_level,
    cur.kyc_status,
    cur.country,

    coalesce(a.lifetime_transaction_count, 0) as lifetime_transaction_count,
    coalesce(a.lifetime_spend, 0) as lifetime_spend,
    a.avg_transaction_amount,
    a.first_transaction_at,
    a.last_transaction_at,
    coalesce(a.fraud_transaction_count, 0) as fraud_transaction_count,
    coalesce(a.successful_transaction_count, 0) as successful_transaction_count

from {{ ref('dim_customers') }} cur
left join aggregated a
    on cur.customer_id = a.customer_id
where cur.is_current
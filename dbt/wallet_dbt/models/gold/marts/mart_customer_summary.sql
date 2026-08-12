
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


with fact_count as (

    select count(*) as row_count
    from {{ ref('fact_transactions') }}

),

staging_count as (

    select count(distinct transaction_id) as row_count
    from {{ ref('stg_transactions') }}

)

select
    fact_count.row_count as fact_row_count,
    staging_count.row_count as staging_row_count
from fact_count
cross join staging_count
where fact_count.row_count != staging_count.row_count
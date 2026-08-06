-- Merchant dimension, SCD Type 2, sourced from the merchants snapshot
-- (check strategy). Tracks category/rating changes over time.

select
    dbt_scd_id as merchant_sk,
    merchant_id,
    merchant_name,
    merchant_category,
    city,
    country,
    merchant_rating,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_current
from {{ ref('merchants_snapshot') }}
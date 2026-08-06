-- Customer dimension, SCD Type 2, sourced from the customers snapshot
-- (timestamp strategy). One row per customer per version of their
-- attributes over time.

select
    dbt_scd_id as customer_sk,
    customer_id,
    first_name,
    last_name,
    gender,
    date_of_birth,
    email,
    phone,
    occupation,
    income_level,
    risk_level,
    kyc_status,
    city,
    country,
    registration_date,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_current
from {{ ref('customers_snapshot') }}
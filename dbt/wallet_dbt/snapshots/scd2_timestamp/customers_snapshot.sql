{% snapshot customers_snapshot %}

{{
    config(
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True,
    )
}}

select
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
    created_at,
    updated_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
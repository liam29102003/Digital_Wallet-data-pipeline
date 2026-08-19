{% snapshot merchants_snapshot %}

{{
    config(
        unique_key='merchant_id',
        strategy='check',
        check_cols=['merchant_name', 'merchant_category', 'city', 'country', 'merchant_rating'],
        invalidate_hard_deletes=True,
    )
}}

select
    merchant_id,
    merchant_name,
    merchant_category,
    city,
    country,
    merchant_rating,
    cast(joined_date as timestamp) as joined_date
from {{ ref('stg_merchants') }}

{% endsnapshot %}
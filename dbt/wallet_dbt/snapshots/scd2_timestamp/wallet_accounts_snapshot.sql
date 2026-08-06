{% snapshot wallet_accounts_snapshot %}

{{
    config(
        unique_key='wallet_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True,
    )
}}

select
    wallet_id,
    customer_id,
    wallet_type,
    wallet_status,
    currency,
    current_balance,
    created_at,
    updated_at
from {{ ref('stg_wallet_accounts') }}

{% endsnapshot %}
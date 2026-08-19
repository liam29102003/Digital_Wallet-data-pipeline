
select
    dbt_scd_id as wallet_sk,
    wallet_id,
    customer_id,
    wallet_type,
    wallet_status,
    currency,
    current_balance,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_current
from {{ ref('wallet_accounts_snapshot') }}
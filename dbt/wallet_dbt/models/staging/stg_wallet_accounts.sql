-- Cleans and standardizes bronze.wallet_accounts: casts current_balance
-- to a fixed-precision numeric type and timestamps, removes duplicate
-- wallet_id records. Dedup prefers the most recent business update
-- (updated_at) before falling back to ingestion recency, matching the
-- incremental-load pattern of the source table.

with source as (

    select * from {{ source('bronze', 'wallet_accounts') }}

),

renamed as (

    select
        wallet_id,
        customer_id,
        wallet_type,
        wallet_status,
        currency,
        cast(current_balance as decimal(18, 2)) as current_balance,
        coalesce(
            try_to_timestamp(created_at, 'M/d/yyyy H:mm'),
            try_to_timestamp(created_at)
        ) as created_at,
        coalesce(
            try_to_timestamp(updated_at, 'M/d/yyyy H:mm'),
            try_to_timestamp(updated_at)
        ) as updated_at,

        -- ingestion metadata, preserved as-is
        _ingested_at,
        source_system,
        batch_id

    from source

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by wallet_id
            order by updated_at desc, _ingested_at desc
        ) as _row_num

    from renamed

)

select
    wallet_id,
    customer_id,
    wallet_type,
    wallet_status,
    currency,
    current_balance,
    created_at,
    updated_at,
    _ingested_at,
    source_system,
    batch_id
from deduplicated
where _row_num = 1

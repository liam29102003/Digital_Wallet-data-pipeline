-- Cleans and standardizes bronze.transactions: casts numeric and
-- timestamp columns, standardizes fraud_flag to a real boolean, and
-- removes duplicate transaction_id records. This is the highest-volume
-- staging model and the one every downstream fact/mart will build on.

with source as (

    select * from {{ source('bronze', 'transactions') }}

),

renamed as (

    select
        transaction_id,
        wallet_id,
        merchant_id,
        payment_method_id,
        device_id,
        coalesce(
            try_to_timestamp(transaction_timestamp, 'M/d/yyyy H:mm'),
            try_to_timestamp(transaction_timestamp)
        ) as transaction_timestamp,
        cast(amount as decimal(18, 2)) as amount,
        cast(transaction_fee as decimal(18, 4)) as transaction_fee,
        cast(cashback as decimal(18, 4)) as cashback,
        cast(loyalty_points as int) as loyalty_points,
        status,
        transaction_type,
        location_city,
        currency,
        cast(fraud_flag as boolean) as fraud_flag,

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
            partition by transaction_id
            order by _ingested_at desc
        ) as _row_num

    from renamed

)

select
    transaction_id,
    wallet_id,
    merchant_id,
    payment_method_id,
    device_id,
    transaction_timestamp,
    amount,
    transaction_fee,
    cashback,
    loyalty_points,
    status,
    transaction_type,
    location_city,
    currency,
    fraud_flag,
    _ingested_at,
    source_system,
    batch_id
from deduplicated
where _row_num = 1


with source as (

    select * from {{ source('bronze', 'payment_methods') }}

),

renamed as (

    select
        payment_method_id,
        payment_method,
        provider,

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
            partition by payment_method_id
            order by _ingested_at desc
        ) as _row_num

    from renamed

)

select
    payment_method_id,
    payment_method,
    provider,
    _ingested_at,
    source_system,
    batch_id
from deduplicated
where _row_num = 1

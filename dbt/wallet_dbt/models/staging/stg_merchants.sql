
with source as (

    select * from {{ source('bronze', 'merchants') }}

),

renamed as (

    select
        merchant_id,
        merchant_name,
        merchant_category,
        city,
        country,
        cast(merchant_rating as decimal(3, 2)) as merchant_rating,
        coalesce(
            try_to_date(joined_date, 'M/d/yyyy'),
            try_to_date(joined_date)
        ) as joined_date,

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
            partition by merchant_id
            order by _ingested_at desc
        ) as _row_num

    from renamed

)

select
    merchant_id,
    merchant_name,
    merchant_category,
    city,
    country,
    merchant_rating,
    joined_date,
    _ingested_at,
    source_system,
    batch_id
from deduplicated
where _row_num = 1

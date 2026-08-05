-- Cleans and standardizes bronze.devices: removes duplicate device_id
-- records, keeping the most recently ingested version. Columns are
-- already clean text, so no type casting is required beyond dedup.

with source as (

    select * from {{ source('bronze', 'devices') }}

),

renamed as (

    select
        device_id,
        device_type,
        operating_system,

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
            partition by device_id
            order by _ingested_at desc
        ) as _row_num

    from renamed

)

select
    device_id,
    device_type,
    operating_system,
    _ingested_at,
    source_system,
    batch_id
from deduplicated
where _row_num = 1

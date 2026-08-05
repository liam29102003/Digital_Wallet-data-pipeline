-- Cleans and standardizes bronze.branches: casts created_at to a real
-- timestamp and removes duplicate branch_id records, keeping the most
-- recently ingested version of each. No joins, no business logic.

with source as (

    select * from {{ source('bronze', 'branches') }}

),

renamed as (

    select
        branch_id,
        branch_name,
        city,
        country,
        region,
        coalesce(
            try_to_timestamp(created_at, 'M/d/yyyy H:mm'),
            try_to_timestamp(created_at)
        ) as created_at,

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
            partition by branch_id
            order by _ingested_at desc
        ) as _row_num

    from renamed

)

select
    branch_id,
    branch_name,
    city,
    country,
    region,
    created_at,
    _ingested_at,
    source_system,
    batch_id
from deduplicated
where _row_num = 1

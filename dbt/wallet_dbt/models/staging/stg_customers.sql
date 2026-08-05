-- Cleans and standardizes bronze.customers: casts date/timestamp
-- columns, lowercases/trims email for consistent matching downstream,
-- and removes duplicate customer_id records. Because this table is
-- loaded incrementally, dedup ordering prefers the most recent business
-- update (updated_at) before falling back to ingestion recency.

with source as (

    select * from {{ source('bronze', 'customers') }}

),

renamed as (

    select
        customer_id,
        first_name,
        last_name,
        gender,
        coalesce(
            try_to_date(date_of_birth, 'M/d/yyyy'),
            try_to_date(date_of_birth)
        ) as date_of_birth,
        lower(trim(email)) as email,
        phone,
        occupation,
        income_level,
        risk_level,
        kyc_status,
        city,
        country,
        coalesce(
            try_to_date(registration_date, 'M/d/yyyy'),
            try_to_date(registration_date)
        ) as registration_date,
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
            partition by customer_id
            order by updated_at desc, _ingested_at desc
        ) as _row_num

    from renamed

)

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
    updated_at,
    _ingested_at,
    source_system,
    batch_id
from deduplicated
where _row_num = 1

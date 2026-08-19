select
    device_id as device_sk,
    device_id,
    device_type,
    operating_system
from {{ ref('stg_devices') }}
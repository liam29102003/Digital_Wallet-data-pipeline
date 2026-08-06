-- Device dimension, Type 1 (current state only) — devices are a small,
-- closed reference set with no meaningful history to preserve.
-- device_id is already a stable, unique natural key, so it doubles as
-- the surrogate key with no additional hashing needed.

select
    device_id as device_sk,
    device_id,
    device_type,
    operating_system
from {{ ref('stg_devices') }}
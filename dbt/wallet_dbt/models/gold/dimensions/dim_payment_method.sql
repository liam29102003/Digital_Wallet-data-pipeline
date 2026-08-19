
select
    payment_method_id as payment_method_sk,
    payment_method_id,
    payment_method,
    provider
from {{ ref('stg_payment_methods') }}
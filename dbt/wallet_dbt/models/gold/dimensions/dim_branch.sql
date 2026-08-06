-- Branch dimension, SCD Type 2, sourced from the branches snapshot
-- (check strategy). NOTE: currently has no foreign key path from
-- fact_transactions — see project-level data model gap notes.

select
    dbt_scd_id as branch_sk,
    branch_id,
    branch_name,
    city,
    country,
    region,
    dbt_valid_from,
    dbt_valid_to,
    dbt_valid_to is null as is_current
from {{ ref('branches_snapshot') }}
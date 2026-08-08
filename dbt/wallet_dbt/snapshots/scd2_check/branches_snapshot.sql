{% snapshot branches_snapshot %}
{{
    config(
        unique_key='branch_id',
        strategy='check',
        check_cols=['branch_name', 'city', 'country', 'region'],
        updated_at='created_at',
        invalidate_hard_deletes=True,
    )
}}
select branch_id, branch_name, city, country, region, created_at
from {{ ref('stg_branches') }}
{% endsnapshot %}
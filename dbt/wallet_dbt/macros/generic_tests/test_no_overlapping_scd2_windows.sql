

{% test no_overlapping_scd2_windows(model, column_name, natural_key) %}

with windows as (

    select
        {{ natural_key }} as natural_key,
        {{ column_name }} as dbt_valid_from,
        coalesce(dbt_valid_to, timestamp('9999-12-31')) as dbt_valid_to
    from {{ model }}

),


overlapping_pairs as (

    select
        a.natural_key,
        a.dbt_valid_from as version_a_from,
        a.dbt_valid_to   as version_a_to,
        b.dbt_valid_from as version_b_from,
        b.dbt_valid_to   as version_b_to
    from windows a
    inner join windows b
        on a.natural_key = b.natural_key
        and a.dbt_valid_from < b.dbt_valid_from
    where a.dbt_valid_to > b.dbt_valid_from

)

select * from overlapping_pairs

{% endtest %}
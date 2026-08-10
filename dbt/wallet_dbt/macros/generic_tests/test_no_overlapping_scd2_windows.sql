{#
  Generic test: no_overlapping_scd2_windows

  Fails if any two rows for the same natural key have validity windows
  [dbt_valid_from, dbt_valid_to) that overlap. Every downstream
  point-in-time join (fact_transactions' effective-date joins against
  wallet_history / customer_history / merchant_history) silently
  produces duplicate or wrong dimension matches if this invariant is
  ever violated — this is the class of bug the LEAST(created_at,
  dbt_valid_from) widening patch on the earliest version was fixing.
  This test guards the invariant directly so a future change to a
  snapshot's check_cols / updated_at config, or a manual backfill,
  can't quietly reintroduce it.

  Usage (in a .yml file, on any column of the snapshot — dbt_valid_from
  is the conventional place to attach it):

      - name: dbt_valid_from
        data_tests:
          - no_overlapping_scd2_windows:
              arguments:
                natural_key: branch_id

  A currently-open version has dbt_valid_to = null; that's treated as
  "infinitely far in the future" via coalesce, matching the pattern
  already used in fact_transactions.sql's *_history CTEs.
#}

{% test no_overlapping_scd2_windows(model, column_name, natural_key) %}

with windows as (

    select
        {{ natural_key }} as natural_key,
        {{ column_name }} as dbt_valid_from,
        coalesce(dbt_valid_to, timestamp('9999-12-31')) as dbt_valid_to
    from {{ model }}

),

-- Self-join every version against every later version (by valid_from)
-- of the SAME natural key. If the earlier version's window hasn't
-- closed before the later version's window opens, they overlap.
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
{#
  Generic test: at_most_one_current_version

  Fails if any natural key has MORE THAN ONE row with is_current = true.
  Zero current rows is valid (e.g. a hard-deleted entity where every
  version has dbt_valid_to set — invalidate_hard_deletes=True on all
  four snapshots means this can legitimately happen). Two or more is
  always a bug: it means either dbt_valid_to wasn't closed on the
  previous version when a new one was created, or a manual write /
  backfill broke the SCD2 chain.

  Currently every dim_*.yml only tests `is_current: not_null`, which
  can't catch this — a row can be non-null AND wrong at the same time.

  Usage (attach to the is_current column):

      - name: is_current
        data_tests:
          - not_null
          - at_most_one_current_version:
              arguments:
                natural_key: branch_id
#}

{% test at_most_one_current_version(model, column_name, natural_key) %}

select
    {{ natural_key }} as natural_key,
    count(*) as current_version_count
from {{ model }}
where {{ column_name }}
group by {{ natural_key }}
having count(*) > 1

{% endtest %}
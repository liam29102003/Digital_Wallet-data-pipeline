{#
  Maintenance macros, deliberately NOT run as part of `dbt run`.

  OPTIMIZE (with ZORDER) and VACUUM are compaction/cleanup operations,
  not transformation logic — bundling them into the build path (e.g.
  as a post-hook) would make every incremental fact_transactions
  build pay for a full table compaction, which defeats the purpose of
  the 3-day-lookback incremental strategy in the first place.

  Instead these are invoked via `dbt run-operation` from a separate,
  weekly Airflow DAG (wallet_gold_maintenance.py) — build cadence and
  maintenance cadence are two different concerns with two different
  cost profiles, and keeping them separate lets each run on its own
  schedule.
#}

{% macro optimize_gold_tables() %}
  {#
    ZORDER columns are chosen from actual downstream join/merge keys,
    not guessed:
      - fact_transactions: transaction_id is the merge key (ZORDER
        here speeds up merge's own file-skipping), customer_sk and
        merchant_sk are the join keys used by the two marts.
      - dim_* tables: ZORDER on the natural key, since that's what
        fact_transactions' point-in-time joins ultimately resolve on
        before hashing to the surrogate key.
  #}
  {% set tables = {
      'gold.fact_transactions': 'transaction_id, customer_sk, merchant_sk',
      'gold.dim_customers': 'customer_id',
      'gold.dim_wallet': 'wallet_id',
      'gold.dim_merchant': 'merchant_id',
  } %}

  {% for relative_table, zorder_cols in tables.items() %}
    {% set full_table = target.database ~ '.' ~ relative_table %}
    {% set query %}
      OPTIMIZE {{ full_table }} ZORDER BY ({{ zorder_cols }})
    {% endset %}
    {% do log('OPTIMIZE ' ~ full_table ~ ' ZORDER BY (' ~ zorder_cols ~ ')', info=True) %}
    {% do run_query(query) %}
  {% endfor %}
{% endmacro %}


{% macro vacuum_gold_tables(retention_hours=168) %}
  {#
    Only actual Delta TABLES can be vacuumed — staging models are
    views (see dbt_project.yml: +materialized: view for the silver
    layer), so this deliberately covers only snapshots + gold.

    retention_hours defaults to 168 (7 days), matching Delta's own
    default safety threshold (spark.databricks.delta.retentionDuration
    Check.enabled). Going lower requires explicitly disabling that
    check — not done here on purpose, since going below 7 days risks
    breaking Time Travel queries against in-flight snapshot reads.
  #}
  {% set schemas_tables = {
      'snapshots': ['customers_snapshot', 'wallet_accounts_snapshot', 'merchants_snapshot'],
      'gold': [
          'dim_customers', 'dim_wallet', 'dim_merchant', 'dim_device', 'dim_payment_method',
          'fact_transactions', 'mart_daily_transaction_summary', 'mart_customer_summary',
      ],
  } %}

  {% for schema, tables in schemas_tables.items() %}
    {% for table_name in tables %}
      {% set full_table = target.database ~ '.' ~ schema ~ '.' ~ table_name %}
      {% set query %}
        VACUUM {{ full_table }} RETAIN {{ retention_hours }} HOURS
      {% endset %}
      {% do log('VACUUM ' ~ full_table ~ ' (retain ' ~ retention_hours ~ 'h)', info=True) %}
      {% do run_query(query) %}
    {% endfor %}
  {% endfor %}
{% endmacro %}
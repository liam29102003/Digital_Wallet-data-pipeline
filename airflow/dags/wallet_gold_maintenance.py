from __future__ import annotations

import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/project/dbt/wallet_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profile"

default_args = {
    "owner": "wallet-data-platform",
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=10),
}


@dag(
    dag_id="wallet_gold_maintenance",
    description=(
        "Weekly Delta maintenance: OPTIMIZE + ZORDER on Gold/dim tables, "
        "VACUUM on Snapshot + Gold tables. Deliberately separate from the "
        "daily bronze->gold build DAG — compaction/cleanup has a different "
        "cost and failure profile than a build, and a failure here should "
        "never block or fail the ingestion pipeline."
    ),
    schedule="@weekly",
    start_date=datetime.datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["maintenance", "wallet-data-platform"],
)
def wallet_gold_maintenance():

    optimize = BashOperator(
        task_id="optimize_gold_tables",
        bash_command=(
            f"dbt run-operation optimize_gold_tables "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    vacuum = BashOperator(
        task_id="vacuum_gold_tables",
        bash_command=(
            f"dbt run-operation vacuum_gold_tables "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # OPTIMIZE before VACUUM: compaction can create new small files that
    # get merged, so running ZORDER first means VACUUM's stale-file
    # cleanup accounts for files OPTIMIZE just made obsolete, not the
    # other way around.
    optimize >> vacuum


wallet_gold_maintenance()
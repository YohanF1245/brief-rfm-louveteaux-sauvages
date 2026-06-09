"""
Ingestion Warcraft Logs — guilde Nightmares Asylum (Dalaran EU).

Flux lakehouse (sans Postgres) :
  API WCL → bronze Delta (MinIO) → silver/gold dbt (ClickHouse)

  1. ``sync_report_catalog`` — catalogue API → ``ingestion_state`` (Delta)
  2. ``ingest_reports_incremental`` — par report : fights + stats → bronze Delta
  3. ``dbt_silver`` / ``dbt_gold`` — ``gold.wcl_boss_dps``, ``gold.wcl_player_dps_viz``, ``gold.wcl_raid_consumables_viz``

Rafraîchir gold seul (sans API) : DAG ``warcraftlogs_lakehouse_dbt``.

Chemins bronze :
  - ``s3://lake/bronze/warcraftlogs/guild_reports``
  - ``s3://lake/bronze/warcraftlogs/fights``
  - ``s3://lake/bronze/warcraftlogs/fight_player_stats``
  - ``s3://lake/bronze/warcraftlogs/fight_tables_raw`` (JSON API complet par TableDataType)
  - ``s3://lake/bronze/warcraftlogs/reports_raw``
  - ``s3://lake/bronze/warcraftlogs/ingestion_state``

Airflow :
  - Connexion ``WCL_API`` (HTTP) : login = client_id, password = client_secret
  - Variables : ``wcl_guild_url``, ``wcl_user_ids``, ``wcl_ingest_batch_size``, etc.

MinIO / ClickHouse : réseau Docker + creds compose (pas de connexion Airflow).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable

from lakehouse_common import WCL_DBT_GOLD, WCL_DBT_SILVER, run_dbt, run_dbt_test
from warcraftlogs_ingest import ingest_reports_incremental, sync_report_catalog

_DEFAULT_SCHEDULE = "0 */6 * * *"


def _dag_schedule() -> str | None:
    try:
        raw = str(Variable.get("wcl_dag_schedule", default=_DEFAULT_SCHEDULE)).strip()
    except Exception:
        raw = _DEFAULT_SCHEDULE
    if not raw or raw.lower() in {"none", "null", "manual"}:
        return None
    return raw


with DAG(
    dag_id="warcraftlogs_guild_nightmares",
    start_date=datetime(2024, 1, 1),
    schedule=_dag_schedule(),
    catchup=False,
    max_active_runs=1,
    tags=["warcraftlogs", "ingest", "api", "raid", "lakehouse", "delta", "dbt", "bronze", "silver", "gold"],
    doc_md=__doc__,
) as dag:
    sync_catalog = PythonOperator(
        task_id="sync_report_catalog",
        python_callable=sync_report_catalog,
    )
    ingest_bronze = PythonOperator(
        task_id="ingest_reports_incremental",
        python_callable=ingest_reports_incremental,
    )
    dbt_silver = PythonOperator(
        task_id="dbt_silver",
        python_callable=run_dbt,
        op_kwargs={"select": WCL_DBT_SILVER},
    )
    dbt_gold = PythonOperator(
        task_id="dbt_gold",
        python_callable=run_dbt,
        op_kwargs={"select": WCL_DBT_GOLD},
    )
    dbt_test_gold = PythonOperator(
        task_id="dbt_test_gold",
        python_callable=run_dbt_test,
        op_kwargs={"select": WCL_DBT_GOLD},
    )

    sync_catalog >> ingest_bronze >> dbt_silver >> dbt_gold >> dbt_test_gold

"""
Transform WCL bronze → silver/gold (dbt + ClickHouse), sans appel API.

Utiliser ce DAG pour rafraîchir ClickHouse quand la bronze MinIO est déjà à jour.
L'ingestion API (300+ reports) reste dans ``warcraftlogs_guild_nightmares``.

Chaîne :
  1. ``dbt_silver`` — ``silver.wcl_*`` depuis Delta (``deltaLake()``)
  2. ``dbt_gold`` — ``gold.wcl_boss_dps``, ``gold.wcl_player_dps_viz``, ``gold.wcl_raid_consumables_viz``
  3. ``dbt_test_gold`` — tests dbt

Coups de gold seul : Trigger DAG puis lancer uniquement ``dbt_gold`` (silver déjà OK).

Planification optionnelle : variable Airflow ``wcl_dbt_schedule`` (ex. ``0 */6 * * *``).
Par défaut : manuel (``schedule=None``).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable

from lakehouse_common import WCL_DBT_GOLD, WCL_DBT_SILVER, run_dbt, run_dbt_test


def _dbt_schedule() -> str | None:
    try:
        raw = str(Variable.get("wcl_dbt_schedule", default="")).strip()
    except Exception:
        raw = ""
    if not raw or raw.lower() in {"none", "null", "manual"}:
        return None
    return raw


with DAG(
    dag_id="warcraftlogs_lakehouse_dbt",
    start_date=datetime(2024, 1, 1),
    schedule=_dbt_schedule(),
    catchup=False,
    max_active_runs=1,
    tags=["warcraftlogs", "lakehouse", "dbt", "silver", "gold", "clickhouse"],
    doc_md=__doc__,
) as dag:
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

    dbt_silver >> dbt_gold >> dbt_test_gold

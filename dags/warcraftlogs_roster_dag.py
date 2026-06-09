"""
Roster guilde Nightmares Asylum — sync journalier API → bronze Delta.

Flux :
  1. ``sync_guild_roster`` — ``guild.members`` WCL → ``s3://lake/bronze/warcraftlogs/guild_roster``
  2. ``dbt_silver_roster`` — ``silver.wcl_guild_roster`` + ``silver.wcl_fight_player_guids``

Puis lancer ``warcraftlogs_lakehouse_dbt`` (gold) ou attendre le schedule dbt pour
propager ``is_guild_member`` / ``player_guid`` dans les viz Streamlit.

Variable Airflow ``wcl_roster_schedule`` (défaut ``0 6 * * *``).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable

from lakehouse_common import run_dbt
from warcraftlogs_roster_ingest import sync_guild_roster

_DEFAULT_SCHEDULE = "0 6 * * *"
_ROSTER_DBT_SILVER = "wcl_guild_roster wcl_fight_player_guids"


def _roster_schedule() -> str | None:
    try:
        raw = str(Variable.get("wcl_roster_schedule", default=_DEFAULT_SCHEDULE)).strip()
    except Exception:
        raw = _DEFAULT_SCHEDULE
    if not raw or raw.lower() in {"none", "null", "manual"}:
        return None
    return raw


with DAG(
    dag_id="warcraftlogs_guild_roster",
    start_date=datetime(2024, 1, 1),
    schedule=_roster_schedule(),
    catchup=False,
    max_active_runs=1,
    tags=["warcraftlogs", "ingest", "api", "roster", "lakehouse", "delta", "bronze"],
    doc_md=__doc__,
) as dag:
    sync_roster = PythonOperator(
        task_id="sync_guild_roster",
        python_callable=sync_guild_roster,
    )
    dbt_silver_roster = PythonOperator(
        task_id="dbt_silver_roster",
        python_callable=run_dbt,
        op_kwargs={"select": _ROSTER_DBT_SILVER},
    )

    sync_roster >> dbt_silver_roster

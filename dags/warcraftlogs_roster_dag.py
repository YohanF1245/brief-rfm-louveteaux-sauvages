"""
Roster guilde Nightmares Asylum — sync journalier API → bronze Delta.

``sync_guild_roster`` : ``guild.members`` WCL → ``s3://lake/bronze/warcraftlogs/guild_roster``
puis publie l'asset ``wcl_bronze_roster`` → déclenche ``warcraftlogs_silver``
(qui reconstruit ``silver.wcl_guild_roster`` et propage ``is_guild_member``
jusqu'au gold via ``warcraftlogs_gold``). Plus de task dbt ici.

Variable Airflow ``wcl_roster_schedule`` (défaut ``0 6 * * *``).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable

from warcraftlogs_assets import WCL_ROSTER
from warcraftlogs_roster_ingest import sync_guild_roster

_DEFAULT_SCHEDULE = "0 6 * * *"


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
        outlets=[WCL_ROSTER],
    )

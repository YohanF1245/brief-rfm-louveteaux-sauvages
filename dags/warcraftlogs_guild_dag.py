"""
Ingestion Warcraft Logs — guilde Nightmares Asylum (Dalaran EU).

Pipeline incrémental bronze :
  1. Catalogue API (léger) → Postgres
  2. Par report : fights + stats (tous combats) → Postgres + Delta MinIO immédiat

Chemins bronze :
  - ``s3://lake/bronze/warcraftlogs/guild_reports``
  - ``s3://lake/bronze/warcraftlogs/fights``
  - ``s3://lake/bronze/warcraftlogs/fight_player_stats``
  - ``s3://lake/bronze/warcraftlogs/reports_raw``

Secrets : ``WCL_CLIENT_ID``, ``WCL_CLIENT_SECRET``.
Optionnel : ``WCL_GUILD_URL``, ``WCL_USER_IDS``, ``WCL_API_SLEEP_SECONDS``,
``WCL_MAX_REPORT_PAGES``, ``WCL_INGEST_BATCH_SIZE``, ``WCL_DAG_SCHEDULE``.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from warcraftlogs_ingest import ingest_reports_incremental, sync_report_catalog

_DEFAULT_SCHEDULE = "0 */6 * * *"


def _dag_schedule() -> str | None:
    raw = os.environ.get("WCL_DAG_SCHEDULE", _DEFAULT_SCHEDULE).strip()
    if not raw or raw.lower() in {"none", "null", "manual"}:
        return None
    return raw


with DAG(
    dag_id="warcraftlogs_guild_nightmares",
    start_date=datetime(2024, 1, 1),
    schedule=_dag_schedule(),
    catchup=False,
    max_active_runs=1,
    tags=["warcraftlogs", "ingest", "api", "raid", "lakehouse", "delta", "bronze"],
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

    sync_catalog >> ingest_bronze

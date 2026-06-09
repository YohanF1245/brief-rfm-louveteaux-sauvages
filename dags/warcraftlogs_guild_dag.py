"""
Ingestion Warcraft Logs — guilde Nightmares Asylum (Dalaran EU).

Flux API → bronze Delta (MinIO) uniquement.
Transform bronze → ClickHouse : DAG séparé ``warcraftlogs_lakehouse_dbt``.

  1. ``sync_report_catalog`` — catalogue API → ``ingestion_state`` (Delta)
  2. ``ingest_reports_incremental`` — par report : fights + stats → bronze Delta
     (``fight_player_stats`` flush par fight ; ``fight_tables_raw`` idem)

Déclencher dbt manuellement après ingest (ex. 3 reports de test) :
  DAG ``warcraftlogs_lakehouse_dbt`` → Trigger → ``dbt_silver`` / ``dbt_gold``.

Roster guilde (``player_guid`` pour jointure logs) : DAG ``warcraftlogs_guild_roster`` (quotidien).

Chemins bronze :
  - ``s3://lake/bronze/warcraftlogs/guild_reports``
  - ``s3://lake/bronze/warcraftlogs/fights``
  - ``s3://lake/bronze/warcraftlogs/fight_player_stats``
  - ``s3://lake/bronze/warcraftlogs/fight_tables_raw`` (JSON API complet par TableDataType)
  - ``s3://lake/bronze/warcraftlogs/reports_raw``
  - ``s3://lake/bronze/warcraftlogs/ingestion_state``

Airflow :
  - Connexion ``WCL_API`` (HTTP) : login = client_id, password = client_secret
  - Variables ingestion (prod) :
    - ``wcl_dag_schedule`` : ``*/30 * * * *`` (défaut)
    - ``wcl_ingest_batch_size`` : ``25`` (max candidats/run, quota réel via API)
    - ``wcl_adaptive_rate_limit`` : ``true``
    - ``wcl_api_sleep_seconds`` : ``0.2``
    - ``wcl_points_per_report_estimate`` : ``1000`` (fallback si pas encore de mesure)
    - ``wcl_quota_reserve_fraction`` : ``0.05``

MinIO / ClickHouse : réseau Docker + creds compose (pas de connexion Airflow).

Docker (``EXTRA_CELERY_CONFIG``) : ``worker_max_tasks_per_child=1`` recycle le worker
après chaque task Airflow pour libérer la RAM (évite OOM sur VPS 8 Go).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Variable

from warcraftlogs_ingest import ingest_reports_incremental, sync_report_catalog

_DEFAULT_SCHEDULE = "*/30 * * * *"


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

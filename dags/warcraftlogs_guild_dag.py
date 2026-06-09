"""
Ingestion Warcraft Logs — guilde Nightmares Asylum (Dalaran EU).

Flux API → bronze Delta (MinIO), puis déclenchement optionnel de dbt → ClickHouse.

  1. ``sync_report_catalog`` — catalogue API → ``ingestion_state`` (Delta)
  2. ``ingest_reports_incremental`` — par report : fights + stats → bronze Delta
  3. ``trigger_lakehouse_dbt`` — si ≥1 report ingéré et ``wcl_dbt_after_ingest=true``

Transform bronze → ClickHouse : DAG ``warcraftlogs_lakehouse_dbt`` (auto ou manuel).

Roster guilde (``player_guid`` pour jointure logs) : DAG ``warcraftlogs_guild_roster`` (quotidien).

Ré-ingest total (ex. buffs ``viewBy: Source``) :
  1. ``python scripts/reset_wcl_ingestion.py`` (ou task équivalente) → repasse ``ok`` en ``pending``
  2. Relancer ``warcraftlogs_guild_nightmares`` en boucle jusqu'à plus de pending (quota ~25/run)
  3. ``warcraftlogs_lakehouse_dbt`` pour silver + gold

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
    - ``wcl_dbt_after_ingest`` : ``false`` (défaut) — ``true`` déclenche lakehouse_dbt après chaque batch ingest

MinIO / ClickHouse : réseau Docker + creds compose (pas de connexion Airflow).

Docker (``EXTRA_CELERY_CONFIG``) : ``worker_max_tasks_per_child=1`` recycle le worker
après chaque task Airflow pour libérer la RAM (évite OOM sur VPS 8 Go).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable

from warcraftlogs_ingest import ingest_reports_incremental, sync_report_catalog, wcl_dbt_after_ingest_enabled

_DEFAULT_SCHEDULE = "*/30 * * * *"


def _dag_schedule() -> str | None:
    try:
        raw = str(Variable.get("wcl_dag_schedule", default=_DEFAULT_SCHEDULE)).strip()
    except Exception:
        raw = _DEFAULT_SCHEDULE
    if not raw or raw.lower() in {"none", "null", "manual"}:
        return None
    return raw


def _should_trigger_dbt_after_ingest(**context) -> bool:
    processed = int(context["ti"].xcom_pull(task_ids="ingest_reports_incremental") or 0)
    if processed <= 0:
        print("Post-ingest dbt : skip (aucun report ingéré ce run).")
        return False
    if not wcl_dbt_after_ingest_enabled():
        print("Post-ingest dbt : skip (variable wcl_dbt_after_ingest désactivée).")
        return False
    print(f"Post-ingest dbt : {processed} report(s) ingéré(s) → trigger lakehouse_dbt.")
    return True


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
    check_dbt = ShortCircuitOperator(
        task_id="check_dbt_after_ingest",
        python_callable=_should_trigger_dbt_after_ingest,
    )
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_lakehouse_dbt",
        trigger_dag_id="warcraftlogs_lakehouse_dbt",
        wait_for_completion=False,
        reset_dag_run=False,
    )

    sync_catalog >> ingest_bronze >> check_dbt >> trigger_dbt

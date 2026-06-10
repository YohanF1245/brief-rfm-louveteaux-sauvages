"""
Ingestion Warcraft Logs — guilde Nightmares Asylum (Dalaran EU).

Flux API → bronze Delta (MinIO), **données BRUTES uniquement** (pas de tables
agrégées WCL). Schéma complet documenté dans ``docs/wcl_bronze.md``.

  1. ``sync_report_catalog`` — catalogue API → ``ingestion_state`` (Delta)
  2. ``ingest_reports_incremental`` — par report :
     a. ``reports_raw`` + ``fights``           (métadonnées, 1 appel API)
     b. ``master_info/actors/abilities``       (masterData, 1 appel)
     c. ``player_details``                     (specs/ilvl/talents/gear, 1 appel)
     d. ``events``                             (log brut ``dataType: All``,
        paginé 10 000/page sur toute la plage du report, flush par page)

Tout est recalculable depuis ``events`` × ``master_actors`` (joueurs, pets via
``pet_owner_id``, NPC) × ``master_abilities`` : DPS, soins, buffs, consommables,
morts, interrupts, … → plus jamais de ré-ingestion pour un nouveau besoin.

Après ingest : lancer **une fois** ``warcraftlogs_lakehouse_dbt``.
Roster guilde (``player_guid``) : DAG ``warcraftlogs_guild_roster`` (quotidien).

Ré-ingest forcé d'un report : ``scripts/reset_wcl_ingestion.py`` (status → pending),
le bronze du report est remplacé de façon idempotente (delete + append).

Chemins bronze :
  - ``s3://lake/bronze/warcraftlogs/guild_reports``    (catalogue)
  - ``s3://lake/bronze/warcraftlogs/fights``           (1 ligne / fight)
  - ``s3://lake/bronze/warcraftlogs/reports_raw``      (JSON report brut)
  - ``s3://lake/bronze/warcraftlogs/master_info``      (versions log/jeu)
  - ``s3://lake/bronze/warcraftlogs/master_actors``    (id → joueur/pet/NPC)
  - ``s3://lake/bronze/warcraftlogs/master_abilities`` (id → sort)
  - ``s3://lake/bronze/warcraftlogs/player_details``   (specs, gear, talents)
  - ``s3://lake/bronze/warcraftlogs/events``           (log brut, 1 ligne / event)
  - ``s3://lake/bronze/warcraftlogs/ingestion_state``  (suivi ingestion)

Airflow :
  - Connexion ``WCL_API`` (HTTP) : login = client_id, password = client_secret
  - Variables : ``wcl_dag_schedule``, ``wcl_ingest_batch_size``,
    ``wcl_adaptive_rate_limit``, ``wcl_events_page_size``,
    ``wcl_events_include_resources``, etc.

MinIO / ClickHouse : réseau Docker + creds compose (pas de connexion Airflow).

Docker : ``worker_max_tasks_per_child=1`` recycle le worker après chaque task (RAM VPS 8 Go).
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

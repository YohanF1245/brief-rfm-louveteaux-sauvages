"""
Silver WCL — dbt bronze Delta → ClickHouse ``silver.wcl_*``.

Déclenchement par ASSETS (pas de cron) : ce DAG tourne dès que
``warcraftlogs_guild_nightmares`` (asset ``wcl_bronze``) OU
``warcraftlogs_guild_roster`` (asset ``wcl_bronze_roster``) a réussi.
En sortie il publie l'asset ``wcl_silver`` qui déclenche ``warcraftlogs_gold``.

Modèles (cf. ``docs/wcl_silver_gold.md``) :
  - ``wcl_reports`` / ``wcl_fights`` / ``wcl_ingestion_state``  (métadonnées)
  - ``wcl_actors``        — dim acteurs, pets résolus → ``resolved_player_name``
  - ``wcl_abilities``     — dim sorts
  - ``wcl_events``        — fait central typé (JSON extrait, MergeTree)
  - ``wcl_pull_combatants`` / ``wcl_pull_auras`` — snapshot au pull (spé, ilvl, buffs)
  - ``wcl_guild_roster`` / ``wcl_player_details`` — roster + flag membre guilde

Trigger manuel possible à tout moment (rebuild silver sans toucher au bronze).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from lakehouse_common import WCL_DBT_SILVER, run_dbt
from warcraftlogs_assets import WCL_BRONZE, WCL_ROSTER, WCL_SILVER

with DAG(
    dag_id="warcraftlogs_silver",
    start_date=datetime(2024, 1, 1),
    schedule=(WCL_BRONZE | WCL_ROSTER),
    catchup=False,
    max_active_runs=1,
    tags=["warcraftlogs", "lakehouse", "dbt", "silver", "clickhouse", "asset"],
    doc_md=__doc__,
) as dag:
    dbt_silver = PythonOperator(
        task_id="dbt_silver",
        python_callable=run_dbt,
        op_kwargs={"select": WCL_DBT_SILVER},
        outlets=[WCL_SILVER],
    )

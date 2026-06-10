"""
Gold WCL — dbt silver → ClickHouse ``gold.wcl_*_viz`` (KPI guilde).

Déclenchement par ASSET : tourne dès que ``warcraftlogs_silver`` a réussi
(asset ``wcl_silver``). Publie ``wcl_gold`` (consommable par BI/exports).

Tables KPI (cf. ``docs/wcl_silver_gold.md``) :
  - ``wcl_fight_player_perf_viz`` — joueur × fight : DPS/HPS/DTPS, morts,
    interrupts, dispels, potions, spé/ilvl au pull, flag membre guilde
  - ``wcl_damage_taken_viz``      — DTPS détaillé par source NPC × sort
  - ``wcl_consumables_viz``       — flask/food/rune/huile : uptime exact par
    fight + potions/healthstones (casts)
  - ``wcl_deaths_viz``            — chaque mort : ordre, % du fight, sort fatal
  - ``wcl_raid_nights_viz``       — synthèse soirée : pulls, kills, wipes, durée
  - ``wcl_attendance_viz``        — présence des membres du roster par soirée

Puis ``dbt_test_gold`` (tests schema.yml).
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from lakehouse_common import WCL_DBT_GOLD, run_dbt, run_dbt_test
from warcraftlogs_assets import WCL_GOLD, WCL_SILVER

with DAG(
    dag_id="warcraftlogs_gold",
    start_date=datetime(2024, 1, 1),
    schedule=WCL_SILVER,
    catchup=False,
    max_active_runs=1,
    tags=["warcraftlogs", "lakehouse", "dbt", "gold", "clickhouse", "asset", "kpi"],
    doc_md=__doc__,
) as dag:
    dbt_gold = PythonOperator(
        task_id="dbt_gold",
        python_callable=run_dbt,
        op_kwargs={"select": WCL_DBT_GOLD},
        outlets=[WCL_GOLD],
    )
    dbt_test_gold = PythonOperator(
        task_id="dbt_test_gold",
        python_callable=run_dbt_test,
        op_kwargs={"select": WCL_DBT_GOLD},
    )

    dbt_gold >> dbt_test_gold

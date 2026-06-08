"""
DAG de test lakehouse local : bronze Delta (MinIO) → silver/gold (dbt + ClickHouse).

Chaîne :
1. Ingestion bronze Delta sur ``s3://lake/bronze/stack_test/ventes``
2. dbt silver : lecture Delta via ``deltaLake()`` ClickHouse
3. dbt gold : agrégats journaliers (table plate pour Power BI)
4. Tests dbt + validation ligne gold

Prérequis compose : minio, minio-init, clickhouse, workers Airflow avec dbt + deltalake.
Power BI : connecteur ClickHouse → hôte ``localhost`` port ``8123`` (dev) base ``gold``,
table ``stack_test_daily_kpis``.
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from lakehouse_common import (
    ingest_bronze_delta,
    run_dbt,
    run_dbt_test,
    validate_gold_power_bi,
)

with DAG(
    dag_id="lakehouse_stack_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "test", "delta", "dbt", "clickhouse"],
    doc_md=__doc__,
) as dag:
    bronze = PythonOperator(
        task_id="ingest_bronze_delta",
        python_callable=ingest_bronze_delta,
    )
    silver = PythonOperator(
        task_id="dbt_silver",
        python_callable=run_dbt,
        op_kwargs={"select": "tag:stack_test,tag:silver"},
    )
    gold = PythonOperator(
        task_id="dbt_gold",
        python_callable=run_dbt,
        op_kwargs={"select": "tag:stack_test,tag:gold"},
    )
    tests = PythonOperator(
        task_id="dbt_test_gold",
        python_callable=run_dbt_test,
        op_kwargs={"select": "tag:stack_test,tag:gold"},
    )
    validate = PythonOperator(
        task_id="validate_gold_power_bi",
        python_callable=validate_gold_power_bi,
    )

    bronze >> silver >> gold >> tests >> validate

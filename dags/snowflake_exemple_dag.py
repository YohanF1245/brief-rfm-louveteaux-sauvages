"""Exemple Snowflake : connexion Airflow ``snowflake`` + SQLExecuteQueryOperator."""

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="snowflake_exemple",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exemple", "snowflake"],
) as dag:
    SQLExecuteQueryOperator(
        task_id="init_et_insert",
        conn_id="snowflake",
        sql=[
            "CREATE DATABASE IF NOT EXISTS RFM_DEMO",
            "CREATE SCHEMA IF NOT EXISTS RFM_DEMO.EXEMPLE",
            """CREATE TABLE IF NOT EXISTS RFM_DEMO.EXEMPLE.DEMO_LIGNE (
                id INT PRIMARY KEY, message VARCHAR)""",
            """MERGE INTO RFM_DEMO.EXEMPLE.DEMO_LIGNE t
               USING (SELECT 1 id, 'hello airflow' message) s ON t.id = s.id
               WHEN NOT MATCHED THEN INSERT (id, message) VALUES (s.id, s.message)""",
        ],
        split_statements=True,
        autocommit=True,
    )

"""NYC yellow taxi 2026-01 (parquet TLC) → Snowflake, connexion ``snowflake``."""

from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.python import PythonOperator
from snowflake.connector.pandas_tools import write_pandas

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
DB, SCHEMA, TABLE = "taxi_airflow", "NYC_TLC", "YELLOW_TRIPDATA_2026_01"


def ingest_parquet() -> None:
    hook = SnowflakeHook(snowflake_conn_id="snowflake")
    conn = hook.get_conn()
    try:
        hook.run(f'CREATE DATABASE IF NOT EXISTS "{DB}"', autocommit=True)
        hook.run(f'CREATE SCHEMA IF NOT EXISTS "{DB}"."{SCHEMA}"', autocommit=True)
        df = pd.read_parquet(URL)
        ok, nchunks, nrows, _ = write_pandas(
            conn,
            df,
            TABLE,
            database=DB,
            schema=SCHEMA,
            auto_create_table=True,
            overwrite=True,
            chunk_size=50_000,
        )
        print(f"write_pandas ok={ok} chunks={nchunks} rows={nrows}")
    finally:
        conn.close()


with DAG(
    dag_id="snowflake_yellow_taxi",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exemple", "snowflake", "nyc-tlc"],
) as dag:
    PythonOperator(task_id="ingest_parquet", python_callable=ingest_parquet)

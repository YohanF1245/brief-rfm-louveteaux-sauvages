import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator

from meteo_nord_common import (
    CONN_ID,
    FULL_TABLE,
    SCHEMA,
    TABLE,
    read_rr_t_vent_gz,
    transform_dataframe,
)

RAW_PATH_1 = Path(__file__).resolve().parent / "data/raw_climat_data/50-24.gz"
RAW_PATH_2 = Path(__file__).resolve().parent / "data/raw_climat_data/25-26.gz"
STAGING_DIR = Path(os.environ.get("METEO_NORD_STAGING_DIR", "/tmp/meteo_nord_staging"))
STAGING_RAW = STAGING_DIR / "climat_raw.pkl"
STAGING_CURATED = STAGING_DIR / "climat_curated.pkl"

DDL_CLIMAT_DATA = f"""
DROP TABLE IF EXISTS {FULL_TABLE} CASCADE;

CREATE TABLE {FULL_TABLE} (
    station_id BIGINT NOT NULL,
    station_nom TEXT,
    date_mesure TIMESTAMP NOT NULL,
    quantite_precipitations DOUBLE PRECISION,
    qualite_quantite_precipitations BIGINT,
    temp_min DOUBLE PRECISION,
    qualite_temp_min BIGINT,
    temp_max DOUBLE PRECISION,
    qualite_temp_max BIGINT,
    temp_min_at TIMESTAMP,
    temp_max_at TIMESTAMP,
    duree_gel BIGINT,
    qualite_duree_gel BIGINT,
    altitude BIGINT,
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    qualite_heure_temp_min BIGINT,
    qualite_heure_temp_max BIGINT,
    PRIMARY KEY (station_id, date_mesure)
);
"""


def extract(**context) -> str:
    """Lit les deux fichiers RR-T-Vent et les fusionne (staging pickle, évite XCom volumineux)."""
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.concat(
        [read_rr_t_vent_gz(RAW_PATH_1), read_rr_t_vent_gz(RAW_PATH_2)],
        ignore_index=True,
    )
    df.to_pickle(STAGING_RAW)
    return str(STAGING_RAW)


def transform(**context) -> str:
    raw_path = context["ti"].xcom_pull(task_ids="extract")
    df = transform_dataframe(pd.read_pickle(raw_path))
    df.to_pickle(STAGING_CURATED)
    return str(STAGING_CURATED)


def _ensure_climat_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_CLIMAT_DATA)
    conn.commit()


def load(**context) -> None:
    """DDL avec PK (station_id, date_mesure) puis chargement."""
    curated_path = context["ti"].xcom_pull(task_ids="transform")
    df = pd.read_pickle(curated_path)
    df = df.drop_duplicates(subset=["station_id", "date_mesure"], keep="last")

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    try:
        _ensure_climat_table(conn)
    finally:
        conn.close()

    engine = hook.get_sqlalchemy_engine()
    df.to_sql(
        TABLE,
        con=engine,
        schema=SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )
    engine.dispose()


with DAG(
    dag_id="meteo_nord",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["meteo", "postgres"],
) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task

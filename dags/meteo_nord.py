import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator

RAW_PATH_1 = Path(__file__).resolve().parent / "data/raw_climat_data/50-24.gz"
RAW_PATH_2 = Path(__file__).resolve().parent / "data/raw_climat_data/25-26.gz"
# /opt/airflow/dags est monté en lecture seule pour l'utilisateur airflow → staging dans /tmp
STAGING_DIR = Path(os.environ.get("METEO_NORD_STAGING_DIR", "/tmp/meteo_nord_staging"))
STAGING_RAW = STAGING_DIR / "climat_raw.pkl"
STAGING_CURATED = STAGING_DIR / "climat_curated.pkl"

CONN_ID = "DATA-DB"
TABLE = "climat_data"
SCHEMA = "public"
FULL_TABLE = f"{SCHEMA}.{TABLE}"

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

COLS_TO_DROP = [
    "HXI2",
    "QHXI2",
    "FF2M",
    "QFF2M",
    "FXI2",
    "QFXI2",
    "DXI2",
    "QDXI2",
    "DRR",
    "QTN50",
    "TN50",
    "QDRR",
    "TNSOL",
    "QTNSOL",
    "STATUS_DXI3S",
    "QDXI3S",
    "DXI3S",
    "FXY",
    "DXI",
    "DXY",
    "HXY",
    "HXI",
    "QDXY",
    "QFXY",
    "QHXY",
    "HXI3S",
    "QDXI",
    "QHXI",
    "QHXI3S",
    "FXI3S",
    "QFXI3S",
    "STATUS_FXI3S",
    "FXI",
    "QFXI",
    "FFM",
    "QFFM",
    "TM",
    "QTM",
    "TNTXM",
    "QTNTXM",
    "QTAMPLI",
    "TAMPLI",
]


def extract(**context) -> str:
    """Lit les deux fichiers RR-T-Vent et les fusionne (staging pickle, évite XCom volumineux)."""
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    df1 = pd.read_csv(RAW_PATH_1, sep=";", compression="gzip", low_memory=False)
    df2 = pd.read_csv(RAW_PATH_2, sep=";", compression="gzip", low_memory=False)
    df = pd.concat([df1, df2], ignore_index=True)
    df.to_pickle(STAGING_RAW)
    return str(STAGING_RAW)


def transform(**context) -> str:
    """Curated : colonnes, types, date_mesure, temp_min_at / temp_max_at."""
    raw_path = context["ti"].xcom_pull(task_ids="extract")
    df = pd.read_pickle(raw_path)

    df.drop(columns=COLS_TO_DROP, inplace=True, errors="ignore")

    df.rename(
        columns={
            "DG": "duree_gel",
            "QDG": "qualite_duree_gel",
            "HTX": "heure_temp_max",
            "QHTX": "qualite_heure_temp_max",
            "HTN": "heure_temp_min",
            "QHTN": "qualite_heure_temp_min",
            "NUM_POSTE": "station_id",
            "NOM_USUEL": "station_nom",
            "QRR": "qualite_quantite_precipitations",
            "RR": "quantite_precipitations",
            "AAAAMMJJ": "date_mesure",
            "ALTI": "altitude",
            "LON": "longitude",
            "LAT": "latitude",
            "TN": "temp_min",
            "QTN": "qualite_temp_min",
            "TX": "temp_max",
            "QTX": "qualite_temp_max",
        },
        inplace=True,
    )

    df = df.astype(
        {
            "duree_gel": "Int64",
            "qualite_duree_gel": "Int64",
            "qualite_heure_temp_max": "Int64",
            "qualite_heure_temp_min": "Int64",
            "station_id": "Int64",
            "station_nom": "string",
            "qualite_quantite_precipitations": "Int64",
            "quantite_precipitations": "Float64",
            "altitude": "Int64",
            "longitude": "Float64",
            "latitude": "Float64",
            "temp_min": "Float64",
            "qualite_temp_min": "Int64",
            "temp_max": "Float64",
            "qualite_temp_max": "Int64",
        }
    )

    df["date_mesure"] = pd.to_datetime(df["date_mesure"].astype(str), format="%Y%m%d")

    df["temp_min_at"] = pd.to_datetime(
        df["date_mesure"].dt.strftime("%Y%m%d")
        + df["heure_temp_min"].astype("Int64").astype(str).str.zfill(4),
        format="%Y%m%d%H%M",
        errors="coerce",
    )
    df["temp_max_at"] = pd.to_datetime(
        df["date_mesure"].dt.strftime("%Y%m%d")
        + df["heure_temp_max"].astype("Int64").astype(str).str.zfill(4),
        format="%Y%m%d%H%M",
        errors="coerce",
    )

    df.drop(columns=["heure_temp_min", "heure_temp_max"], inplace=True)

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

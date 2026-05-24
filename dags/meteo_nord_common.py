"""Logique partagée entre meteo_nord (chargement initial) et meteo_nord_quotidien."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

CONN_ID = "DATA-DB"
TABLE = "climat_data"
SCHEMA = "public"
FULL_TABLE = f"{SCHEMA}.{TABLE}"

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

DDL_CLIMAT_DATA = f"""
CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
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

UPSERT_COLUMNS = [
    "station_id",
    "station_nom",
    "date_mesure",
    "quantite_precipitations",
    "qualite_quantite_precipitations",
    "temp_min",
    "qualite_temp_min",
    "temp_max",
    "qualite_temp_max",
    "temp_min_at",
    "temp_max_at",
    "duree_gel",
    "qualite_duree_gel",
    "altitude",
    "longitude",
    "latitude",
    "qualite_heure_temp_min",
    "qualite_heure_temp_max",
]


def transform_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Curated : colonnes, types, date_mesure, temp_min_at / temp_max_at."""
    df = df.copy()
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
    return df


def ensure_climat_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL_CLIMAT_DATA)
    conn.commit()


def upsert_climat_data(df: pd.DataFrame, conn_id: str = CONN_ID) -> int:
    """Upsert sur (station_id, date_mesure). Retourne le nombre de lignes traitées."""
    df = df.drop_duplicates(subset=["station_id", "date_mesure"], keep="last")
    if df.empty:
        return 0

    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    try:
        ensure_climat_table(conn)
        cols_sql = ", ".join(UPSERT_COLUMNS)
        updates = ", ".join(
            f"{col} = EXCLUDED.{col}" for col in UPSERT_COLUMNS if col not in ("station_id", "date_mesure")
        )
        sql = f"""
            INSERT INTO {FULL_TABLE} ({cols_sql})
            VALUES %s
            ON CONFLICT (station_id, date_mesure) DO UPDATE SET {updates}
        """
        records = [
            tuple(None if pd.isna(row[col]) else row[col] for col in UPSERT_COLUMNS)
            for _, row in df[UPSERT_COLUMNS].iterrows()
        ]
        with conn.cursor() as cur:
            from psycopg2.extras import execute_values

            execute_values(cur, sql, records, page_size=5000)
        conn.commit()
        return len(records)
    finally:
        conn.close()


def read_rr_t_vent_gz(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";", compression="gzip", low_memory=False)

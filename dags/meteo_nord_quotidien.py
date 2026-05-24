"""
Ingestion quotidienne du fichier RR-T-Vent « latest 2025-2026 » (département 59).

Télécharge la ressource data.gouv (Q_59_latest-2025-2026) et upsert dans public.climat_data.
Idempotent via PRIMARY KEY (station_id, date_mesure).
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from meteo_nord_common import (
    read_rr_t_vent_gz,
    transform_dataframe,
    upsert_climat_data,
)

# Ressource : QUOT_departement_59_periode_2025-2026_RR-T-Vent
RESOURCE_ID = "e356abd8-90ff-4c7f-a0cd-55661c3be402"
API_V2_RESOURCE = f"https://www.data.gouv.fr/api/2/datasets/resources/{RESOURCE_ID}/"
FALLBACK_GZ_URL = (
    "https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/BASE/QUOT/"
    "Q_59_latest-2025-2026_RR-T-Vent.csv.gz"
)

STAGING_DIR = Path(os.environ.get("METEO_NORD_STAGING_DIR", "/tmp/meteo_nord_staging"))
STAGING_GZ = STAGING_DIR / "Q_59_latest-2025-2026.csv.gz"
STAGING_CURATED = STAGING_DIR / "quotidien_curated.pkl"


def _resolve_download_url() -> str:
    """URL du fichier .csv.gz via l'API v2 data.gouv."""
    headers = {"Accept": "application/json", "User-Agent": "brief-rfm-meteo-nord/1.0"}
    try:
        resp = requests.get(API_V2_RESOURCE, headers=headers, timeout=60)
        resp.raise_for_status()
        url = resp.json().get("resource", {}).get("url")
        if url:
            return url
    except requests.RequestException as exc:
        print(f"API v2 indisponible ({exc}), URL de repli statique.")

    return FALLBACK_GZ_URL


def download_latest(**context) -> str:
    """Télécharge le dernier Q_59 latest 2025-2026 RR-T-Vent."""
    url = _resolve_download_url()
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Téléchargement : {url}")
    with requests.get(url, stream=True, timeout=300, headers={"User-Agent": "brief-rfm-meteo-nord/1.0"}) as resp:
        resp.raise_for_status()
        with STAGING_GZ.open("wb") as handle:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
    print(f"Fichier local : {STAGING_GZ} ({STAGING_GZ.stat().st_size} octets)")
    return str(STAGING_GZ)


def transform(**context) -> str:
    """Transforme le gzip téléchargé (même logique que meteo_nord)."""
    gz_path = context["ti"].xcom_pull(task_ids="download_latest")
    df = read_rr_t_vent_gz(Path(gz_path))
    curated = transform_dataframe(df)
    curated.to_pickle(STAGING_CURATED)
    print(f"Lignes curated : {len(curated)}")
    return str(STAGING_CURATED)


def load(**context) -> None:
    """Upsert dans climat_data (sans DROP TABLE)."""
    curated_path = context["ti"].xcom_pull(task_ids="transform")
    df = pd.read_pickle(curated_path)
    n = upsert_climat_data(df)
    print(f"Upsert terminé : {n} lignes.")


with DAG(
    dag_id="meteo_nord_quotidien",
    start_date=datetime(2024, 1, 1),
    schedule="0 7 * * *",
    catchup=False,
    tags=["meteo", "postgres", "quotidien"],
    doc_md="""
    ### Ingestion quotidienne climat Nord (59)

    - **Planification** : tous les jours à 07:00 (fuseau du scheduler Airflow).
    - **Source** : [ressource data.gouv](https://www.data.gouv.fr/api/1/datasets/r/e356abd8-90ff-4c7f-a0cd-55661c3be402)
      → `Q_59_latest-2025-2026_RR-T-Vent.csv.gz`
    - **Cible** : `public.climat_data` (upsert sur `station_id`, `date_mesure`).

    Exécuter `gold_meteo_nord` une fois après le premier chargement pour créer les vues.
    """,
) as dag:
    download_task = PythonOperator(
        task_id="download_latest",
        python_callable=download_latest,
    )
    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )
    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    download_task >> transform_task >> load_task

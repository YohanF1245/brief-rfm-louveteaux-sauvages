"""
DAG scraping Pokémon Pokopia (Serebii) + CSV objets, puis **insert PostgreSQL**
(schéma ``pokopia``) et export CSV optionnel dans ``dags/data/pokemon_pokopia/staging/``.

**Scraping** : **à but scolaire uniquement** (démonstration / cours). Ne pas
industrialiser ni surcharger le site source ; hors cadre pédagogique, se conformer
aux règles du site et aux obligations légales applicables.

Montures Docker : ``./dags`` -> ``/opt/airflow/dags``. CSV : ``dags/data/pokemon_pokopia/items.csv``.

Variable ``POKOPIA_SCRAPE_LIMIT`` : nombre max d’espèces à scraper (**0** = toute la liste).
Défaut **0**. Variable ``POKOPIA_WRITE_STAGING`` (``true`` / ``false``, défaut ``false``).
Variable ``POKOPIA_ITEMS_ONLY`` (``true`` / ``false``, défaut ``false``) : recharge
uniquement les tables alimentées par ``items.csv`` sans rescraper Serebii.

Les **GRANT** en lecture pour Streamlit se font **à la main** en SQL (voir page doc Pokopia) ; le DAG ne fait
que **TRUNCATE + INSERT** pour ne pas supprimer les droits entre deux runs.

Connexion Postgres : ``DATA-DB`` — doit viser **la même base applicative** que Streamlit
(variables ``APP_DB_*`` / secrets), pas la base interne Airflow seule.
"""

from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime


@dag(
    dag_id="pokopia_scrape_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["scraping", "pokemon", "pokopia", "postgres"],
    doc_md=__doc__,
)
def pokopia_scrape_dag():
    @task()
    def scrape_load_and_export() -> dict:
        from pathlib import Path

        from pokopia_db import refresh_pokopia_tables
        from pokopia_scrape_lib import (
            build_item_tables_from_csv,
            resolve_items_csv,
            run_pipeline,
            write_tables_to_staging,
        )

        limit = int(Variable.get("POKOPIA_SCRAPE_LIMIT", default_var="0"))
        items_only = Variable.get("POKOPIA_ITEMS_ONLY", default_var="false").lower() in (
            "1",
            "true",
            "yes",
        )
        if items_only:
            tables = build_item_tables_from_csv(resolve_items_csv())
        else:
            tables = run_pipeline(limit=limit, items_csv=None)

        counts = refresh_pokopia_tables(
            tables,
            postgres_conn_id="DATA-DB",
            items_only=items_only,
        )

        paths: list[str] = []
        if Variable.get("POKOPIA_WRITE_STAGING", default_var="false").lower() in (
            "1",
            "true",
            "yes",
        ):
            root = Path(__file__).resolve().parent / "data" / "pokemon_pokopia" / "staging"
            day = pendulum.now("UTC").format("YYYY-MM-DD")
            paths = write_tables_to_staging(tables, root / day)

        return {"db_row_counts": counts, "staging_csv": paths}

    scrape_load_and_export()


pokopia_scrape_dag()

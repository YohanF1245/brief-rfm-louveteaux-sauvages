"""
DAG scraping Pokémon Pokopia (Serebii) + jeux de données objets (items.csv).

Montures Docker (docker-compose) : uniquement ``./dags`` -> ``/opt/airflow/dags``.
Le CSV source est versionné sous ``dags/data/pokemon_pokopia/items.csv``.
Les sorties vont dans ``dags/data/pokemon_pokopia/staging/<date_UTC>/`` (date du run).

Variable Airflow optionnelle : ``POKOPIA_SCRAPE_LIMIT`` (défaut 10).
Dépendances conteneur : ``requests``, ``beautifulsoup4`` (voir docker-compose.dev.yaml).
"""

from __future__ import annotations

from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from pendulum import datetime


def _staging_root() -> Path:
    return Path(__file__).resolve().parent / "data" / "pokemon_pokopia" / "staging"


@dag(
    dag_id="pokopia_scrape_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["scraping", "pokemon", "pokopia"],
    doc_md=__doc__,
)
def pokopia_scrape_dag():
    @task()
    def scrape_and_export() -> dict:
        from pokopia_scrape_lib import run_pipeline, write_tables_to_staging

        limit = int(Variable.get("POKOPIA_SCRAPE_LIMIT", default_var="10"))
        tables = run_pipeline(limit=limit, items_csv=None)
        day = pendulum.now("UTC").format("YYYY-MM-DD")
        out_dir = _staging_root() / day
        paths = write_tables_to_staging(tables, out_dir)
        return {"row_counts": {k: len(v) for k, v in tables.items()}, "csv_paths": paths}

    scrape_and_export()


pokopia_scrape_dag()

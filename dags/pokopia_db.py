"""
Schéma PostgreSQL ``pokopia`` + chargement des tables produites par ``pokopia_scrape_lib``.

Connexion Airflow : ``DATA-DB`` (identique au pipeline RFM / app Postgres).
"""

from __future__ import annotations

from typing import Any

from psycopg2.extras import execute_values

SCHEMA = "pokopia"

DDL_STATEMENTS = [
    f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}",
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.ideal_habitat (
        valeur TEXT PRIMARY KEY
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.specialty (
        valeur TEXT PRIMARY KEY
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.favorite (
        valeur TEXT PRIMARY KEY
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.pokemon (
        nom TEXT PRIMARY KEY,
        num TEXT NOT NULL,
        ideal_habitat_valeur TEXT REFERENCES {SCHEMA}.ideal_habitat (valeur)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.pokemon_specialty (
        pokemon_nom TEXT NOT NULL REFERENCES {SCHEMA}.pokemon (nom) ON DELETE CASCADE,
        specialty_valeur TEXT NOT NULL REFERENCES {SCHEMA}.specialty (valeur) ON DELETE CASCADE,
        ordre SMALLINT NOT NULL,
        PRIMARY KEY (pokemon_nom, ordre),
        UNIQUE (pokemon_nom, specialty_valeur)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.pokemon_favorite (
        pokemon_nom TEXT NOT NULL REFERENCES {SCHEMA}.pokemon (nom) ON DELETE CASCADE,
        favorite_valeur TEXT NOT NULL REFERENCES {SCHEMA}.favorite (valeur) ON DELETE CASCADE,
        ordre SMALLINT NOT NULL,
        PRIMARY KEY (pokemon_nom, ordre),
        UNIQUE (pokemon_nom, favorite_valeur)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.category (
        valeur TEXT PRIMARY KEY
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.type (
        valeur TEXT PRIMARY KEY,
        category_valeur TEXT NOT NULL REFERENCES {SCHEMA}.category (valeur)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.gift_theme (
        valeur TEXT PRIMARY KEY
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA}.item (
        gift_theme_valeur TEXT NOT NULL REFERENCES {SCHEMA}.gift_theme (valeur) ON DELETE CASCADE,
        type_valeur TEXT NOT NULL REFERENCES {SCHEMA}.type (valeur),
        name TEXT NOT NULL,
        PRIMARY KEY (gift_theme_valeur, type_valeur, name)
    )
    """,
]

def ensure_tables(conn) -> None:
    with conn.cursor() as cur:
        for stmt in DDL_STATEMENTS:
            cur.execute(stmt)
    conn.commit()


def refresh_pokopia_tables(
    tables: dict[str, list[dict[str, Any]]],
    *,
    postgres_conn_id: str = "DATA-DB",
) -> dict[str, int]:
    """
    Recrée le contenu du schéma ``pokopia`` (TRUNCATE puis INSERT).
    Retourne le nombre de lignes insérées par table logique.
    """
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
    except ImportError as e:
        raise RuntimeError("Airflow PostgresHook requis pour le chargement BDD.") from e

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    counts: dict[str, int] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
        conn.commit()
        ensure_tables(conn)

        with conn.cursor() as cur:

            def ins(sql: str, rows: list[tuple]) -> int:
                if not rows:
                    return 0
                execute_values(cur, sql, rows, page_size=500)
                return len(rows)

            ih = [(r["valeur"],) for r in tables.get("ideal_habitat", [])]
            counts["ideal_habitat"] = ins(
                f"INSERT INTO {SCHEMA}.ideal_habitat (valeur) VALUES %s", ih
            )

            sp = [(r["valeur"],) for r in tables.get("specialty", [])]
            counts["specialty"] = ins(
                f"INSERT INTO {SCHEMA}.specialty (valeur) VALUES %s", sp
            )

            fav = [(r["valeur"],) for r in tables.get("favorite", [])]
            counts["favorite"] = ins(
                f"INSERT INTO {SCHEMA}.favorite (valeur) VALUES %s", fav
            )

            cat = [(r["valeur"],) for r in tables.get("category", [])]
            counts["category"] = ins(
                f"INSERT INTO {SCHEMA}.category (valeur) VALUES %s", cat
            )

            typ = [
                (r["valeur"], r["category_valeur"]) for r in tables.get("type", [])
            ]
            counts["type"] = ins(
                f"INSERT INTO {SCHEMA}.type (valeur, category_valeur) VALUES %s", typ
            )

            gt = [(r["valeur"],) for r in tables.get("gift_theme", [])]
            counts["gift_theme"] = ins(
                f"INSERT INTO {SCHEMA}.gift_theme (valeur) VALUES %s", gt
            )

            pk = [
                (r["nom"], r["num"], r.get("ideal_habitat_valeur"))
                for r in tables.get("pokemon", [])
            ]
            counts["pokemon"] = ins(
                f"INSERT INTO {SCHEMA}.pokemon (nom, num, ideal_habitat_valeur) VALUES %s",
                pk,
            )

            ps = [
                (r["pokemon_nom"], r["specialty_valeur"], r["ordre"])
                for r in tables.get("pokemon_specialty", [])
            ]
            counts["pokemon_specialty"] = ins(
                f"INSERT INTO {SCHEMA}.pokemon_specialty (pokemon_nom, specialty_valeur, ordre) VALUES %s",
                ps,
            )

            pf = [
                (r["pokemon_nom"], r["favorite_valeur"], r["ordre"])
                for r in tables.get("pokemon_favorite", [])
            ]
            counts["pokemon_favorite"] = ins(
                f"INSERT INTO {SCHEMA}.pokemon_favorite (pokemon_nom, favorite_valeur, ordre) VALUES %s",
                pf,
            )

            it = [
                (r["gift_theme_valeur"], r["type_valeur"], r["name"])
                for r in tables.get("item", [])
            ]
            counts["item"] = ins(
                f"INSERT INTO {SCHEMA}.item (gift_theme_valeur, type_valeur, name) VALUES %s",
                it,
            )

        conn.commit()
    finally:
        conn.close()
    return counts

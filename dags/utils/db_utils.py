import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from .logger import get_logger

load_dotenv()

log = get_logger("db_utils")

CONN_ID = os.getenv("AIRFLOW_CONN_ID", "DATA-DB")

# ─────────────────────────────────────────────────────────
# CREDENTIALS
# ─────────────────────────────────────────────────────────

def _get_credentials() -> dict:
    """
    Détecte l'environnement :
    - Airflow disponible → BaseHook.get_connection(CONN_ID)
    - Local              → .env
    """
    try:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(CONN_ID)
        log.debug(f"Credentials depuis Airflow '{CONN_ID}'")
        return {
            "host"    : conn.host,
            "port"    : conn.port or 5432,
            "dbname"  : conn.schema,
            "user"    : conn.login,
            "password": conn.password,
        }
    except Exception:
        log.debug("Fallback → credentials depuis .env (POSTGRES_* ou APP_DB_* dev)")
        return {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(
                os.getenv("POSTGRES_PORT", os.getenv("APP_DB_PUBLIC_PORT", "5433"))
            ),
            "dbname": os.getenv("POSTGRES_DB", os.getenv("APP_DB_NAME", "rfm")),
            "user": os.getenv("POSTGRES_USER", os.getenv("APP_DB_USER", "rfm")),
            "password": os.getenv(
                "POSTGRES_PASSWORD", os.getenv("APP_DB_PASSWORD", "rfm")
            ),
        }


# ─────────────────────────────────────────────────────────
# CRÉATION DE LA BASE
# ─────────────────────────────────────────────────────────

def create_database():
    creds = _get_credentials()
    log.info(f"Vérification de la base '{creds['dbname']}'")
    conn = psycopg2.connect(
        host=creds["host"], port=creds["port"],
        dbname="postgres",
        user=creds["user"], password=creds["password"],
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (creds["dbname"],))
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {creds['dbname']}")
            log.info(f"Base '{creds['dbname']}' créée")
        else:
            log.info(f"Base '{creds['dbname']}' existe déjà")
    conn.close()


# ─────────────────────────────────────────────────────────
# CRÉATION DES SCHÉMAS
# ─────────────────────────────────────────────────────────

def create_schemas(conn):
    log.info("Création des schémas")
    execute_query(conn, """
        CREATE SCHEMA IF NOT EXISTS raw;
        CREATE SCHEMA IF NOT EXISTS clean;
    """)
    log.info("Schémas raw / clean prêts")


# ─────────────────────────────────────────────────────────
# CONNEXION
# ─────────────────────────────────────────────────────────

def get_connection():
    creds = _get_credentials()
    return psycopg2.connect(**creds)


def get_engine():
    c = _get_credentials()
    url = f"postgresql+psycopg2://{c['user']}:{c['password']}@{c['host']}:{c['port']}/{c['dbname']}"
    return create_engine(url)


def log_connection_target(conn_id: str = CONN_ID) -> dict:
    """Log les paramètres de connexion (sans mot de passe) pour debug dev."""
    creds = _get_credentials()
    source = "airflow" if _credentials_from_airflow() else "env"
    log.info(
        "Postgres [%s] | host=%s port=%s database=%s user=%s (source=%s)",
        conn_id,
        creds["host"],
        creds["port"],
        creds["dbname"],
        creds["user"],
        source,
    )
    return creds


def _credentials_from_airflow() -> bool:
    try:
        from airflow.hooks.base import BaseHook

        BaseHook.get_connection(CONN_ID)
        return True
    except Exception:
        return False


def execute_query(conn, query: str, params=None):
    """Exécute une requête sans retour de données."""
    with conn.cursor() as cur:
        cur.execute(query, params)
    conn.commit()


def fetch_dataframe(query: str, conn) -> pd.DataFrame:
    """Retourne le résultat d'un SELECT en DataFrame."""
    engine = get_engine()
    df = pd.read_sql(query, engine)
    engine.dispose()
    return df
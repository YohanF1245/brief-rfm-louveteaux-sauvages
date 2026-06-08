"""Helpers lakehouse : MinIO (Delta) → dbt (ClickHouse silver/gold)."""

from __future__ import annotations

import os
import subprocess
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from deltalake import write_deltalake

DBT_DIR = Path(os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
WCL_DBT_SILVER = "wcl_reports wcl_fights wcl_player_fight_metrics wcl_ingestion_state"
WCL_DBT_GOLD = "wcl_boss_dps"
BRONZE_DELTA_PATH = "s3://lake/bronze/stack_test/ventes"
DELTA_TABLE_URL = "http://minio:9000/lake/bronze/stack_test/ventes"


def s3_storage_options() -> dict[str, str]:
    return {
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "AWS_ENDPOINT_URL": os.environ.get("AWS_ENDPOINT_URL", "http://minio:9000"),
        "AWS_REGION": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "AWS_ALLOW_HTTP": "true",
    }


def dbt_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))
    env.setdefault("DBT_CLICKHOUSE_HOST", "clickhouse")
    env.setdefault("DBT_CLICKHOUSE_PORT", "8123")
    env.setdefault("DBT_CLICKHOUSE_USER", "default")
    env.setdefault(
        "DBT_CLICKHOUSE_PASSWORD",
        os.environ.get("DBT_CLICKHOUSE_PASSWORD") or os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    env.setdefault("MINIO_ROOT_USER", os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"))
    env.setdefault("MINIO_ROOT_PASSWORD", os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"))
    return env


def sample_ventes_df(rows: int = 500) -> pd.DataFrame:
    """Jeu de test type ventes régionales (léger, reproductible)."""
    rng = pd.Series(range(rows))
    base = date.today() - timedelta(days=30)
    return pd.DataFrame(
        {
            "event_date": [(base + timedelta(days=int(i % 30))).isoformat() for i in rng],
            "region": rng.map(lambda i: ["Nord", "Sud", "Est", "Ouest"][i % 4]),
            "product": rng.map(lambda i: ["Alpha", "Beta", "Gamma"][i % 3]),
            "quantity": rng.map(lambda i: (i % 10) + 1),
            "unit_price": rng.map(lambda i: round(10.0 + (i % 7) * 2.5, 2)),
        }
    )


def ingest_bronze_delta(**_) -> int:
    """Écrit la bronze Delta sur MinIO (bucket lake)."""
    df = sample_ventes_df()
    write_deltalake(
        BRONZE_DELTA_PATH,
        df,
        mode="overwrite",
        storage_options=s3_storage_options(),
    )
    print(f"Bronze Delta écrite : {BRONZE_DELTA_PATH} ({len(df)} lignes)")
    return len(df)


def _dbt_base_cmd(*subcommand: str) -> list[str]:
    return [
        "dbt",
        *subcommand,
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
    ]


def _dbt_ls(select: str) -> list[str]:
    """Liste les nœuds dbt correspondant au sélecteur (vide = rien à exécuter)."""
    result = subprocess.run(
        _dbt_base_cmd("ls", "--select", select),
        capture_output=True,
        text=True,
        check=False,
        env=dbt_env(),
        cwd=str(DBT_DIR),
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"dbt ls échoué pour --select {select!r}:\n{result.stderr or result.stdout}"
        )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def run_dbt(select: str, **_) -> None:
    """Lance dbt run dans le projet monté sur le worker Airflow."""
    subprocess.run(
        _dbt_base_cmd("deps"),
        check=False,
        env=dbt_env(),
        cwd=str(DBT_DIR),
    )
    matched = _dbt_ls(select)
    if not matched:
        all_models = _dbt_ls("fqn:*")
        raise RuntimeError(
            f"Aucun modèle dbt pour --select {select!r}. "
            f"Modèles visibles ({len(all_models)}) : {', '.join(all_models) or '(aucun)'}. "
            f"Vérifier le déploiement de {DBT_DIR}/models/silver/wcl_*.sql sur le worker."
        )
    print(f"Modèles sélectionnés ({len(matched)}) : {', '.join(matched)}")
    cmd = _dbt_base_cmd("run", "--select", select)
    print("Commande :", " ".join(cmd))
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False,
        env=dbt_env(),
        cwd=str(DBT_DIR),
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"dbt run échoué (code {result.returncode})")
    if "Nothing to do" in result.stdout:
        raise RuntimeError(
            f"dbt run n'a rien exécuté pour --select {select!r} (Nothing to do)."
        )


def run_dbt_test(select: str, **_) -> None:
    matched = _dbt_ls(select)
    if not matched:
        print(f"Aucun test dbt pour --select {select!r}, skip.")
        return
    cmd = _dbt_base_cmd("test", "--select", select)
    subprocess.run(cmd, check=True, env=dbt_env(), cwd=str(DBT_DIR))


def validate_gold_power_bi(**_) -> int:
    """Vérifie que la table gold exposée à Power BI contient des lignes."""
    query = "SELECT count() FROM gold.stack_test_daily_kpis"
    url = f"http://clickhouse:8123/?query={urllib.parse.quote(query)}"
    with urllib.request.urlopen(url, timeout=30) as resp:
        n = int(resp.read().decode().strip())
    print(f"Validation gold : {n} lignes dans gold.stack_test_daily_kpis")
    if n <= 0:
        raise RuntimeError("Table gold.stack_test_daily_kpis vide")
    return n

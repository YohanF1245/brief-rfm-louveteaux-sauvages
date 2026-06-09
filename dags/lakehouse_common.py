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
DBT_RUNTIME_DIR = Path(os.environ.get("DBT_RUNTIME_DIR", "/opt/airflow/logs/dbt"))
WCL_DBT_SILVER = (
    "wcl_reports wcl_fights wcl_player_fight_metrics wcl_ingestion_state "
    "wcl_guild_roster wcl_fight_player_guids"
)
WCL_DBT_GOLD = "wcl_boss_dps wcl_player_dps_viz wcl_raid_consumables_viz"
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
    _ensure_dbt_runtime_dirs()
    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))
    # dbt-core >= 1.9 : --target-path retiré de plusieurs commandes (dont deps).
    env.setdefault("DBT_LOG_PATH", str(DBT_RUNTIME_DIR))
    env.setdefault("DBT_TARGET_PATH", str(DBT_RUNTIME_DIR / "target"))
    env.setdefault("DBT_PACKAGES_INSTALL_PATH", str(DBT_RUNTIME_DIR / "dbt_packages"))
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


def _ensure_dbt_runtime_dirs() -> None:
    """Répertoires dbt hors du projet monté (évite Permission denied sur dbt/logs)."""
    for sub in ("", "target", "dbt_packages"):
        path = DBT_RUNTIME_DIR if not sub else DBT_RUNTIME_DIR / sub
        path.mkdir(parents=True, exist_ok=True)


def _dbt_base_cmd(*subcommand: str) -> list[str]:
    return [
        "dbt",
        *subcommand,
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
    ]


def _dbt_deps_cmd() -> list[str]:
    return [
        "dbt",
        "deps",
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
    ]


def _dbt_packages_ready() -> bool:
    """``dbt_utils`` installé dans le répertoire runtime (writable par airflow)."""
    marker = DBT_RUNTIME_DIR / "dbt_packages" / "dbt_utils" / "dbt_project.yml"
    return marker.is_file()


def _ensure_dbt_deps() -> None:
    """Installe les packages une fois dans DBT_RUNTIME_DIR (évite Permission denied)."""
    _ensure_dbt_runtime_dirs()
    pkg_dir = DBT_RUNTIME_DIR / "dbt_packages"
    if _dbt_packages_ready():
        print(f"dbt packages OK : {pkg_dir}")
        return
    print(f"dbt deps → {pkg_dir}")
    deps_result = subprocess.run(
        _dbt_deps_cmd(),
        capture_output=True,
        text=True,
        check=False,
        env=dbt_env(),
        cwd=str(DBT_DIR),
    )
    if deps_result.stdout:
        print(deps_result.stdout)
    if deps_result.returncode != 0:
        raise RuntimeError(
            "dbt deps échoué (dbt_utils requis). "
            f"stderr:\n{deps_result.stderr or deps_result.stdout}"
        )


def _dbt_ls(select: str) -> list[str]:
    """Liste les nœuds dbt correspondant au sélecteur (vide = rien à exécuter)."""
    result = subprocess.run(
        _dbt_base_cmd(
            "ls",
            "--select",
            select,
            "--resource-type",
            "model",
            "--output",
            "name",
            "--quiet",
        ),
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


def _assert_wcl_models_on_disk() -> None:
    """Vérifie que les SQL WCL sont bien montés sur le worker (/opt/airflow/dbt)."""
    expected = [
        "wcl_reports.sql",
        "wcl_fights.sql",
        "wcl_player_fight_metrics.sql",
        "wcl_ingestion_state.sql",
        "wcl_guild_roster.sql",
        "wcl_fight_player_guids.sql",
    ]
    silver_dir = DBT_DIR / "models" / "silver"
    gold_dir = DBT_DIR / "models" / "gold"
    gold_expected = ["wcl_boss_dps.sql", "wcl_player_dps_viz.sql", "wcl_raid_consumables_viz.sql"]
    present = sorted(p.name for p in silver_dir.glob("wcl_*.sql")) if silver_dir.is_dir() else []
    gold_present = sorted(p.name for p in gold_dir.glob("wcl_*.sql")) if gold_dir.is_dir() else []
    missing = [name for name in expected if name not in present]
    missing_gold = [name for name in gold_expected if name not in gold_present]
    if missing or missing_gold:
        raise RuntimeError(
            "Modèles dbt WCL absents sur le worker Airflow. "
            f"Présents dans {silver_dir}: {present or '(aucun)'}. "
            f"Manquants silver: {missing or '—'}. "
            f"Gold présents: {gold_present or '(aucun)'}. "
            f"Manquants gold: {missing_gold or '—'}. "
            "Déployer le repo ou copier dbt/models/{{silver,gold}}/wcl_*.sql sur le serveur."
        )


def run_dbt(select: str, **_) -> None:
    """Lance dbt run dans le projet monté sur le worker Airflow."""
    if "wcl_" in select:
        _assert_wcl_models_on_disk()
    if "wcl_ingestion_state" in select:
        try:
            from warcraftlogs_lake import repair_ingestion_state_schema

            repair_ingestion_state_schema()
        except Exception as exc:
            print(f"repair_ingestion_state_schema ignoré : {exc}")

    _ensure_dbt_deps()
    matched = _dbt_ls(select)
    if not matched:
        listed = subprocess.run(
            _dbt_base_cmd("ls", "--resource-type", "model", "--output", "name", "--quiet"),
            capture_output=True,
            text=True,
            check=False,
            env=dbt_env(),
            cwd=str(DBT_DIR),
        )
        model_names = [
            line.strip() for line in (listed.stdout or "").splitlines() if line.strip()
        ]
        raise RuntimeError(
            f"Aucun modèle dbt pour --select {select!r}. "
            f"Modèles visibles ({len(model_names)}) : {', '.join(model_names) or '(aucun)'}. "
            f"Vérifier {DBT_DIR}/models/ sur le worker."
        )
    print(f"Modèles sélectionnés ({len(matched)}) : {', '.join(matched)}")
    cmd = _dbt_base_cmd("run", "--select", select)
    # Gold WCL : --full-refresh recrée les tables (schéma ClickHouse), sans pre_hook DROP
    # qui casse l'échange de tables du adapter dbt-clickhouse.
    if set(matched) & {"wcl_boss_dps", "wcl_player_dps_viz", "wcl_raid_consumables_viz"}:
        cmd.append("--full-refresh")
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
        detail = result.stderr or result.stdout
        raise RuntimeError(f"dbt run échoué (code {result.returncode}):\n{detail}")
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

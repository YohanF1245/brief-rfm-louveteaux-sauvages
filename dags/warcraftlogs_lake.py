"""Export bronze Warcraft Logs vers Delta MinIO (incrémental par report)."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from lakehouse_common import s3_storage_options

BRONZE_BASE = "s3://lake/bronze/warcraftlogs"

BRONZE_PATHS = {
    "guild_reports": f"{BRONZE_BASE}/guild_reports",
    "fights": f"{BRONZE_BASE}/fights",
    "fight_player_stats": f"{BRONZE_BASE}/fight_player_stats",
    "reports_raw": f"{BRONZE_BASE}/reports_raw",
}


def _prepare_delta_df(df: pd.DataFrame) -> pd.DataFrame:
    """Typage explicite pour Delta/Arrow (colonnes 100 % NULL interdites sans type)."""
    out = df.copy()
    for col in out.columns:
        series = out[col]
        if pd.api.types.is_datetime64_any_dtype(series):
            out[col] = pd.to_datetime(series, utc=True)
            continue
        if pd.api.types.is_bool_dtype(series):
            out[col] = series.astype("boolean")
            continue
        if pd.api.types.is_integer_dtype(series):
            out[col] = series.astype("Int64")
            continue
        if pd.api.types.is_float_dtype(series):
            out[col] = series.astype("Float64")
            continue
        out[col] = series.astype("string")
    return out


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _delta_table_exists(path: str) -> bool:
    storage = s3_storage_options()
    try:
        DeltaTable(path, storage_options=storage)
        return True
    except Exception:
        return False


def replace_report_in_bronze(path: str, df: pd.DataFrame, report_code: str) -> int:
    """Remplace les lignes d'un report dans une table Delta (delete + append)."""
    if df.empty:
        return 0

    prepared = _prepare_delta_df(df)
    storage = s3_storage_options()
    safe_code = _escape_sql_literal(report_code)

    if _delta_table_exists(path):
        dt = DeltaTable(path, storage_options=storage)
        dt.delete(f"report_code = '{safe_code}'")

    mode = "append" if _delta_table_exists(path) else "overwrite"
    write_deltalake(
        path,
        prepared,
        mode=mode,
        schema_mode="merge" if mode == "append" else "overwrite",
        storage_options=storage,
    )
    return len(prepared)


def export_report_raw_to_bronze(report_code: str, raw_payload: dict[str, Any]) -> int:
    """Bronze JSON brut : réponse API fights complète par report."""
    row = pd.DataFrame(
        [
            {
                "report_code": report_code,
                "raw_json": json.dumps(raw_payload, ensure_ascii=False),
                "fetched_at": pd.Timestamp.utcnow(),
            }
        ]
    )
    return replace_report_in_bronze(BRONZE_PATHS["reports_raw"], row, report_code)


def export_report_tables_to_bronze(
    report_code: str,
    reports_df: pd.DataFrame,
    fights_df: pd.DataFrame,
    stats_df: pd.DataFrame,
) -> dict[str, int]:
    """Écrit les 3 tables relationnelles bronze pour un seul report."""
    counts = {
        "guild_reports": replace_report_in_bronze(BRONZE_PATHS["guild_reports"], reports_df, report_code),
        "fights": replace_report_in_bronze(BRONZE_PATHS["fights"], fights_df, report_code),
        "fight_player_stats": replace_report_in_bronze(BRONZE_PATHS["fight_player_stats"], stats_df, report_code),
    }
    return counts


def export_bronze_delta(**_) -> int:
    """Compat : snapshot complet Postgres → Delta (overwrite). Préférer l'ingest incrémental."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from warcraftlogs_db import FULL_FIGHTS, FULL_PLAYER_STATS, FULL_REPORTS

    hook = PostgresHook(postgres_conn_id="DATA-DB")
    mapping = {
        "guild_reports": FULL_REPORTS,
        "fights": FULL_FIGHTS,
        "fight_player_stats": FULL_PLAYER_STATS,
    }
    total = 0
    for name, sql_table in mapping.items():
        df = hook.get_pandas_df(f"SELECT * FROM {sql_table}")
        path = BRONZE_PATHS[name]
        if df.empty:
            print(f"Bronze WCL {name} : Postgres vide, skip.")
            continue
        prepared = _prepare_delta_df(df)
        write_deltalake(
            path,
            prepared,
            mode="overwrite",
            schema_mode="overwrite",
            storage_options=s3_storage_options(),
        )
        print(f"Bronze Delta snapshot : {path} ({len(prepared)} lignes)")
        total += len(prepared)
    return total

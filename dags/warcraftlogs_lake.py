"""Bronze Warcraft Logs : Delta MinIO uniquement (pas de staging Postgres)."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
import s3fs
from deltalake import DeltaTable, write_deltalake

from lakehouse_common import s3_storage_options

BRONZE_BASE = "s3://lake/bronze/warcraftlogs"

BRONZE_PATHS = {
    "guild_reports": f"{BRONZE_BASE}/guild_reports",
    "guild_roster": f"{BRONZE_BASE}/guild_roster",
    "fights": f"{BRONZE_BASE}/fights",
    "fight_player_stats": f"{BRONZE_BASE}/fight_player_stats",
    "fight_tables_raw": f"{BRONZE_BASE}/fight_tables_raw",
    "reports_raw": f"{BRONZE_BASE}/reports_raw",
    "ingestion_state": f"{BRONZE_BASE}/ingestion_state",
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


def _s3_fs() -> s3fs.S3FileSystem:
    storage = s3_storage_options()
    return s3fs.S3FileSystem(
        key=storage["AWS_ACCESS_KEY_ID"],
        secret=storage["AWS_SECRET_ACCESS_KEY"],
        client_kwargs={"endpoint_url": storage["AWS_ENDPOINT_URL"]},
    )


def _s3_object_key(s3_uri: str) -> str:
    return s3_uri.removeprefix("s3://")


def _delete_delta_prefix(path: str) -> None:
    """Supprime un préfixe Delta corrompu ou vide sur MinIO."""
    fs = _s3_fs()
    key = _s3_object_key(path)
    if fs.exists(key):
        fs.rm(key, recursive=True)
        print(f"Delta supprimé : {key}")


def _delta_table_exists(path: str) -> bool:
    storage = s3_storage_options()
    try:
        DeltaTable(path, storage_options=storage)
        return True
    except Exception:
        return False


def _delta_table_readable(path: str) -> bool:
    if not _delta_table_exists(path):
        return False
    storage = s3_storage_options()
    try:
        DeltaTable(path, storage_options=storage).to_pandas()
        return True
    except Exception as exc:
        print(f"Delta illisible {path} : {exc}")
        _delete_delta_prefix(path)
        return False


def read_delta_df(path: str) -> pd.DataFrame:
    if not _delta_table_readable(path):
        return pd.DataFrame()
    storage = s3_storage_options()
    return DeltaTable(path, storage_options=storage).to_pandas()


def replace_report_in_bronze(
    path: str,
    df: pd.DataFrame,
    report_code: str,
    *,
    fight_id: int | None = None,
) -> int:
    """Remplace les lignes d'un report (ou report+fight) dans une table Delta."""
    if df.empty:
        return 0

    prepared = _prepare_delta_df(df)
    storage = s3_storage_options()
    safe_code = _escape_sql_literal(report_code)

    exists = _delta_table_exists(path)
    if exists:
        dt = DeltaTable(path, storage_options=storage)
        if fight_id is not None:
            dt.delete(f"report_code = '{safe_code}' AND fight_id = {int(fight_id)}")
        else:
            dt.delete(f"report_code = '{safe_code}'")

    mode = "append" if exists else "overwrite"
    write_deltalake(
        path,
        prepared,
        mode=mode,
        schema_mode="merge" if mode == "append" else "overwrite",
        storage_options=storage,
    )
    return len(prepared)


def report_catalog_row(
    report: dict[str, Any],
    guild: dict[str, Any],
    keys: dict[str, Any],
) -> dict[str, Any]:
    zone = (report.get("zone") or {}) if isinstance(report.get("zone"), dict) else {}
    owner = (report.get("owner") or {}) if isinstance(report.get("owner"), dict) else {}
    report_guild = (report.get("guild") or {}) if isinstance(report.get("guild"), dict) else {}
    return {
        "report_code": report.get("code"),
        "guild_id": report_guild.get("id") or guild.get("id"),
        "guild_name": report_guild.get("name") or guild.get("name"),
        "server_region": keys["server_region"],
        "server_slug": keys["server_slug"],
        "title": report.get("title"),
        "zone_name": zone.get("name"),
        "owner_name": owner.get("name"),
        "owner_user_id": report.get("_owner_user_id") or owner.get("id"),
        "log_source": report.get("_log_source") or "guild",
        "visibility": report.get("visibility"),
        "start_time_ms": report.get("startTime"),
        "end_time_ms": report.get("endTime"),
        "fetched_at": pd.Timestamp.utcnow(),
    }


def _ensure_ingestion_state_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Colonnes optionnelles requises par dbt / quota (schéma Delta évolutif)."""
    out = df.copy()
    if "last_ingest_points" not in out.columns:
        out["last_ingest_points"] = pd.NA
    return out


def repair_ingestion_state_schema() -> bool:
    """Ajoute ``last_ingest_points`` au bronze si absent (ClickHouse deltaLake)."""
    path = BRONZE_PATHS["ingestion_state"]
    if not _delta_table_readable(path):
        return False
    df = read_delta_df(path)
    if "last_ingest_points" in df.columns:
        return False
    df = _ensure_ingestion_state_columns(df)
    storage = s3_storage_options()
    write_deltalake(
        path,
        _prepare_delta_df(df),
        mode="overwrite",
        storage_options=storage,
    )
    print(f"ingestion_state : colonne last_ingest_points ajoutée ({len(df)} lignes).")
    return True


def _catalog_state_row(
    report: dict[str, Any],
    guild: dict[str, Any],
    keys: dict[str, Any],
    *,
    attempts: int = 0,
) -> dict[str, Any]:
    return {
        "report_code": report.get("code"),
        "status": "pending",
        "title": report.get("title"),
        "start_time_ms": report.get("startTime"),
        "last_error": None,
        "ingestion_attempts": attempts,
        "last_ingest_points": None,
        "synced_at": None,
        "fetched_at": pd.Timestamp.utcnow(),
        "catalog_json": json.dumps(
            {"report": report, "guild": guild, "keys": keys},
            ensure_ascii=False,
        ),
    }


def write_catalog_to_delta(
    reports: list[dict[str, Any]],
    guild: dict[str, Any],
    keys: dict[str, Any],
    *,
    attempts: int = 0,
) -> int:
    """Écrit tout le catalogue en ``pending`` (création / réparation ingestion_state)."""
    rows = [
        _catalog_state_row(report, guild, keys, attempts=attempts)
        for report in reports
        if report.get("code")
    ]
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    storage = s3_storage_options()
    write_deltalake(
        BRONZE_PATHS["ingestion_state"],
        _prepare_delta_df(df),
        mode="overwrite",
        storage_options=storage,
    )
    return len(df)


def bulk_upsert_catalog_state(
    reports: list[dict[str, Any]],
    guild: dict[str, Any],
    keys: dict[str, Any],
) -> int:
    """Catalogue API → Delta ingestion_state en une passe (évite l'épuisement TCP MinIO)."""
    if not _delta_table_readable(BRONZE_PATHS["ingestion_state"]):
        return write_catalog_to_delta(reports, guild, keys)

    df = _ensure_ingestion_state_columns(read_delta_df(BRONZE_PATHS["ingestion_state"]))
    updated = 0

    for report in reports:
        code = report.get("code")
        if not code:
            continue

        if not df.empty:
            match = df[df["report_code"] == code]
            if not match.empty and str(match.iloc[0].get("status", "")) == "ok":
                continue
            attempts = int(match.iloc[0].get("ingestion_attempts") or 0) if not match.empty else 0
        else:
            attempts = 0

        row = _catalog_state_row(report, guild, keys, attempts=attempts)
        if not df.empty and code in df["report_code"].values:
            idx = df.index[df["report_code"] == code][0]
            for col, value in row.items():
                df.at[idx, col] = value
        else:
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        updated += 1

    if updated == 0:
        return 0

    storage = s3_storage_options()
    write_deltalake(
        BRONZE_PATHS["ingestion_state"],
        _prepare_delta_df(_ensure_ingestion_state_columns(df)),
        mode="overwrite",
        storage_options=storage,
    )
    return updated


def upsert_catalog_state(
    report: dict[str, Any],
    guild: dict[str, Any],
    keys: dict[str, Any],
) -> None:
    """Catalogue API → Delta ingestion_state (pending si pas déjà ok)."""
    bulk_upsert_catalog_state([report], guild, keys)


def list_pending_report_codes(limit: int | None = None) -> list[str]:
    df = read_delta_df(BRONZE_PATHS["ingestion_state"])
    if df.empty:
        return []
    pending = df[df["status"].isin(["pending", "error"])].copy()
    pending = pending.sort_values("start_time_ms", ascending=False, na_position="last")
    codes = pending["report_code"].dropna().astype(str).tolist()
    if limit:
        codes = codes[: int(limit)]
    return codes


def median_ingest_points(*, last_n: int = 20) -> float | None:
    """Médiane des points API mesurés sur les derniers reports ``ok``."""
    df = read_delta_df(BRONZE_PATHS["ingestion_state"])
    if df.empty or "last_ingest_points" not in df.columns:
        return None
    ok = df[df["status"].astype(str) == "ok"].copy()
    pts = pd.to_numeric(ok["last_ingest_points"], errors="coerce").dropna()
    if pts.empty:
        return None
    sample = pts.tail(max(1, last_n))
    return float(sample.median())


def mark_ingestion_ok(report_code: str, *, ingest_points: int | None = None) -> None:
    df = read_delta_df(BRONZE_PATHS["ingestion_state"])
    if df.empty or report_code not in df["report_code"].values:
        return
    row = df[df["report_code"] == report_code].iloc[0].to_dict()
    row["status"] = "ok"
    row["last_error"] = None
    row["synced_at"] = pd.Timestamp.utcnow()
    row["fetched_at"] = pd.Timestamp.utcnow()
    if ingest_points is not None:
        row["last_ingest_points"] = int(ingest_points)
    replace_report_in_bronze(BRONZE_PATHS["ingestion_state"], pd.DataFrame([row]), report_code)


def mark_ingestion_error(report_code: str, error: str) -> None:
    df = read_delta_df(BRONZE_PATHS["ingestion_state"])
    if df.empty or report_code not in df["report_code"].values:
        return
    row = df[df["report_code"] == report_code].iloc[0].to_dict()
    row["status"] = "error"
    row["last_error"] = error[:2000]
    row["ingestion_attempts"] = int(row.get("ingestion_attempts") or 0) + 1
    row["fetched_at"] = pd.Timestamp.utcnow()
    replace_report_in_bronze(BRONZE_PATHS["ingestion_state"], pd.DataFrame([row]), report_code)


def reset_ingestion_status_to_pending(
    *,
    from_statuses: tuple[str, ...] = ("ok", "error"),
    report_codes: list[str] | None = None,
) -> int:
    """
    Repasse des reports en ``pending`` pour forcer un ré-ingest API.

    Par défaut : tous les ``ok`` et ``error``. Bronze existante écrasée report par report
    à la prochaine ingestion (``replace_report_in_bronze``).
    """
    df = read_delta_df(BRONZE_PATHS["ingestion_state"])
    if df.empty:
        print("ingestion_state vide — rien à reset.")
        return 0

    mask = df["status"].astype(str).isin(from_statuses)
    if report_codes:
        wanted = {str(c).strip() for c in report_codes if str(c).strip()}
        mask &= df["report_code"].astype(str).isin(wanted)

    n = int(mask.sum())
    if n == 0:
        print("Aucun report à repasser en pending.")
        return 0

    df.loc[mask, "status"] = "pending"
    df.loc[mask, "last_error"] = None
    df.loc[mask, "synced_at"] = None
    df.loc[mask, "fetched_at"] = pd.Timestamp.utcnow()

    storage = s3_storage_options()
    write_deltalake(
        BRONZE_PATHS["ingestion_state"],
        _prepare_delta_df(_ensure_ingestion_state_columns(df)),
        mode="overwrite",
        storage_options=storage,
    )
    print(f"ingestion_state : {n} report(s) repassé(s) en pending ({from_statuses}).")
    return n


def load_catalog_context(report_code: str) -> dict[str, Any] | None:
    df = read_delta_df(BRONZE_PATHS["ingestion_state"])
    if df.empty:
        return None
    match = df[df["report_code"] == report_code]
    if match.empty:
        return None
    raw = match.iloc[0].get("catalog_json")
    if not raw:
        return None
    return json.loads(raw)


def export_report_raw_to_bronze(report_code: str, raw_payload: dict[str, Any]) -> int:
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


def export_fight_player_stats_to_bronze(
    report_code: str,
    fight_id: int,
    stat_rows: list[dict[str, Any]],
) -> int:
    """Bronze stats parsées : flush par fight (évite d'accumuler tout le report en RAM)."""
    if not stat_rows:
        return 0
    df = pd.DataFrame(stat_rows)
    df["fetched_at"] = pd.Timestamp.utcnow()
    return replace_report_in_bronze(
        BRONZE_PATHS["fight_player_stats"],
        df,
        report_code,
        fight_id=fight_id,
    )


def export_fight_tables_raw_to_bronze(
    report_code: str,
    fight_id: int,
    raw_by_type: dict[str, Any],
) -> int:
    """Bronze JSON brut : toutes les tables API par fight et TableDataType."""
    rows = []
    for data_type, payload in raw_by_type.items():
        rows.append(
            {
                "report_code": report_code,
                "fight_id": fight_id,
                "data_type": data_type,
                "raw_json": json.dumps(payload, ensure_ascii=False),
                "fetched_at": pd.Timestamp.utcnow(),
            }
        )
    if not rows:
        return 0
    return replace_report_in_bronze(
        BRONZE_PATHS["fight_tables_raw"],
        pd.DataFrame(rows),
        report_code,
        fight_id=fight_id,
    )


def export_report_tables_to_bronze(
    report_code: str,
    reports_df: pd.DataFrame,
    fights_df: pd.DataFrame,
) -> dict[str, int]:
    """Métadonnées report + fights. ``fight_player_stats`` : flush par fight (``export_fight_player_stats_to_bronze``)."""
    return {
        "guild_reports": replace_report_in_bronze(BRONZE_PATHS["guild_reports"], reports_df, report_code),
        "fights": replace_report_in_bronze(BRONZE_PATHS["fights"], fights_df, report_code),
    }


def write_guild_roster_to_bronze(rows: list[dict[str, Any]]) -> int:
    """Snapshot complet du roster guilde (overwrite quotidien)."""
    if not rows:
        print("Roster guilde vide, bronze inchangée.")
        return 0
    df = pd.DataFrame(rows)
    df["fetched_at"] = pd.Timestamp.utcnow()
    storage = s3_storage_options()
    write_deltalake(
        BRONZE_PATHS["guild_roster"],
        _prepare_delta_df(df),
        mode="overwrite",
        storage_options=storage,
    )
    print(f"Bronze roster : {BRONZE_PATHS['guild_roster']} ({len(df)} membres)")
    return len(df)

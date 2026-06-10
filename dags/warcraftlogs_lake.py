"""Bronze Warcraft Logs : Delta MinIO uniquement (pas de staging Postgres)."""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
import s3fs
from deltalake import DeltaTable, write_deltalake

from lakehouse_common import s3_storage_options

BRONZE_BASE = "s3://lake/bronze/warcraftlogs"

# Tables bronze "données brutes" — voir docs/wcl_bronze.md pour le schéma complet.
BRONZE_PATHS = {
    # Catalogue + métadonnées (1 ligne / report, 1 ligne / fight)
    "guild_reports": f"{BRONZE_BASE}/guild_reports",
    "guild_roster": f"{BRONZE_BASE}/guild_roster",
    "fights": f"{BRONZE_BASE}/fights",
    "reports_raw": f"{BRONZE_BASE}/reports_raw",
    "ingestion_state": f"{BRONZE_BASE}/ingestion_state",
    # masterData : référentiel id → acteur / ability (jointures events)
    "master_info": f"{BRONZE_BASE}/master_info",
    "master_actors": f"{BRONZE_BASE}/master_actors",
    "master_abilities": f"{BRONZE_BASE}/master_abilities",
    # playerDetails : specs / ilvl / talents / gear par joueur
    "player_details": f"{BRONZE_BASE}/player_details",
    # events : log brut complet (dataType: All), 1 ligne = 1 event
    "events": f"{BRONZE_BASE}/events",
}


# Colonnes entières nullables : typage explicite Int64 AVANT écriture Delta.
# Sans ça, une page/report où la colonne est 100 % NULL serait inférée "string"
# et entrerait en conflit de schéma avec les appends suivants (Int64).
EVENTS_INT_COLUMNS: tuple[str, ...] = (
    "fight_id",
    "page_index",
    "event_index",
    "timestamp_ms",
    "source_id",
    "source_instance",
    "target_id",
    "target_instance",
    "ability_game_id",
)
ACTORS_INT_COLUMNS: tuple[str, ...] = ("actor_id", "game_id", "pet_owner_id")
ABILITIES_INT_COLUMNS: tuple[str, ...] = ("ability_game_id",)
MASTER_INFO_INT_COLUMNS: tuple[str, ...] = (
    "log_version",
    "game_version",
    "actors_count",
    "abilities_count",
)
PLAYER_DETAILS_INT_COLUMNS: tuple[str, ...] = (
    "player_id",
    "player_guid",
    "min_item_level",
    "max_item_level",
    "potion_use",
    "healthstone_use",
)


def _coerce_int64(df: pd.DataFrame, columns: tuple[str, ...]) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


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


def delete_report_rows(path: str, report_code: str) -> None:
    """Supprime les lignes d'un report dans une table Delta (idempotence ré-ingest).

    À appeler AVANT un flux d'appends paginés (events) : delete une fois,
    puis append page par page sans relire la table.
    """
    if not _delta_table_exists(path):
        return
    storage = s3_storage_options()
    safe_code = _escape_sql_literal(report_code)
    DeltaTable(path, storage_options=storage).delete(f"report_code = '{safe_code}'")


def append_rows_to_bronze(
    path: str,
    rows: list[dict[str, Any]],
    *,
    int_columns: tuple[str, ...] = (),
) -> int:
    """Append brut de lignes dans une table Delta (création si absente)."""
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    df["fetched_at"] = pd.Timestamp.utcnow()
    df = _coerce_int64(df, int_columns)
    prepared = _prepare_delta_df(df)
    storage = s3_storage_options()
    exists = _delta_table_exists(path)
    write_deltalake(
        path,
        prepared,
        mode="append" if exists else "overwrite",
        schema_mode="merge" if exists else "overwrite",
        storage_options=storage,
    )
    return len(prepared)


def export_master_data_to_bronze(
    report_code: str,
    info_row: dict[str, Any],
    actor_rows: list[dict[str, Any]],
    ability_rows: list[dict[str, Any]],
) -> dict[str, int]:
    """Bronze masterData : info report + acteurs + abilities (remplace le report)."""
    info_df = _coerce_int64(pd.DataFrame([info_row]), MASTER_INFO_INT_COLUMNS)
    info_df["fetched_at"] = pd.Timestamp.utcnow()
    counts = {
        "master_info": replace_report_in_bronze(
            BRONZE_PATHS["master_info"], info_df, report_code
        ),
        "master_actors": 0,
        "master_abilities": 0,
    }
    if actor_rows:
        actors_df = _coerce_int64(pd.DataFrame(actor_rows), ACTORS_INT_COLUMNS)
        actors_df["fetched_at"] = pd.Timestamp.utcnow()
        counts["master_actors"] = replace_report_in_bronze(
            BRONZE_PATHS["master_actors"], actors_df, report_code
        )
    if ability_rows:
        abilities_df = _coerce_int64(pd.DataFrame(ability_rows), ABILITIES_INT_COLUMNS)
        abilities_df["fetched_at"] = pd.Timestamp.utcnow()
        counts["master_abilities"] = replace_report_in_bronze(
            BRONZE_PATHS["master_abilities"], abilities_df, report_code
        )
    return counts


def export_player_details_to_bronze(
    report_code: str,
    rows: list[dict[str, Any]],
) -> int:
    """Bronze playerDetails : 1 ligne / joueur / rôle (remplace le report)."""
    if not rows:
        return 0
    df = _coerce_int64(pd.DataFrame(rows), PLAYER_DETAILS_INT_COLUMNS)
    df["fetched_at"] = pd.Timestamp.utcnow()
    return replace_report_in_bronze(BRONZE_PATHS["player_details"], df, report_code)


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

"""Ingestion WCL : API → bronze Delta (MinIO). ClickHouse lit le lake via deltaLake()."""

from __future__ import annotations

import json
import os
import re
from typing import Any

import pandas as pd

from warcraftlogs_common import (
    fetch_all_guild_and_member_reports,
    fetch_fight_tables,
    fetch_report_fights,
    fight_rows_from_report,
    guild_url_from_env,
)
from warcraftlogs_lake import (
    bulk_upsert_catalog_state,
    export_fight_tables_raw_to_bronze,
    export_report_raw_to_bronze,
    export_report_tables_to_bronze,
    list_pending_report_codes,
    load_catalog_context,
    mark_ingestion_error,
    mark_ingestion_ok,
    report_catalog_row,
)

_RATE_LIMIT_RE = re.compile(r"\b(429|4\d{2})\b|rate.?limit", re.I)


def _is_rate_limit_error(exc: BaseException) -> bool:
    return bool(_RATE_LIMIT_RE.search(str(exc)))


def _ingest_batch_size() -> int:
    try:
        from airflow.sdk import Variable

        raw = str(Variable.get("wcl_ingest_batch_size", default="50")).strip()
        return max(1, int(raw))
    except Exception:
        return max(1, int(os.environ.get("WCL_INGEST_BATCH_SIZE", "50")))


def sync_report_catalog(**_) -> int:
    """Catalogue API → Delta ingestion_state (léger, reprise safe)."""
    payload = fetch_all_guild_and_member_reports(guild_url_from_env())
    guild = payload.get("guild") or {}
    keys = payload["query"]
    reports = payload.get("reports") or []
    if not reports:
        print("Catalogue WCL vide.")
        return 0

    pending_n = bulk_upsert_catalog_state(reports, guild, keys)

    print(
        f"Catalogue Delta : {len(reports)} reports API, {pending_n} pending/error mis à jour "
        f"(guilde={payload.get('guild_reports_count')} "
        f"perso={payload.get('personal_reports_count')})"
    )
    return len(reports)


def _fights_df(fight_rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not fight_rows:
        return pd.DataFrame()
    df = pd.DataFrame(fight_rows)
    df["fetched_at"] = pd.Timestamp.utcnow()
    return df


def _stats_df(stat_rows: list[dict[str, Any]]) -> pd.DataFrame:
    if not stat_rows:
        return pd.DataFrame()
    df = pd.DataFrame(stat_rows)
    df["fetched_at"] = pd.Timestamp.utcnow()
    return df


def _ingest_single_report(report_code: str) -> dict[str, int]:
    """Pipeline complet : API → bronze Delta (commit par report)."""
    catalog = load_catalog_context(report_code)
    api_report = fetch_report_fights(report_code)
    export_report_raw_to_bronze(report_code, api_report)

    fight_rows = fight_rows_from_report(report_code, api_report)
    stat_rows: list[dict[str, Any]] = []

    for fight in fight_rows:
        start_ms = fight.get("start_time_ms")
        end_ms = fight.get("end_time_ms")
        if start_ms is None or end_ms is None or int(start_ms) >= int(end_ms):
            continue

        metrics, raw_tables = fetch_fight_tables(report_code, int(start_ms), int(end_ms))
        export_fight_tables_raw_to_bronze(report_code, int(fight["fight_id"]), raw_tables)
        for metric_rows in metrics.values():
            for row in metric_rows:
                stat_rows.append(
                    {
                        "report_code": report_code,
                        "fight_id": fight["fight_id"],
                        "player_name": row["player_name"],
                        "metric": row["metric"],
                        "player_id": row.get("player_id"),
                        "class_name": row.get("class_name"),
                        "spec_name": row.get("spec_name"),
                        "total_amount": row.get("total_amount"),
                        "active_time_ms": row.get("active_time_ms"),
                        "rate_per_sec": row.get("rate_per_sec"),
                        "extra": json.dumps(row.get("extra") or {}),
                    }
                )

    if catalog:
        reports_df = pd.DataFrame(
            [report_catalog_row(catalog["report"], catalog["guild"], catalog["keys"])]
        )
    else:
        owner = (api_report.get("owner") or {}) if isinstance(api_report.get("owner"), dict) else {}
        zone = (api_report.get("zone") or {}) if isinstance(api_report.get("zone"), dict) else {}
        guild = (api_report.get("guild") or {}) if isinstance(api_report.get("guild"), dict) else {}
        reports_df = pd.DataFrame(
            [
                {
                    "report_code": report_code,
                    "guild_id": guild.get("id"),
                    "guild_name": guild.get("name"),
                    "title": api_report.get("title"),
                    "zone_name": zone.get("name"),
                    "owner_name": owner.get("name"),
                    "owner_user_id": owner.get("id"),
                    "start_time_ms": api_report.get("startTime"),
                    "end_time_ms": api_report.get("endTime"),
                    "fetched_at": pd.Timestamp.utcnow(),
                }
            ]
        )

    bronze_counts = export_report_tables_to_bronze(
        report_code,
        reports_df,
        _fights_df(fight_rows),
        _stats_df(stat_rows),
    )
    mark_ingestion_ok(report_code)

    return {
        "fights": len(fight_rows),
        "stats": len(stat_rows),
        **{f"bronze_{k}": v for k, v in bronze_counts.items()},
    }


def ingest_reports_incremental(**_) -> int:
    """Ingère les reports pending/error depuis Delta state, un par un."""
    batch_limit = _ingest_batch_size()
    pending = list_pending_report_codes(limit=batch_limit)
    if not pending:
        print("Aucun report en attente dans le lake.")
        return 0

    processed = 0
    print(f"{len(pending)} reports à ingérer (batch max {batch_limit}).")
    for report_code in pending:
        try:
            summary = _ingest_single_report(report_code)
            processed += 1
            print(f"OK {report_code} : {summary}")
        except Exception as exc:
            mark_ingestion_error(report_code, str(exc))
            print(f"ERREUR {report_code} : {exc}")
            if _is_rate_limit_error(exc):
                print("Quota / 4xx API — arrêt du batch (reports déjà OK conservés).")
                break

    print(f"Ingestion bronze terminée : {processed}/{len(pending)} reports.")
    return processed

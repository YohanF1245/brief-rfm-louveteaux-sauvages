"""Ingestion WCL : API → bronze Delta (MinIO). ClickHouse lit le lake via deltaLake()."""

from __future__ import annotations

import json
import os
import re
from typing import Any

import pandas as pd

from warcraftlogs_common import (
    estimated_report_cost,
    fetch_all_guild_and_member_reports,
    fetch_fight_tables,
    fetch_report_fights,
    fight_rows_from_report,
    guild_url_from_env,
    measure_report_points_delta,
    quota_allows_next_report,
    refresh_rate_limit_data,
)
from warcraftlogs_lake import (
    BRONZE_PATHS,
    bulk_upsert_catalog_state,
    export_fight_tables_raw_to_bronze,
    export_report_raw_to_bronze,
    export_report_tables_to_bronze,
    list_pending_report_codes,
    load_catalog_context,
    mark_ingestion_error,
    mark_ingestion_ok,
    median_ingest_points,
    report_catalog_row,
    _delta_table_readable,
)

_RATE_LIMIT_RE = re.compile(r"\b(429|4\d{2})\b|rate.?limit", re.I)


def _is_rate_limit_error(exc: BaseException) -> bool:
    return bool(_RATE_LIMIT_RE.search(str(exc)))


def _ingest_batch_size() -> int:
    try:
        from airflow.sdk import Variable

        raw = str(Variable.get("wcl_ingest_batch_size", default="25")).strip()
        return max(1, int(raw))
    except Exception:
        return max(1, int(os.environ.get("WCL_INGEST_BATCH_SIZE", "25")))


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
    if not _delta_table_readable(BRONZE_PATHS["ingestion_state"]):
        raise RuntimeError(
            "ingestion_state Delta absente ou illisible après sync_report_catalog. "
            "Vérifier MinIO (lake/bronze/warcraftlogs/ingestion_state)."
        )

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

    eligible = [
        f
        for f in fight_rows
        if f.get("start_time_ms") is not None
        and f.get("end_time_ms") is not None
        and int(f["start_time_ms"]) < int(f["end_time_ms"])
    ]
    total_fights = len(eligible)
    print(f"  {report_code} : {total_fights} fights à traiter (sur {len(fight_rows)} total).")

    for idx, fight in enumerate(eligible, 1):
        start_ms = int(fight["start_time_ms"])
        end_ms = int(fight["end_time_ms"])
        fight_id = int(fight["fight_id"])
        fight_name = fight.get("fight_name") or f"#{fight_id}"
        print(f"  fight {idx}/{total_fights} id={fight_id} {fight_name}")

        metrics, raw_tables = fetch_fight_tables(report_code, start_ms, end_ms)
        export_fight_tables_raw_to_bronze(report_code, fight_id, raw_tables)
        for metric_rows in metrics.values():
            for row in metric_rows:
                stat_rows.append(
                    {
                        "report_code": report_code,
                        "fight_id": fight_id,
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

    return {
        "fights": len(fight_rows),
        "stats": len(stat_rows),
        **{f"bronze_{k}": v for k, v in bronze_counts.items()},
    }


def ingest_reports_incremental(**_) -> int:
    """Ingère les reports pending jusqu'au quota API (arrêt propre, pas d'erreur)."""
    if not _delta_table_readable(BRONZE_PATHS["ingestion_state"]):
        print("ingestion_state absent — sync catalogue avant ingestion.")
        sync_report_catalog()

    refresh_rate_limit_data(force=True)
    batch_max = _ingest_batch_size()
    pending = list_pending_report_codes(limit=batch_max)
    if not pending:
        print("Aucun report en attente dans le lake.")
        return 0

    measured = median_ingest_points()
    est = estimated_report_cost()
    quota = refresh_rate_limit_data()
    if quota:
        print(
            f"Quota WCL : {quota['pointsSpentThisHour']}/{quota['limitPerHour']} pts "
            f"(reset {quota['pointsResetIn']} s). "
            f"Coût estimé/report : ~{est} pts"
            + (f" (médiane mesurée {int(measured)} pts)" if measured else " (fallback config)")
        )

    processed = 0
    print(f"{len(pending)} reports candidats (max {batch_max} par run).")
    for idx, report_code in enumerate(pending, 1):
        allowed, reason = quota_allows_next_report()
        if not allowed:
            print(f"Quota atteint — arrêt propre avant report {idx}/{len(pending)} ({reason}).")
            break

        quota_before = refresh_rate_limit_data(force=True)
        spent_before = quota_before["pointsSpentThisHour"] if quota_before else None
        print(f"[{idx}/{len(pending)}] Ingestion {report_code} — {reason}")

        try:
            summary = _ingest_single_report(report_code)
            ingest_pts = measure_report_points_delta(spent_before)
            mark_ingestion_ok(report_code, ingest_points=ingest_pts)
            processed += 1
            pts_msg = f", points={ingest_pts}" if ingest_pts is not None else ""
            if quota_before and ingest_pts is not None:
                pts_msg += f" ({spent_before}→{spent_before + ingest_pts}/{quota_before['limitPerHour']})"
            print(f"OK {report_code} : {summary}{pts_msg}")
        except Exception as exc:
            mark_ingestion_error(report_code, str(exc))
            print(f"ERREUR {report_code} : {exc}")
            if _is_rate_limit_error(exc):
                refresh_rate_limit_data(force=True)
                print("Quota / 4xx API — arrêt propre du batch (reports OK conservés).")
                break

    print(f"Ingestion bronze terminée : {processed}/{len(pending)} reports.")
    return processed

"""Ingestion incrémentale WCL : 1 report → Postgres + bronze Delta, commit par log."""

from __future__ import annotations

import json
import os
import re
from typing import Any

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

from warcraftlogs_common import (
    fetch_all_guild_and_member_reports,
    fetch_fight_tables,
    fetch_report_fights,
    fight_rows_from_report,
    guild_url_from_env,
)
from warcraftlogs_db import (
    FIGHT_UPSERT_SQL,
    PLAYER_STAT_UPSERT_SQL,
    REPORT_UPSERT_SQL,
    ensure_tables,
    list_reports_pending_bronze,
    mark_report_bronze_error,
    mark_report_bronze_synced,
    upsert_report_catalog_row,
)
from warcraftlogs_lake import export_report_raw_to_bronze, export_report_tables_to_bronze

CONN_ID = "DATA-DB"
_RATE_LIMIT_RE = re.compile(r"\b(429|4\d{2})\b|rate.?limit", re.I)


def _is_rate_limit_error(exc: BaseException) -> bool:
    return bool(_RATE_LIMIT_RE.search(str(exc)))


def _hook_df(hook: PostgresHook, sql: str, report_code: str) -> pd.DataFrame:
    return hook.get_pandas_df(sql, parameters=(report_code,))


def sync_report_catalog(conn_id: str = CONN_ID, **_) -> int:
    """Catalogue API → Postgres uniquement (léger, reprise safe)."""
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    try:
        ensure_tables(conn)
        payload = fetch_all_guild_and_member_reports(guild_url_from_env())
        guild = payload.get("guild") or {}
        keys = payload["query"]
        reports = payload.get("reports") or []
        if not reports:
            print("Catalogue WCL vide.")
            return 0

        count = 0
        for report in reports:
            upsert_report_catalog_row(conn, report, guild, keys)
            count += 1
        conn.commit()
        print(
            f"Catalogue : {count} reports (guilde={payload.get('guild_reports_count')} "
            f"perso={payload.get('personal_reports_count')})"
        )
        return count
    finally:
        conn.close()


def _ingest_single_report(
    conn: Any,
    hook: PostgresHook,
    report_code: str,
) -> dict[str, int]:
    """Pipeline complet pour un report : API → PG → Delta bronze."""
    api_report = fetch_report_fights(report_code)
    export_report_raw_to_bronze(report_code, api_report)

    fight_rows = fight_rows_from_report(report_code, api_report)
    with conn.cursor() as cur:
        if fight_rows:
            cur.executemany(
                FIGHT_UPSERT_SQL,
                [
                    (
                        row["report_code"],
                        row["fight_id"],
                        row["encounter_id"],
                        row["fight_name"],
                        row["start_time_ms"],
                        row["end_time_ms"],
                        row["duration_ms"],
                        row["kill"],
                        row["difficulty"],
                        row["size"],
                        row["boss_percentage"],
                        row["keystone_level"],
                        row["keystone_time_ms"],
                        row["is_boss"],
                    )
                    for row in fight_rows
                ],
            )
    conn.commit()

    stat_count = 0
    for fight in fight_rows:
        start_ms = fight.get("start_time_ms")
        end_ms = fight.get("end_time_ms")
        if start_ms is None or end_ms is None or int(start_ms) >= int(end_ms):
            continue

        metrics = fetch_fight_tables(report_code, int(start_ms), int(end_ms))
        stat_rows = []
        for metric_rows in metrics.values():
            for row in metric_rows:
                stat_rows.append(
                    (
                        report_code,
                        fight["fight_id"],
                        row["player_name"],
                        row["metric"],
                        row.get("player_id"),
                        row.get("class_name"),
                        row.get("spec_name"),
                        row.get("total_amount"),
                        row.get("active_time_ms"),
                        row.get("rate_per_sec"),
                        json.dumps(row.get("extra") or {}),
                    )
                )

        if not stat_rows:
            continue

        with conn.cursor() as cur:
            cur.executemany(PLAYER_STAT_UPSERT_SQL, stat_rows)
        conn.commit()
        stat_count += len(stat_rows)

    reports_df = _hook_df(
        hook,
        "SELECT * FROM public.wcl_guild_reports WHERE report_code = %s",
        report_code,
    )
    fights_df = _hook_df(
        hook,
        "SELECT * FROM public.wcl_fights WHERE report_code = %s",
        report_code,
    )
    stats_df = _hook_df(
        hook,
        "SELECT * FROM public.wcl_fight_player_stats WHERE report_code = %s",
        report_code,
    )
    bronze_counts = export_report_tables_to_bronze(
        report_code,
        reports_df,
        fights_df,
        stats_df,
    )
    mark_report_bronze_synced(conn, report_code)
    conn.commit()

    return {
        "fights": len(fight_rows),
        "stats": stat_count,
        **{f"bronze_{k}": v for k, v in bronze_counts.items()},
    }


def ingest_reports_incremental(conn_id: str = CONN_ID, **_) -> int:
    """Ingère les reports non synchronisés bronze, un par un (checkpoint par log)."""
    hook = PostgresHook(postgres_conn_id=conn_id)
    conn = hook.get_conn()
    batch_limit = int(os.environ.get("WCL_INGEST_BATCH_SIZE", "50"))
    processed = 0

    try:
        ensure_tables(conn)
        pending = list_reports_pending_bronze(conn, limit=batch_limit)
        if not pending:
            print("Aucun report en attente de bronze.")
            return 0

        print(f"{len(pending)} reports à ingérer (batch max {batch_limit}).")
        for report_code in pending:
            try:
                summary = _ingest_single_report(conn, hook, report_code)
                processed += 1
                print(f"OK {report_code} : {summary}")
            except Exception as exc:
                conn.rollback()
                mark_report_bronze_error(conn, report_code, str(exc)[:2000])
                conn.commit()
                print(f"ERREUR {report_code} : {exc}")
                if _is_rate_limit_error(exc):
                    print("Quota / 4xx API — arrêt du batch (reports déjà OK conservés).")
                    break
        print(f"Ingestion incrémentale terminée : {processed}/{len(pending)} reports.")
        return processed
    finally:
        conn.close()

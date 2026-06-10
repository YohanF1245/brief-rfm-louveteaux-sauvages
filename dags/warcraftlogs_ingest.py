"""Ingestion WCL : API → bronze Delta (MinIO), données BRUTES uniquement.

Par report :
  1. ``reports_raw`` + ``fights``      — métadonnées (1 appel API)
  2. ``master_info/actors/abilities``  — masterData, mapping id → acteur (1 appel)
  3. ``player_details``                — specs / ilvl / talents / gear (1 appel)
  4. ``events``                        — log brut complet ``dataType: All``,
     paginé sur TOUTE la plage du report (boss + trash + inter-pulls),
     flush Delta par page de 10 000 events (RAM constante).

Aucune table agrégée WCL (``report.table``) n'est appelée : tout est
recalculable depuis ``events`` + ``master_actors``. Voir docs/wcl_bronze.md.
"""

from __future__ import annotations

import gc
import os
import re
from typing import Any

import pandas as pd

from warcraftlogs_common import (
    estimated_report_cost,
    event_rows_from_page,
    fetch_all_guild_and_member_reports,
    fetch_report_fights,
    fetch_report_master_data,
    fetch_report_player_details,
    fight_rows_from_report,
    guild_url_from_env,
    iter_report_events_pages,
    master_data_rows,
    measure_report_points_delta,
    player_details_rows,
    quota_allows_next_report,
    refresh_rate_limit_data,
)
from warcraftlogs_lake import (
    BRONZE_PATHS,
    EVENTS_INT_COLUMNS,
    append_rows_to_bronze,
    bulk_upsert_catalog_state,
    delete_report_rows,
    export_master_data_to_bronze,
    export_player_details_to_bronze,
    export_report_raw_to_bronze,
    export_report_tables_to_bronze,
    list_pending_report_codes,
    load_catalog_context,
    mark_ingestion_error,
    mark_ingestion_ok,
    median_ingest_points,
    repair_ingestion_state_schema,
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
    repair_ingestion_state_schema()
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


def _report_time_range_ms(api_report: dict[str, Any]) -> float:
    """Durée totale du report en ms (les events utilisent des times RELATIFS)."""
    start = api_report.get("startTime")
    end = api_report.get("endTime")
    if start is None or end is None:
        return 0.0
    return max(0.0, float(end) - float(start))


def _ingest_single_report(report_code: str) -> dict[str, int]:
    """Pipeline complet données brutes : API → bronze Delta (commit par report)."""
    catalog = load_catalog_context(report_code)
    api_report = fetch_report_fights(report_code)
    export_report_raw_to_bronze(report_code, api_report)

    fight_rows = fight_rows_from_report(report_code, api_report)
    range_end_ms = _report_time_range_ms(api_report)
    print(f"  {report_code} : {len(fight_rows)} fights, plage events 0 → {int(range_end_ms)} ms.")

    # 1) masterData : référentiel acteurs / abilities (requis pour joindre les events)
    master = fetch_report_master_data(report_code)
    info_row, actor_rows, ability_rows = master_data_rows(report_code, master)
    master_counts = export_master_data_to_bronze(report_code, info_row, actor_rows, ability_rows)
    print(
        f"  masterData : {master_counts['master_actors']} acteurs, "
        f"{master_counts['master_abilities']} abilities."
    )
    del master, actor_rows, ability_rows
    gc.collect()

    # 2) playerDetails : specs / ilvl / talents / gear (toute la plage du report)
    details_count = 0
    if range_end_ms > 0:
        try:
            details_payload = fetch_report_player_details(report_code, 0.0, range_end_ms)
            details_count = export_player_details_to_bronze(
                report_code, player_details_rows(report_code, details_payload)
            )
            del details_payload
        except Exception as exc:
            # Non bloquant : certains reports (vides, archivés) n'ont pas de playerDetails.
            print(f"  playerDetails indisponible : {exc}")
    print(f"  playerDetails : {details_count} lignes joueur.")

    # 3) events bruts : TOUTE la plage du report (boss + trash + inter-pulls),
    #    dataType: All, flush Delta par page (RAM constante, reprise par report)
    total_events = 0
    if range_end_ms > 0:
        delete_report_rows(BRONZE_PATHS["events"], report_code)
        for page_index, events in iter_report_events_pages(report_code, 0.0, range_end_ms):
            rows = event_rows_from_page(report_code, page_index, events)
            total_events += append_rows_to_bronze(
                BRONZE_PATHS["events"], rows, int_columns=EVENTS_INT_COLUMNS
            )
            print(f"  events page {page_index} : +{len(rows)} (total {total_events})")
            del events, rows
            gc.collect()

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
    )
    bronze_counts.update(master_counts)
    bronze_counts["player_details"] = details_count
    bronze_counts["events"] = total_events

    return {
        "fights": len(fight_rows),
        "events": total_events,
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

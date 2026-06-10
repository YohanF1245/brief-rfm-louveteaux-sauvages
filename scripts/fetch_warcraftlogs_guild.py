#!/usr/bin/env python3
"""Test local : récupère guilde + reports + fights Warcraft Logs (API v2)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))

from warcraftlogs_common import (  # noqa: E402
    DEFAULT_GUILD_URL,
    fetch_all_guild_and_member_reports,
    fetch_all_guild_members,
    fetch_all_guild_reports,
    fetch_events_page,
    fetch_guild_reports,
    fetch_report_fights,
    fetch_report_master_data,
    fetch_report_player_details,
    guild_url_from_env,
    master_data_rows,
    player_details_rows,
    user_ids_from_env,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch Warcraft Logs guild via API v2")
    parser.add_argument("--url", default=None, help="URL page guilde WCL")
    parser.add_argument("--limit", type=int, default=10, help="Nombre max de reports (mode page unique)")
    parser.add_argument("--all", action="store_true", help="Paginer tous les reports publics")
    parser.add_argument(
        "--members",
        action="store_true",
        help="Inclure logs personnels publics (WCL_USER_IDS)",
    )
    parser.add_argument("--fights", metavar="REPORT_CODE", help="Afficher les fights d'un report")
    parser.add_argument(
        "--raw",
        metavar="REPORT_CODE",
        help="Données brutes d'un report : masterData + playerDetails + 1ère page events",
    )
    parser.add_argument("--roster", action="store_true", help="Afficher le roster guilde (guild.members)")
    parser.add_argument("--json", action="store_true", help="Sortie JSON brute")
    args = parser.parse_args()

    guild_url = args.url or guild_url_from_env() or DEFAULT_GUILD_URL

    if args.roster:
        data = fetch_all_guild_members(guild_url)
        if args.json:
            print(json.dumps(data, indent=2, ensure_ascii=False))
        else:
            guild = data.get("guild") or {}
            print(f"Roster {guild.get('name')} : {len(data.get('rows') or [])} membres")
            for row in (data.get("rows") or [])[:30]:
                print(
                    f"  - {row.get('character_name')} "
                    f"guid={row.get('player_guid')} "
                    f"({row.get('class_name')}) rank={row.get('guild_rank')}"
                )
        return 0

    if args.fights:
        report = fetch_report_fights(args.fights)
        if args.json:
            print(json.dumps(report, indent=2, ensure_ascii=False))
        else:
            print(f"Report {args.fights} : {report.get('title')}")
            for fight in report.get("fights") or []:
                boss = "boss" if fight.get("encounterID") else "trash"
                kill = "kill" if fight.get("kill") else "wipe"
                print(
                    f"  - #{fight.get('id')} {fight.get('name')} "
                    f"[{boss}/{kill}] {fight.get('startTime')}->{fight.get('endTime')}"
                )
        return 0

    if args.raw:
        code = args.raw
        report = fetch_report_fights(code)
        range_end = float(report.get("endTime", 0)) - float(report.get("startTime", 0))

        master = fetch_report_master_data(code)
        info_row, actor_rows, ability_rows = master_data_rows(code, master)

        details_payload = fetch_report_player_details(code, 0.0, range_end)
        detail_rows = player_details_rows(code, details_payload)

        page = fetch_events_page(code, 0.0, range_end)
        events = page.get("data") or []

        if args.json:
            print(
                json.dumps(
                    {
                        "master_info": info_row,
                        "actors_sample": actor_rows[:10],
                        "player_details": detail_rows,
                        "events_sample": events[:20],
                        "nextPageTimestamp": page.get("nextPageTimestamp"),
                    },
                    indent=2,
                    ensure_ascii=False,
                )
            )
            return 0

        print(f"Report {code} : {report.get('title')} ({len(report.get('fights') or [])} fights)")
        print(
            f"masterData : {len(actor_rows)} acteurs, {len(ability_rows)} abilities, "
            f"lang={info_row.get('lang')}"
        )
        players = [a for a in actor_rows if a.get("actor_type") == "Player"]
        for actor in players[:15]:
            print(f"  - [{actor['actor_id']}] {actor['name']} ({actor['sub_type']}) {actor.get('server') or ''}")
        print(f"playerDetails : {len(detail_rows)} lignes joueur/rôle")
        for row in detail_rows[:10]:
            print(
                f"  - {row['player_name']} [{row['role']}] {row['class_name']} "
                f"ilvl={row.get('max_item_level')} guid={row.get('player_guid')}"
            )
        types: dict[str, int] = {}
        for event in events:
            types[event.get("type") or "?"] = types.get(event.get("type") or "?", 0) + 1
        print(f"events page 1 : {len(events)} events, next={page.get('nextPageTimestamp')}")
        for etype, count in sorted(types.items(), key=lambda x: -x[1]):
            print(f"  - {etype}: {count}")
        return 0

    if args.members or (args.all and user_ids_from_env()):
        data = fetch_all_guild_and_member_reports(guild_url)
    elif args.all:
        data = fetch_all_guild_reports(guild_url)
    else:
        data = fetch_guild_reports(guild_url, limit=args.limit)

    if args.json:
        print(json.dumps(data, indent=2, ensure_ascii=False))
        return 0

    guild = data.get("guild") or {}
    print(f"Guilde : {guild.get('name')} (id={guild.get('id')})")
    print(f"Serveur : {(guild.get('server') or {}).get('name')}")
    if data.get("guild_reports_count") is not None:
        print(
            f"Reports : {len(data.get('reports') or [])} "
            f"(guilde={data.get('guild_reports_count')} personnel={data.get('personal_reports_count')})"
        )
    else:
        print(f"Reports ({len(data.get('reports') or [])} / total API {data.get('reports_total')}) :")
    for report in data.get("reports") or []:
        zone = (report.get("zone") or {}).get("name", "?")
        src = report.get("_log_source", "guild")
        print(f"  - [{src}] {report.get('code')} | {report.get('title')} | {zone}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

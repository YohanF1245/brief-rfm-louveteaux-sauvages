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
    fetch_fight_tables,
    fetch_guild_reports,
    fetch_report_fights,
    guild_url_from_env,
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
    parser.add_argument("--stats", metavar="REPORT_CODE", help="Stats raid d'un report (1er boss fight)")
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

    if args.stats:
        report = fetch_report_fights(args.stats)
        boss_fights = [f for f in (report.get("fights") or []) if f.get("encounterID")]
        if not boss_fights:
            print("Aucun fight boss dans ce report.")
            return 1
        fight = boss_fights[0]
        metrics, _ = fetch_fight_tables(args.stats, int(fight["startTime"]), int(fight["endTime"]))
        if args.json:
            print(json.dumps({"fight": fight, "metrics": metrics}, indent=2, ensure_ascii=False))
        else:
            print(f"Fight {fight.get('name')} (id={fight.get('id')})")
            for metric, rows in metrics.items():
                print(f"  {metric}:")
                for row in rows[:5]:
                    print(
                        f"    - {row['player_name']} ({row.get('class_name')}) "
                        f"{row['rate_per_sec']:.0f}/s total={row['total_amount']}"
                    )
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

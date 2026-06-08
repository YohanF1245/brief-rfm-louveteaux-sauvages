#!/usr/bin/env python3
"""Debug : combien de reports publics WCL pour la guilde ?"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))

from warcraftlogs_common import (  # noqa: E402
    fetch_all_guild_reports,
    fetch_guild_reports_page,
    graphql_request,
    guild_url_from_env,
    parse_guild_url,
)

REPORTS_BY_GUILD_ID_QUERY = """
query ReportsByGuildId($guildId: Int!, $limit: Int!, $page: Int!) {
  guildData {
    guild(id: $guildId) {
      id
      name
      server { name slug }
    }
  }
  reportData {
    reports(guildID: $guildId, limit: $limit, page: $page) {
      total
      has_more_pages
      data {
        code
        title
        visibility
        zone { name }
        owner { name }
      }
    }
  }
}
"""


def main() -> int:
    url = guild_url_from_env()
    keys = parse_guild_url(url)
    print("Guilde parsée :", json.dumps(keys, ensure_ascii=False))

    page = fetch_guild_reports_page(url, page=1, limit=100)
    print(
        f"reportData.reports : {len(page['reports'])} / total={page['reports_total']} "
        f"has_more={page['has_more_pages']} last_page={page.get('last_page')}"
    )
    for r in page.get("reports") or []:
        zone = (r.get("zone") or {}).get("name", "?")
        print(f"  - {r.get('code')} | {r.get('title')} | {zone}")

    guild = (page.get("guild") or {})
    guild_id = guild.get("id")
    print(f"\nguildData.guild.id = {guild_id} name = {guild.get('name')}")

    if guild_id:
        by_id = graphql_request(
            REPORTS_BY_GUILD_ID_QUERY,
            {"guildId": int(guild_id), "limit": 100, "page": 1},
        )
        api_reports = (by_id.get("reportData") or {}).get("reports") or {}
        print(
            f"reportData.reports(guildID) : {len(api_reports.get('data') or [])} / "
            f"total={api_reports.get('total')} has_more={api_reports.get('has_more_pages')}"
        )
        for r in api_reports.get("data") or []:
            zone = (r.get("zone") or {}).get("name", "?")
            print(
                f"  [{r.get('visibility', '?')}] {r.get('code')} | {r.get('title')} | {zone} | "
                f"owner={(r.get('owner') or {}).get('name')}"
            )

    all_payload = fetch_all_guild_reports(url)
    print(f"\nfetch_all_guild_reports : {len(all_payload['reports'])} reports")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

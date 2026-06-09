"""Sync roster guilde WCL → bronze Delta (``guild_roster``)."""

from __future__ import annotations

from warcraftlogs_common import fetch_all_guild_members, guild_url_from_env
from warcraftlogs_lake import write_guild_roster_to_bronze


def sync_guild_roster(**_) -> int:
    """Récupère ``guild.members`` (API v2) et écrase la bronze roster."""
    payload = fetch_all_guild_members(guild_url_from_env())
    rows = payload.get("rows") or []
    guild = payload.get("guild") or {}
    print(
        f"Roster API : {guild.get('name')} — {len(rows)} personnages "
        f"(total API {payload.get('members_total')})"
    )
    return write_guild_roster_to_bronze(rows)

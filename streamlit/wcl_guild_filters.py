"""Filtres guilde WCL partagés entre pages Streamlit."""

from __future__ import annotations

GUILD_NAME_LIKE = "%nightmares asylum%"


def guild_report_clause(guild_only: bool) -> str:
    """Filtre les logs tagués Nightmares Asylum (niveau report)."""
    return "is_nightmares_asylum_report = 1" if guild_only else "1 = 1"


def guild_player_clause(guild_only: bool) -> str:
    """Restreint aux membres du roster guilde (``is_guild_member`` en gold)."""
    if not guild_only:
        return "1 = 1"
    return "is_guild_member = 1"

"""Filtres guilde WCL partagés entre pages Streamlit."""

from __future__ import annotations

GUILD_NAME_LIKE = "%nightmares asylum%"


def guild_report_clause(guild_only: bool) -> str:
    """Filtre les logs dont la guilde du report correspond à Nightmares Asylum."""
    if not guild_only:
        return "1 = 1"
    return f"lower(coalesce(report_guild_name, '')) LIKE '{GUILD_NAME_LIKE}'"


def guild_player_clause(guild_only: bool) -> str:
    """Restreint aux membres du roster guilde (``is_guild_member`` en gold)."""
    if not guild_only:
        return "1 = 1"
    return "is_guild_member = 1"

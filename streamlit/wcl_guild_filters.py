"""Filtres guilde WCL partagés entre pages Streamlit."""

from __future__ import annotations

GUILD_NAME_LIKE = "%nightmares asylum%"


def guild_report_clause(guild_only: bool) -> str:
    """Filtre les logs tagués Nightmares Asylum (niveau report)."""
    return "is_nightmares_asylum = 1" if guild_only else "1 = 1"


def guild_member_subquery() -> str:
    """
    Roster dérivé : persos vus dans au moins un log guilde officiel WCL.
    Exclut les noms vides et ``unknown`` (buffs sans joueur).
    """
    return f"""
    SELECT DISTINCT m.player_name
    FROM silver.wcl_player_fight_metrics AS m
    INNER JOIN silver.wcl_reports AS r ON m.report_code = r.report_code
    WHERE r.log_source = 'guild'
      AND lower(coalesce(r.guild_name, '')) LIKE '{GUILD_NAME_LIKE}'
      AND m.metric IN ('dps', 'summary')
      AND m.player_name != ''
      AND lower(m.player_name) != 'unknown'
    """


def guild_player_clause(guild_only: bool) -> str:
    """Restreint aux membres du roster guilde quand la case est cochée."""
    if not guild_only:
        return "1 = 1"
    return f"player_name IN ({guild_member_subquery()})"

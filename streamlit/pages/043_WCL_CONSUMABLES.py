"""Uptime consommables raid — food, flacon, huile, potions (gold.wcl_consumables_viz)."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from ch_utils import _esc, ch_query, ch_scalar
from wcl_guild_filters import guild_player_clause, guild_report_clause
from wcl_streamlit_helpers import column_values

VIZ_TABLE = "wcl_consumables_viz"

# Types buffs reconnus par le gold (``weapon_buff`` = huiles / pierres à aiguiser)
BUFF_TYPES = ("food", "flask", "weapon_buff", "augment_rune")
CONSUMABLE_LABELS = {
    "food": "food",
    "flask": "flacon",
    "weapon_buff": "huile/arme",
    "augment_rune": "rune",
    "potion": "potion",
    "healthstone": "healthstone",
}

st.set_page_config(page_title="WCL — Consommables raid", layout="wide")

st.title("Warcraft Logs — Consommables raid")
st.caption(
    "Uptime food / flacon / huile et potions en combat boss. "
    f"Source : `gold.{VIZ_TABLE}`."
)


@st.cache_data(ttl=120)
def load_raids(guild_only: bool) -> list[str]:
    df = ch_query(
        f"""
        SELECT DISTINCT raid_or_dungeon
        FROM {VIZ_TABLE}
        WHERE {guild_report_clause(guild_only)}
          AND {guild_player_clause(guild_only)}
          AND is_boss = 1
          AND raid_or_dungeon != ''
        ORDER BY raid_or_dungeon
        """
    )
    return column_values(df, "raid_or_dungeon")


@st.cache_data(ttl=120)
def load_bosses(guild_only: bool, raid: str) -> list[str]:
    df = ch_query(
        f"""
        SELECT DISTINCT boss_name
        FROM {VIZ_TABLE}
        WHERE {guild_report_clause(guild_only)}
          AND {guild_player_clause(guild_only)}
          AND is_boss = 1
          AND raid_or_dungeon = '{_esc(raid)}'
        ORDER BY boss_name
        """
    )
    return column_values(df, "boss_name")


@st.cache_data(ttl=60)
def load_consumables(
    guild_only: bool,
    raid: str,
    boss: str | None,
    difficulty_labels: tuple[str, ...],
) -> pd.DataFrame:
    filters = [
        guild_report_clause(guild_only),
        guild_player_clause(guild_only),
        "is_boss = 1",
        f"raid_or_dungeon = '{_esc(raid)}'",
        "lower(player_name) != 'unknown'",
    ]
    if boss:
        filters.append(f"boss_name = '{_esc(boss)}'")
    if difficulty_labels:
        quoted = ", ".join(f"'{_esc(d)}'" for d in difficulty_labels)
        filters.append(f"difficulty_label IN ({quoted})")

    df = ch_query(
        f"""
        SELECT
            report_start_at,
            player_name,
            report_guild_name AS guild_name,
            boss_name,
            raid_or_dungeon,
            difficulty_label,
            consumable_type,
            consumable_name,
            present_at_pull,
            uptime_sec,
            uptime_pct,
            casts AS potion_uses,
            fight_duration_sec
        FROM {VIZ_TABLE}
        WHERE {" AND ".join(filters)}
        ORDER BY report_start_at, player_name, consumable_type
        """
    )
    if df.empty:
        return df

    df["report_start_at"] = pd.to_datetime(df["report_start_at"], utc=True)
    df["uptime_pct"] = pd.to_numeric(df["uptime_pct"], errors="coerce")
    df["uptime_sec"] = pd.to_numeric(df["uptime_sec"], errors="coerce")
    df["potion_uses"] = pd.to_numeric(df["potion_uses"], errors="coerce")
    df["present_at_pull"] = pd.to_numeric(df["present_at_pull"], errors="coerce")
    df["consumable_label"] = df["consumable_type"].map(CONSUMABLE_LABELS).fillna(
        df["consumable_type"]
    )
    return df


try:
    row_count = ch_scalar(f"SELECT count() FROM {VIZ_TABLE}")
except RuntimeError as exc:
    st.error(f"Connexion ClickHouse impossible : {exc}")
    st.stop()

if row_count == 0:
    st.warning(
        f"La table `gold.{VIZ_TABLE}` est vide. "
        "Lance **warcraftlogs_silver** puis **warcraftlogs_gold** dans Airflow."
    )
    st.stop()

guild_only = st.checkbox("Nightmares Asylum uniquement", value=True)
raids = load_raids(guild_only)
if not raids:
    if guild_only and row_count > 0:
        st.warning(
            "Des lignes existent en gold, mais le filtre **membres guilde** ne retourne rien. "
            "Décoche « Nightmares Asylum » ou synchronise le roster (**warcraftlogs_guild_roster**)."
        )
    else:
        st.info("Aucune donnée consommables pour ce filtre.")
    st.stop()

col_raid, col_boss, col_diff = st.columns([2, 1, 1])

with col_raid:
    raid = st.selectbox("Raid", raids)

bosses = load_bosses(guild_only, raid)
with col_boss:
    boss_choice = st.selectbox("Boss", ["Tous les boss"] + bosses)
    boss_filter = None if boss_choice == "Tous les boss" else boss_choice

with col_diff:
    diff_df = ch_query(
        f"""
        SELECT DISTINCT difficulty_label
        FROM {VIZ_TABLE}
        WHERE {guild_report_clause(guild_only)}
          AND {guild_player_clause(guild_only)}
          AND is_boss = 1
          AND raid_or_dungeon = '{_esc(raid)}'
        ORDER BY difficulty_label
        """
    )
    diff_labels = diff_df["difficulty_label"].tolist() if not diff_df.empty else []
    difficulty_selected = st.multiselect("Difficulté", diff_labels, default=diff_labels)

df = load_consumables(guild_only, raid, boss_filter, tuple(difficulty_selected))
if df.empty:
    st.info("Aucune ligne pour ces filtres.")
    st.stop()

buffs = df[df["consumable_type"].isin(BUFF_TYPES)].copy()
potions = df[df["consumable_type"] == "potion"].copy()

st.subheader("Uptime consommables (%)")
if buffs.empty:
    st.info("Aucun buff food / flacon / huile détecté pour ces filtres.")
else:
    uptime_avg = (
        buffs.groupby(["player_name", "consumable_label"], as_index=False)["uptime_pct"]
        .max()
        .rename(columns={"uptime_pct": "uptime_pct_max"})
    )
    fig_heat = px.density_heatmap(
        uptime_avg,
        x="consumable_label",
        y="player_name",
        z="uptime_pct_max",
        color_continuous_scale="RdYlGn",
        range_color=[0, 100],
        labels={
            "consumable_label": "Type",
            "player_name": "Joueur",
            "uptime_pct_max": "Uptime % (max)",
        },
        title="Uptime max par joueur et type de consommable",
    )
    fig_heat.update_layout(height=max(400, 28 * uptime_avg["player_name"].nunique()))
    st.plotly_chart(fig_heat, use_container_width=True)

    fig_ts = px.line(
        buffs,
        x="report_start_at",
        y="uptime_pct",
        color="player_name",
        facet_row="consumable_label",
        markers=True,
        hover_data=["boss_name", "consumable_name", "difficulty_label", "uptime_sec", "present_at_pull"],
        labels={
            "report_start_at": "Date",
            "uptime_pct": "Uptime %",
            "player_name": "Joueur",
        },
        title="Évolution uptime food / flacon / huile",
    )
    fig_ts.update_layout(height=700)
    st.plotly_chart(fig_ts, use_container_width=True)

st.subheader("Potions en combat")
if potions.empty:
    st.info("Aucune potion détectée pour ces filtres.")
else:
    pot_agg = (
        potions.groupby(["player_name", "boss_name"], as_index=False)["potion_uses"]
        .sum()
        .sort_values("potion_uses", ascending=False)
    )
    fig_pot = px.bar(
        pot_agg,
        x="player_name",
        y="potion_uses",
        color="boss_name",
        barmode="group",
        labels={"player_name": "Joueur", "potion_uses": "Potions", "boss_name": "Boss"},
        title="Potions consommées par joueur et boss",
    )
    fig_pot.update_layout(height=420)
    st.plotly_chart(fig_pot, use_container_width=True)

with st.expander("Détail brut"):
    show = df.copy()
    show["report_start_at"] = show["report_start_at"].dt.strftime("%Y-%m-%d %H:%M")
    st.dataframe(show.sort_values("report_start_at", ascending=False), hide_index=True)

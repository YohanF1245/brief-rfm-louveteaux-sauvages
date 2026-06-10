"""Consommables en combat — gold.wcl_consumables_viz."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from ch_utils import _esc, ch_query, ch_scalar
from wcl_guild_filters import guild_player_clause, guild_report_clause
from wcl_streamlit_helpers import column_values

VIZ_TABLE = "wcl_consumables_viz"

BUFF_TYPES = frozenset({"food", "flask", "weapon_buff", "augment_rune"})
CAST_TYPES = frozenset({"potion", "healthstone"})
TYPE_LABELS = {
    "food": "Food",
    "flask": "Flacon",
    "weapon_buff": "Huile / arme",
    "augment_rune": "Rune",
    "potion": "Potion",
    "healthstone": "Pierre de soins",
}

st.set_page_config(page_title="WCL — Consommables", layout="wide")

st.title("Warcraft Logs — Consommables")
st.caption(
    "Uptime des buffs (food, flacon, huile, rune) et casts (potions, pierres de soins) "
    f"par joueur et par fight. Source : `gold.{VIZ_TABLE}`."
)


def content_clause(mode: str) -> str:
    if mode == "Raid":
        return "coalesce(keystone_level, 0) = 0"
    if mode == "M+":
        return "keystone_level > 0"
    return "1 = 1"


@st.cache_data(ttl=120)
def load_zones(guild_only: bool, content_mode: str, boss_only: bool) -> list[str]:
    filters = [
        guild_report_clause(guild_only),
        guild_player_clause(guild_only),
        content_clause(content_mode),
        "raid_or_dungeon != ''",
    ]
    if boss_only:
        filters.append("is_boss = 1")

    df = ch_query(
        f"""
        SELECT DISTINCT raid_or_dungeon
        FROM {VIZ_TABLE}
        WHERE {" AND ".join(filters)}
        ORDER BY raid_or_dungeon
        """
    )
    return column_values(df, "raid_or_dungeon")


@st.cache_data(ttl=120)
def load_bosses(
    guild_only: bool,
    content_mode: str,
    boss_only: bool,
    zone: str,
) -> list[str]:
    filters = [
        guild_report_clause(guild_only),
        guild_player_clause(guild_only),
        content_clause(content_mode),
        f"raid_or_dungeon = '{_esc(zone)}'",
        "boss_name != ''",
    ]
    if boss_only:
        filters.append("is_boss = 1")

    df = ch_query(
        f"""
        SELECT DISTINCT boss_name
        FROM {VIZ_TABLE}
        WHERE {" AND ".join(filters)}
        ORDER BY boss_name
        """
    )
    return column_values(df, "boss_name")


@st.cache_data(ttl=60)
def load_consumables(
    guild_only: bool,
    content_mode: str,
    boss_only: bool,
    zone: str,
    boss: str | None,
    difficulty_labels: tuple[str, ...],
    kills_only: bool,
) -> pd.DataFrame:
    filters = [
        guild_report_clause(guild_only),
        guild_player_clause(guild_only),
        content_clause(content_mode),
        f"raid_or_dungeon = '{_esc(zone)}'",
        "lower(player_name) != 'unknown'",
        "player_name != ''",
    ]
    if boss_only:
        filters.append("is_boss = 1")
    if boss:
        filters.append(f"boss_name = '{_esc(boss)}'")
    if difficulty_labels:
        quoted = ", ".join(f"'{_esc(d)}'" for d in difficulty_labels)
        filters.append(f"difficulty_label IN ({quoted})")
    if kills_only:
        filters.append("is_kill = 1")

    df = ch_query(
        f"""
        SELECT
            report_start_at,
            report_date,
            report_code,
            report_guild_name,
            raid_or_dungeon,
            fight_id,
            boss_name,
            is_boss,
            is_kill,
            difficulty_label,
            keystone_level,
            fight_duration_sec,
            player_name,
            class_name,
            is_guild_member,
            consumable_type,
            consumable_name,
            present_at_pull,
            uptime_sec,
            uptime_pct,
            casts
        FROM {VIZ_TABLE}
        WHERE {" AND ".join(filters)}
        ORDER BY report_start_at, player_name, consumable_type
        """
    )
    if df.empty:
        return df

    df["report_start_at"] = pd.to_datetime(df["report_start_at"], utc=True)
    df["report_date"] = pd.to_datetime(df["report_date"])
    for col in ("uptime_pct", "uptime_sec", "casts", "present_at_pull", "keystone_level"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["type_label"] = df["consumable_type"].map(TYPE_LABELS).fillna(df["consumable_type"])
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

col_guild, col_content, col_boss_only = st.columns([1, 1, 1])
with col_guild:
    guild_only = st.checkbox("Nightmares Asylum uniquement", value=True)
with col_content:
    content_mode = st.radio("Contenu", ["Raid", "M+", "Tous"], horizontal=True)
with col_boss_only:
    boss_only = st.checkbox("Combats boss uniquement", value=True)

zones = load_zones(guild_only, content_mode, boss_only)
if not zones:
    if guild_only and row_count > 0:
        st.warning(
            "Des lignes existent en gold, mais aucun contenu ne correspond au filtre guilde. "
            "Décoche « Nightmares Asylum » ou relance **warcraftlogs_guild_roster** + **warcraftlogs_silver**."
        )
    else:
        st.info("Aucune donnée pour ce filtre.")
    st.stop()

col_zone, col_boss, col_diff, col_kill = st.columns([2, 1, 1, 1])
with col_zone:
    zone = st.selectbox("Zone", zones)

bosses = load_bosses(guild_only, content_mode, boss_only, zone)
with col_boss:
    boss_choice = st.selectbox("Boss / fight", ["Tous"] + bosses)
    boss_filter = None if boss_choice == "Tous" else boss_choice

with col_diff:
    diff_df = ch_query(
        f"""
        SELECT DISTINCT difficulty_label
        FROM {VIZ_TABLE}
        WHERE {guild_report_clause(guild_only)}
          AND {guild_player_clause(guild_only)}
          AND {content_clause(content_mode)}
          AND raid_or_dungeon = '{_esc(zone)}'
          {"AND is_boss = 1" if boss_only else ""}
        ORDER BY difficulty_label
        """
    )
    diff_labels = column_values(diff_df, "difficulty_label")
    difficulty_selected = st.multiselect("Difficulté", diff_labels, default=diff_labels)

with col_kill:
    kills_only = st.checkbox("Kills uniquement", value=False)

df = load_consumables(
    guild_only,
    content_mode,
    boss_only,
    zone,
    boss_filter,
    tuple(difficulty_selected),
    kills_only,
)
if df.empty:
    st.info("Aucune ligne pour ces filtres.")
    st.stop()

buffs = df[df["consumable_type"].isin(BUFF_TYPES)].copy()
potions = df[df["consumable_type"] == "potion"].copy()
healthstones = df[df["consumable_type"] == "healthstone"].copy()

m1, m2, m3, m4 = st.columns(4)
m1.metric("Joueurs", df["player_name"].nunique())
m2.metric("Fights", df[["report_code", "fight_id"]].drop_duplicates().shape[0])
m3.metric(
    "Uptime moyen buffs",
    f"{buffs['uptime_pct'].mean():.0f} %" if not buffs.empty else "—",
)
m4.metric(
    "Potions (total)",
    int(potions["casts"].sum()) if not potions.empty else 0,
)

st.subheader("Uptime des buffs")
if buffs.empty:
    st.info("Aucun buff food / flacon / huile / rune pour ces filtres.")
else:
    uptime_max = (
        buffs.groupby(["player_name", "type_label"], as_index=False)["uptime_pct"]
        .max()
        .rename(columns={"uptime_pct": "uptime_pct_max"})
    )
    fig_heat = px.density_heatmap(
        uptime_max,
        x="type_label",
        y="player_name",
        z="uptime_pct_max",
        color_continuous_scale="RdYlGn",
        range_color=[0, 100],
        labels={
            "type_label": "Type",
            "player_name": "Joueur",
            "uptime_pct_max": "Uptime % (max)",
        },
        title="Uptime max par joueur et type de buff",
    )
    fig_heat.update_layout(height=max(400, 28 * uptime_max["player_name"].nunique()))
    st.plotly_chart(fig_heat, use_container_width=True)

    fig_ts = px.line(
        buffs,
        x="report_start_at",
        y="uptime_pct",
        color="player_name",
        facet_row="type_label",
        markers=True,
        hover_data=[
            "boss_name",
            "consumable_name",
            "difficulty_label",
            "uptime_sec",
            "present_at_pull",
            "report_guild_name",
        ],
        labels={
            "report_start_at": "Date",
            "uptime_pct": "Uptime %",
            "player_name": "Joueur",
        },
        title="Évolution de l'uptime des buffs",
    )
    fig_ts.update_layout(height=700)
    st.plotly_chart(fig_ts, use_container_width=True)

st.subheader("Potions")
if potions.empty:
    st.info("Aucune potion pour ces filtres.")
else:
    pot_agg = (
        potions.groupby(["player_name", "boss_name"], as_index=False)["casts"]
        .sum()
        .sort_values("casts", ascending=False)
    )
    fig_pot = px.bar(
        pot_agg,
        x="player_name",
        y="casts",
        color="boss_name",
        barmode="group",
        labels={"player_name": "Joueur", "casts": "Potions", "boss_name": "Boss"},
        title="Potions consommées par joueur et boss",
    )
    fig_pot.update_layout(height=420)
    st.plotly_chart(fig_pot, use_container_width=True)

if not healthstones.empty:
    st.subheader("Pierres de soins")
    hs_agg = (
        healthstones.groupby(["player_name", "boss_name"], as_index=False)["casts"]
        .sum()
        .sort_values("casts", ascending=False)
    )
    fig_hs = px.bar(
        hs_agg,
        x="player_name",
        y="casts",
        color="boss_name",
        barmode="group",
        labels={"player_name": "Joueur", "casts": "Utilisations", "boss_name": "Boss"},
        title="Pierres de soins par joueur et boss",
    )
    fig_hs.update_layout(height=380)
    st.plotly_chart(fig_hs, use_container_width=True)

with st.expander("Détail brut"):
    show = df.copy()
    show["report_start_at"] = show["report_start_at"].dt.strftime("%Y-%m-%d %H:%M")
    st.dataframe(show.sort_values("report_start_at", ascending=False), hide_index=True)

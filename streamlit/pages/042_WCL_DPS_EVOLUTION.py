"""Évolution DPS multi-joueurs par raid / donjon (gold.wcl_fight_player_perf_viz)."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from ch_utils import _esc, ch_query, ch_scalar
from wcl_guild_filters import guild_player_clause, guild_report_clause
from wcl_streamlit_helpers import column_values

VIZ_TABLE = "wcl_fight_player_perf_viz"

st.set_page_config(page_title="WCL — Évolution DPS", layout="wide")

st.title("Warcraft Logs — Évolution DPS")
st.caption(
    f"Source : `gold.{VIZ_TABLE}` — pipeline "
    "`warcraftlogs_guild_nightmares` → `warcraftlogs_silver` → `warcraftlogs_gold`."
)


@st.cache_data(ttl=120)
def load_raids(guild_only: bool) -> list[str]:
    df = ch_query(
        f"""
        SELECT DISTINCT raid_or_dungeon
        FROM {VIZ_TABLE}
        WHERE {guild_report_clause(guild_only)}
          AND {guild_player_clause(guild_only)}
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
          AND raid_or_dungeon = '{_esc(raid)}'
          AND boss_name != ''
        ORDER BY boss_name
        """
    )
    return column_values(df, "boss_name")


@st.cache_data(ttl=120)
def load_cohort_players(guild_only: bool, raid: str) -> list[str]:
    df = ch_query(
        f"""
        SELECT DISTINCT player_name
        FROM {VIZ_TABLE}
        WHERE {guild_report_clause(guild_only)}
          AND {guild_player_clause(guild_only)}
          AND raid_or_dungeon = '{_esc(raid)}'
          AND player_name != ''
          AND lower(player_name) != 'unknown'
        ORDER BY player_name
        """
    )
    return column_values(df, "player_name")


@st.cache_data(ttl=60)
def load_dps_data(
    guild_only: bool,
    raid: str,
    boss: str | None,
    players: tuple[str, ...],
    difficulty_labels: tuple[str, ...],
    keystone_levels: tuple[int, ...],
    kills_only: bool,
) -> pd.DataFrame:
    filters = [
        guild_report_clause(guild_only),
        guild_player_clause(guild_only),
        f"raid_or_dungeon = '{_esc(raid)}'",
        "lower(player_name) != 'unknown'",
    ]
    if boss:
        filters.append(f"boss_name = '{_esc(boss)}'")
    if players:
        quoted = ", ".join(f"'{_esc(p)}'" for p in players)
        filters.append(f"player_name IN ({quoted})")
    if difficulty_labels:
        quoted = ", ".join(f"'{_esc(d)}'" for d in difficulty_labels)
        filters.append(f"difficulty_label IN ({quoted})")
    if keystone_levels:
        levels = ", ".join(str(int(k)) for k in keystone_levels)
        filters.append(f"keystone_level IN ({levels})")
    if kills_only:
        filters.append("is_kill = 1")

    where = " AND ".join(filters)
    df = ch_query(
        f"""
        SELECT
            report_start_at,
            report_date,
            player_name,
            report_guild_name AS guild_name,
            player_guild_name,
            dps,
            class_name,
            toString(spec_id) AS spec_name,
            avg_item_level AS item_level,
            raid_or_dungeon,
            boss_name,
            difficulty_label,
            keystone_level,
            content_type,
            if(is_kill = 1, 'kill', 'wipe') AS outcome,
            fight_duration_sec AS duration_sec,
            report_title
        FROM {VIZ_TABLE}
        WHERE {where}
        ORDER BY report_start_at, player_name
        """
    )
    if df.empty:
        return df

    df["report_start_at"] = pd.to_datetime(df["report_start_at"], utc=True)
    df["report_date"] = pd.to_datetime(df["report_date"])
    df["dps"] = pd.to_numeric(df["dps"], errors="coerce")
    df["item_level"] = pd.to_numeric(df["item_level"], errors="coerce")
    df["keystone_level"] = pd.to_numeric(df["keystone_level"], errors="coerce")
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

col_guild, col_raid = st.columns([1, 2])

with col_guild:
    guild_only = st.checkbox("Nightmares Asylum uniquement", value=True)

raids = load_raids(guild_only)
if not raids:
    if guild_only and row_count > 0:
        st.warning(
            "Des lignes existent dans la table gold, mais aucun raid ne correspond au filtre "
            "**membres guilde** (`is_guild_member = 1`). "
            "Décoche « Nightmares Asylum » ou lance **warcraftlogs_guild_roster** + **warcraftlogs_silver**."
        )
    else:
        st.info("Aucun raid / donjon pour ce filtre.")
    st.stop()

with col_raid:
    raid = st.selectbox("Raid / donjon", raids)

bosses = load_bosses(guild_only, raid)
cohort = load_cohort_players(guild_only, raid)

col_boss, col_diff, col_opts = st.columns([1, 1, 1])

with col_boss:
    boss_options = ["Tous les boss"] + bosses
    boss_choice = st.selectbox("Boss", boss_options)
    boss_filter = None if boss_choice == "Tous les boss" else boss_choice

with col_diff:
    diff_df = ch_query(
        f"""
        SELECT DISTINCT difficulty_label
        FROM {VIZ_TABLE}
        WHERE {guild_report_clause(guild_only)}
          AND {guild_player_clause(guild_only)}
          AND raid_or_dungeon = '{_esc(raid)}'
        ORDER BY difficulty_label
        """
    )
    diff_labels = diff_df["difficulty_label"].tolist() if not diff_df.empty else []
    difficulty_selected = st.multiselect(
        "Difficulté",
        diff_labels,
        default=diff_labels,
    )

with col_opts:
    kills_only = st.checkbox("Kills uniquement", value=False)
    key_df = ch_query(
        f"""
        SELECT DISTINCT keystone_level
        FROM {VIZ_TABLE}
        WHERE {guild_report_clause(guild_only)}
          AND {guild_player_clause(guild_only)}
          AND raid_or_dungeon = '{_esc(raid)}'
          AND keystone_level > 0
        ORDER BY keystone_level
        """
    )
    key_levels: list[int] = []
    if not key_df.empty:
        key_levels = [int(v) for v in key_df["keystone_level"].dropna()]
    keystone_selected: tuple[int, ...] = ()
    if key_levels:
        keystone_selected = tuple(
            st.multiselect("Niveau de clé (M+)", key_levels, default=key_levels)
        )

players_selected = st.multiselect(
    "Joueurs (cohorte du raid / donjon)",
    cohort,
    default=cohort,
    help=(
        "Par défaut : joueurs du roster guilde ayant au moins un pull "
        "dans ce contenu. Décoche « Nightmares Asylum » pour inclure les logs perso / M+."
    ),
)

if not players_selected:
    st.info("Sélectionne au moins un joueur.")
    st.stop()

df = load_dps_data(
    guild_only,
    raid,
    boss_filter,
    tuple(players_selected),
    tuple(difficulty_selected),
    keystone_selected,
    kills_only,
)

if df.empty:
    st.info("Aucun pull pour cette combinaison de filtres.")
    st.stop()

m1, m2, m3, m4 = st.columns(4)
m1.metric("Joueurs", df["player_name"].nunique())
m2.metric("Pulls", len(df))
m3.metric("Meilleur DPS", f"{int(df['dps'].max()):,}")
m4.metric("DPS moyen", f"{int(df['dps'].mean()):,}")

subtitle = raid if boss_filter is None else f"{raid} — {boss_filter}"
fig = px.line(
    df,
    x="report_start_at",
    y="dps",
    color="player_name",
    markers=True,
    hover_data={
        "report_start_at": "|%Y-%m-%d %H:%M",
        "dps": ":,.0f",
        "boss_name": True,
        "difficulty_label": True,
        "keystone_level": True,
        "spec_name": True,
        "item_level": True,
        "outcome": True,
        "guild_name": True,
        "player_guild_name": True,
        "player_name": False,
    },
    labels={
        "report_start_at": "Date du log",
        "dps": "DPS",
        "player_name": "Joueur",
    },
    title=subtitle,
)
fig.update_layout(
    hovermode="x unified",
    legend=dict(title="Joueur"),
    height=520,
)
fig.update_traces(mode="lines+markers")
st.plotly_chart(fig, use_container_width=True)

with st.expander("Détail des pulls"):
    show = df[
        [
            "report_start_at",
            "player_name",
            "guild_name",
            "player_guild_name",
            "boss_name",
            "dps",
            "difficulty_label",
            "keystone_level",
            "spec_name",
            "item_level",
            "outcome",
            "duration_sec",
            "report_title",
        ]
    ].copy()
    show["report_start_at"] = show["report_start_at"].dt.strftime("%Y-%m-%d %H:%M")
    st.dataframe(show.sort_values("report_start_at", ascending=False), hide_index=True)

"""Évolution du DPS boss par joueur (gold.wcl_boss_dps / ClickHouse)."""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from ch_utils import _esc, ch_query, ch_scalar

st.set_page_config(page_title="WCL — Évolution DPS", layout="wide")

st.title("Warcraft Logs — Évolution DPS")
st.caption(
    "Courbe DPS par pull (boss × joueur). "
    "Alimenté par `gold.wcl_boss_dps` — DAG `warcraftlogs_lakehouse_dbt`."
)


@st.cache_data(ttl=120)
def load_players() -> list[str]:
    df = ch_query(
        """
        SELECT DISTINCT player_name
        FROM wcl_boss_dps
        WHERE player_name != ''
        ORDER BY player_name
        """
    )
    return df["player_name"].tolist()


@st.cache_data(ttl=120)
def load_bosses(player: str) -> list[str]:
    df = ch_query(
        f"""
        SELECT DISTINCT fight_name
        FROM wcl_boss_dps
        WHERE player_name = '{_esc(player)}'
        ORDER BY fight_name
        """
    )
    return df["fight_name"].tolist()


@st.cache_data(ttl=60)
def load_dps_series(
    player: str,
    boss: str,
    kills_only: bool,
    difficulty: int | None,
) -> pd.DataFrame:
    filters = [
        f"player_name = '{_esc(player)}'",
        f"fight_name = '{_esc(boss)}'",
    ]
    if kills_only:
        filters.append("outcome = 'kill'")
    if difficulty is not None:
        filters.append(f"difficulty = {int(difficulty)}")

    where = " AND ".join(filters)
    df = ch_query(
        f"""
        SELECT
            report_start_at,
            report_date,
            dps,
            item_level,
            spec_name,
            class_name,
            outcome,
            difficulty,
            duration_sec,
            zone_name,
            report_title
        FROM wcl_boss_dps
        WHERE {where}
        ORDER BY report_start_at
        """
    )
    if df.empty:
        return df

    df["report_start_at"] = pd.to_datetime(df["report_start_at"], utc=True)
    df["report_date"] = pd.to_datetime(df["report_date"])
    df["dps"] = pd.to_numeric(df["dps"], errors="coerce")
    df["item_level"] = pd.to_numeric(df["item_level"], errors="coerce")
    return df


try:
    row_count = ch_scalar("SELECT count() FROM wcl_boss_dps")
except RuntimeError as exc:
    st.error(f"Connexion ClickHouse impossible : {exc}")
    st.stop()

if row_count == 0:
    st.warning(
        "La table `gold.wcl_boss_dps` est vide. "
        "Lance le DAG **warcraftlogs_lakehouse_dbt** (silver + gold) dans Airflow."
    )
    st.stop()

players = load_players()
if not players:
    st.warning("Aucun joueur dans gold.wcl_boss_dps.")
    st.stop()

default_player_idx = next(
    (i for i, name in enumerate(players) if "kimahri" in name.lower()),
    0,
)

col_player, col_boss, col_opts = st.columns([1, 1, 1])

with col_player:
    player = st.selectbox("Joueur", players, index=default_player_idx)

bosses = load_bosses(player)
if not bosses:
    st.info(f"Aucun boss enregistré pour **{player}**.")
    st.stop()

with col_boss:
    boss = st.selectbox("Boss", bosses)

with col_opts:
    kills_only = st.checkbox("Kills uniquement", value=False)
    diff_df = ch_query(
        f"""
        SELECT DISTINCT difficulty
        FROM wcl_boss_dps
        WHERE player_name = '{_esc(player)}'
          AND fight_name = '{_esc(boss)}'
          AND difficulty IS NOT NULL
        ORDER BY difficulty
        """
    )
    diff_options = ["Toutes"]
    if not diff_df.empty:
        diff_options += [str(int(v)) for v in diff_df["difficulty"].dropna()]
    difficulty_choice = st.selectbox("Difficulté", diff_options)
    difficulty = None if difficulty_choice == "Toutes" else int(difficulty_choice)

df = load_dps_series(player, boss, kills_only, difficulty)

if df.empty:
    st.info("Aucun pull pour cette combinaison (filtres inclus).")
    st.stop()

best = int(df["dps"].max())
avg = int(df["dps"].mean())
nb = len(df)
m1, m2, m3, m4 = st.columns(4)
m1.metric("Pulls", nb)
m2.metric("Meilleur DPS", f"{best:,}")
m3.metric("DPS moyen", f"{avg:,}")
m4.metric("Ilvl max", int(df["item_level"].max()) if df["item_level"].notna().any() else "—")

df["label"] = df.apply(
    lambda r: (
        f"{r['outcome']} · {r['spec_name']} · ilvl {int(r['item_level'])}"
        if pd.notna(r["item_level"])
        else f"{r['outcome']} · {r['spec_name']}"
    ),
    axis=1,
)

fig = px.line(
    df,
    x="report_start_at",
    y="dps",
    markers=True,
    color="outcome",
    color_discrete_map={"kill": "#2ecc71", "wipe": "#e74c3c"},
    hover_data={
        "report_start_at": "|%Y-%m-%d %H:%M",
        "dps": ":,.0f",
        "item_level": True,
        "spec_name": True,
        "duration_sec": True,
        "zone_name": True,
        "label": False,
    },
    labels={
        "report_start_at": "Date du log",
        "dps": "DPS",
        "outcome": "Résultat",
    },
    title=f"{player} — {boss}",
)
fig.update_layout(
    hovermode="x unified",
    legend=dict(title="Résultat"),
    height=480,
)
fig.update_traces(mode="lines+markers")
st.plotly_chart(fig, use_container_width=True)

with st.expander("Détail des pulls"):
    show = df[
        [
            "report_start_at",
            "dps",
            "item_level",
            "spec_name",
            "outcome",
            "difficulty",
            "duration_sec",
            "zone_name",
            "report_title",
        ]
    ].copy()
    show["report_start_at"] = show["report_start_at"].dt.strftime("%Y-%m-%d %H:%M")
    st.dataframe(show.sort_values("report_start_at", ascending=False), hide_index=True)

import os
import calendar

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st

st.set_page_config(page_title="Climat Nord — Analyse", layout="wide")

DEFAULT_STATION = "VALENCIENNES"
JOURS_SEMAINE = ["Lun", "Mar", "Mer", "Jeu", "Ven", "Sam", "Dim"]
METRIQUES_CALENDRIER = {
    "temp_max": ("TX (°C)", "RdYlBu_r", None),
    "temp_min": ("TN (°C)", "Blues", None),
    "ecart_tx_normale": ("Écart TX vs normale (°C)", "RdBu", 0.0),
    "ecart_tn_normale": ("Écart TN vs normale (°C)", "RdBu", 0.0),
}
COLOR_TX = "#FF8C00"
COLOR_TN = "#87CEEB"
COLOR_CHAUD = "#E74C3C"
COLOR_FROID = "#3498DB"
COLOR_PLUIE = "#2E86AB"
SEUIL_ECART_CHAUD = 2.0
SEUIL_ECART_FROID = -2.0

MOIS_FR = {
    1: "Janvier",
    2: "Février",
    3: "Mars",
    4: "Avril",
    5: "Mai",
    6: "Juin",
    7: "Juillet",
    8: "Août",
    9: "Septembre",
    10: "Octobre",
    11: "Novembre",
    12: "Décembre",
}


def _cfg(name: str, default: str = "") -> str:
    if name in st.secrets:
        return str(st.secrets[name])
    return os.getenv(name, default)


def _connect():
    return psycopg2.connect(
        host=_cfg("APP_DB_HOST", "postgres-db"),
        port=int(_cfg("APP_DB_PORT", "5432")),
        user=_cfg("APP_DB_USER", ""),
        password=_cfg("APP_DB_PASSWORD", ""),
        dbname=_cfg("APP_DB_NAME", "rfm"),
    )


@st.cache_data(ttl=30)
def load_dernier_jour_kpis() -> pd.DataFrame:
    """Extrêmes du dernier jour + normales historiques pour ce jour calendaire."""
    conn = _connect()
    try:
        return pd.read_sql_query(
            """
            WITH last AS (
                SELECT MAX(date_mesure::date) AS d
                FROM public.climat_data
            )
            SELECT
                l.d AS date_mesure,
                MIN(c.temp_min) FILTER (WHERE c.temp_min IS NOT NULL) AS temp_min_jour,
                MAX(c.temp_max) FILTER (WHERE c.temp_max IS NOT NULL) AS temp_max_jour,
                ROUND(AVG(n.tx_moy)::numeric, 2) AS moy_max_historique,
                ROUND(AVG(n.tn_moy)::numeric, 2) AS moy_min_historique,
                COUNT(*) FILTER (WHERE c.temp_max IS NOT NULL) AS nb_postes
            FROM last l
            JOIN public.climat_data c ON c.date_mesure::date = l.d
            LEFT JOIN public.v_climat_normale_jour n
              ON n.mois = EXTRACT(MONTH FROM l.d)::int
             AND n.jour = EXTRACT(DAY FROM l.d)::int
            GROUP BY l.d
            """,
            conn,
        )
    finally:
        conn.close()


def _fmt_date_fr(d: pd.Timestamp) -> str:
    return f"{int(d.day)} {MOIS_FR[int(d.month)]} {int(d.year)}"


@st.cache_data(ttl=120)
def load_temp_max_par_station() -> pd.DataFrame:
    conn = _connect()
    try:
        return pd.read_sql_query(
            """
            SELECT *
            FROM public.v_temp_max_par_station
            ORDER BY temp_max_record DESC
            """,
            conn,
        )
    finally:
        conn.close()


@st.cache_data(ttl=120)
def load_station_names() -> list[str]:
    conn = _connect()
    try:
        df = pd.read_sql_query(
            """
            SELECT DISTINCT station_nom
            FROM public.v_climat_evolution_annee
            ORDER BY station_nom
            """,
            conn,
        )
    finally:
        conn.close()
    return df["station_nom"].tolist()


@st.cache_data(ttl=120)
def load_years() -> list[int]:
    conn = _connect()
    try:
        df = pd.read_sql_query(
            """
            SELECT DISTINCT annee
            FROM public.v_climat_evolution_annee
            ORDER BY annee DESC
            """,
            conn,
        )
    finally:
        conn.close()
    return df["annee"].astype(int).tolist()


@st.cache_data(ttl=120)
def load_evolution(station_nom: str, annee: int) -> pd.DataFrame:
    conn = _connect()
    try:
        return pd.read_sql_query(
            """
            SELECT
                date_mesure,
                temp_max_n,
                temp_max_n1,
                temp_max_record,
                annee_record
            FROM public.v_climat_evolution_annee
            WHERE station_nom = %(station_nom)s
              AND annee = %(annee)s
            ORDER BY date_mesure
            """,
            conn,
            params={"station_nom": station_nom, "annee": annee},
        )
    finally:
        conn.close()


@st.cache_data(ttl=120)
def load_normale_jour(mois: int, jour: int) -> pd.DataFrame:
    conn = _connect()
    try:
        return pd.read_sql_query(
            """
            SELECT
                station_id,
                station_nom,
                tn_moy,
                tn_min_hist,
                tn_max_hist,
                tx_moy,
                tx_min_hist,
                tx_max_hist,
                nb_annees_tn,
                nb_annees_tx
            FROM public.v_climat_normale_jour
            WHERE mois = %(mois)s
              AND jour = %(jour)s
            ORDER BY station_nom
            """,
            conn,
            params={"mois": mois, "jour": jour},
        )
    finally:
        conn.close()


def _libelle_station(df: pd.DataFrame) -> pd.Series:
    """Libellé unique : même nom usuel peut couvrir plusieurs NUM_POSTE MF."""
    nom = df["station_nom"].astype(str)
    poste = df["station_id"].astype(int).astype(str)
    plusieurs = df.groupby("station_nom")["station_id"].transform("size") > 1
    return nom.where(~plusieurs, nom + " (" + poste + ")")


def _barres_normale_avec_fourchette(df: pd.DataFrame, label_y: str) -> go.Figure:
    """Barres TN (bleu) / TX (orange) par station, fourchette min–max historique."""
    plot_df = df.dropna(
        subset=["tn_moy", "tn_min_hist", "tn_max_hist", "tx_moy", "tx_min_hist", "tx_max_hist"]
    ).copy()
    plot_df["libelle"] = _libelle_station(plot_df)
    stations = plot_df["libelle"].tolist()
    tn = plot_df["tn_moy"].astype(float)
    tx = plot_df["tx_moy"].astype(float)

    fig = go.Figure(
        data=[
            go.Bar(
                name="TN moy.",
                x=stations,
                y=tn,
                marker_color=COLOR_TN,
                error_y=dict(
                    type="data",
                    symmetric=False,
                    array=(plot_df["tn_max_hist"] - tn).tolist(),
                    arrayminus=(tn - plot_df["tn_min_hist"]).tolist(),
                ),
            ),
            go.Bar(
                name="TX moy.",
                x=stations,
                y=tx,
                marker_color=COLOR_TX,
                error_y=dict(
                    type="data",
                    symmetric=False,
                    array=(plot_df["tx_max_hist"] - tx).tolist(),
                    arrayminus=(tx - plot_df["tx_min_hist"]).tolist(),
                ),
            ),
        ]
    )
    n = len(stations)
    fig.update_layout(
        barmode="group",
        yaxis_title=label_y,
        xaxis_title="Station",
        height=min(900, max(480, 32 * n)),
        xaxis_tickangle=-45,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=80, b=120),
    )
    return fig


@st.cache_data(ttl=120)
def load_stations_calendrier() -> pd.DataFrame:
    conn = _connect()
    try:
        return pd.read_sql_query(
            """
            SELECT DISTINCT station_id, station_nom
            FROM public.v_climat_jour_station
            ORDER BY station_nom
            """,
            conn,
        )
    finally:
        conn.close()


@st.cache_data(ttl=120)
def load_annees_calendrier(station_id: int) -> list[int]:
    conn = _connect()
    try:
        df = pd.read_sql_query(
            """
            SELECT DISTINCT annee
            FROM public.v_climat_jour_station
            WHERE station_id = %(station_id)s
            ORDER BY annee DESC
            """,
            conn,
            params={"station_id": station_id},
        )
    finally:
        conn.close()
    return df["annee"].astype(int).tolist()


@st.cache_data(ttl=120)
def load_calendrier_station(station_id: int, annee: int) -> pd.DataFrame:
    conn = _connect()
    try:
        return pd.read_sql_query(
            """
            SELECT
                date_mesure,
                temp_min,
                temp_max,
                ecart_tn_normale,
                ecart_tx_normale
            FROM public.v_climat_jour_station
            WHERE station_id = %(station_id)s
              AND annee = %(annee)s
            ORDER BY date_mesure
            """,
            conn,
            params={"station_id": station_id, "annee": annee},
        )
    finally:
        conn.close()


def _fig_calendrier_heatmap(df: pd.DataFrame, col_z: str, label: str, colorscale: str, zmid) -> go.Figure:
    """Heatmap semaine ISO × jour de semaine."""
    plot = df.copy()
    plot["date"] = pd.to_datetime(plot["date_mesure"])
    iso = plot["date"].dt.isocalendar()
    plot["semaine"] = iso.week.astype(int)
    plot["jour_sem"] = plot["date"].dt.dayofweek
    pivot_z = plot.pivot_table(index="jour_sem", columns="semaine", values=col_z, aggfunc="first")
    pivot_dt = plot.pivot_table(
        index="jour_sem", columns="semaine", values="date", aggfunc="first"
    )
    pivot_z = pivot_z.reindex(range(7)).sort_index(axis=1)
    pivot_dt = pivot_dt.reindex(range(7)).reindex(columns=pivot_z.columns)

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot_z.values,
            x=pivot_z.columns.astype(str),
            y=JOURS_SEMAINE,
            customdata=pivot_dt.values,
            colorscale=colorscale,
            zmid=zmid,
            hoverongaps=False,
            hovertemplate=(
                "Semaine %{x}<br>%{y}<br>"
                "%{customdata|%d/%m/%Y}<br>"
                f"{label}: %{{z:.1f}}<extra></extra>"
            ),
            colorbar=dict(title=label),
        )
    )
    fig.update_layout(
        xaxis_title="Semaine",
        yaxis_title="",
        height=320,
        margin=dict(l=48, r=24, t=24, b=48),
    )
    return fig


@st.cache_data(ttl=120)
def vue_disponible(nom_vue: str) -> bool:
    conn = _connect()
    try:
        pd.read_sql_query(f"SELECT 1 FROM public.{nom_vue} LIMIT 1", conn)
        return True
    except Exception:
        return False
    finally:
        conn.close()


def normale_vue_disponible() -> bool:
    return vue_disponible("v_climat_normale_jour")


@st.cache_data(ttl=120)
def load_stations_pluie() -> pd.DataFrame:
    conn = _connect()
    try:
        return pd.read_sql_query(
            """
            SELECT DISTINCT station_id, station_nom
            FROM public.v_climat_pluie_mois
            ORDER BY station_nom
            """,
            conn,
        )
    finally:
        conn.close()


@st.cache_data(ttl=120)
def load_annees_pluie(station_id: int) -> list[int]:
    conn = _connect()
    try:
        df = pd.read_sql_query(
            """
            SELECT DISTINCT annee
            FROM public.v_climat_pluie_mois
            WHERE station_id = %(station_id)s
            ORDER BY annee DESC
            """,
            conn,
            params={"station_id": station_id},
        )
    finally:
        conn.close()
    return df["annee"].astype(int).tolist()


@st.cache_data(ttl=120)
def load_pluie_mois(station_id: int, annee: int) -> pd.DataFrame:
    conn = _connect()
    try:
        return pd.read_sql_query(
            """
            SELECT mois, rr_sum_mm, nb_jours_pluie
            FROM public.v_climat_pluie_mois
            WHERE station_id = %(station_id)s
              AND annee = %(annee)s
            ORDER BY mois
            """,
            conn,
            params={"station_id": station_id, "annee": annee},
        )
    finally:
        conn.close()


@st.cache_data(ttl=120)
def load_pluie_annee_reseau(annee: int) -> pd.DataFrame:
    conn = _connect()
    try:
        return pd.read_sql_query(
            """
            SELECT
                station_id,
                station_nom,
                rr_sum_annuel,
                nb_jours_pluie,
                latitude,
                longitude
            FROM public.v_climat_pluie_annee
            WHERE annee = %(annee)s
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
            ORDER BY rr_sum_annuel DESC NULLS LAST
            """,
            conn,
            params={"annee": annee},
        )
    finally:
        conn.close()


@st.cache_data(ttl=120)
def load_annees_pluie_reseau() -> list[int]:
    conn = _connect()
    try:
        df = pd.read_sql_query(
            """
            SELECT DISTINCT annee
            FROM public.v_climat_pluie_annee
            ORDER BY annee DESC
            """,
            conn,
        )
    finally:
        conn.close()
    return df["annee"].astype(int).tolist()


@st.cache_data(ttl=120)
def load_chaud_froid_annee(annee: int) -> pd.DataFrame:
    conn = _connect()
    try:
        return pd.read_sql_query(
            """
            SELECT
                station_id,
                station_nom,
                nb_jours_chauds,
                nb_jours_froids,
                latitude,
                longitude
            FROM public.v_climat_chaud_froid_annee
            WHERE annee = %(annee)s
            ORDER BY station_nom
            """,
            conn,
            params={"annee": annee},
        )
    finally:
        conn.close()


@st.cache_data(ttl=120)
def load_annees_chaud_froid() -> list[int]:
    conn = _connect()
    try:
        df = pd.read_sql_query(
            """
            SELECT DISTINCT annee
            FROM public.v_climat_chaud_froid_annee
            ORDER BY annee DESC
            """,
            conn,
        )
    finally:
        conn.close()
    return df["annee"].astype(int).tolist()


st.title("Analyse du climat Nord")

hdr_kpi, hdr_refresh = st.columns([5, 1])
with hdr_refresh:
    if st.button("Actualiser", help="Recharge les indicateurs depuis la base"):
        load_dernier_jour_kpis.clear()
        load_stations_calendrier.clear()
        load_annees_calendrier.clear()
        load_calendrier_station.clear()
        load_stations_pluie.clear()
        load_annees_pluie.clear()
        load_pluie_mois.clear()
        load_pluie_annee_reseau.clear()
        load_annees_pluie_reseau.clear()
        load_chaud_froid_annee.clear()
        load_annees_chaud_froid.clear()

try:
    kpis_jour = load_dernier_jour_kpis()
except Exception as exc:
    kpis_jour = pd.DataFrame()
    st.error(f"Impossible de lire les KPIs (`climat_data`) : {exc}")

if kpis_jour.empty:
    st.warning(
        "Aucune mesure thermo sur le dernier jour — vérifiez `climat_data` "
        "ou lancez `meteo_nord` / `meteo_nord_quotidien`."
    )
else:
    kpi = kpis_jour.iloc[0]
    date_ts = pd.to_datetime(kpi["date_mesure"])
    date_long = _fmt_date_fr(date_ts)
    nb_postes = int(kpi["nb_postes"])
    st.markdown(
        f"**Dernier jour enregistré :** {date_long} "
        f"({nb_postes} postes thermo)"
    )
    col_max, col_min, col_moy_max, col_moy_min = st.columns(4)
    with col_max:
        v = kpi["temp_max_jour"]
        st.metric(
            "Temp. max du jour",
            f"{v:.1f} °C" if pd.notna(v) else "—",
            help="Plus haute température maximale observée ce jour-là sur le réseau",
        )
    with col_min:
        v = kpi["temp_min_jour"]
        st.metric(
            "Temp. min du jour",
            f"{v:.1f} °C" if pd.notna(v) else "—",
            help="Plus basse température minimale observée ce jour-là sur le réseau",
        )
    with col_moy_max:
        v = kpi["moy_max_historique"]
        st.metric(
            "Moy. max historique",
            f"{v:.1f} °C" if pd.notna(v) else "—",
            help=(
                f"Moyenne des TX habituelles pour le {date_ts.day} "
                f"{MOIS_FR[int(date_ts.month)]} (tous postes, historique)"
            ),
        )
    with col_moy_min:
        v = kpi["moy_min_historique"]
        st.metric(
            "Moy. min historique",
            f"{v:.1f} °C" if pd.notna(v) else "—",
            help=(
                f"Moyenne des TN habituelles pour le {date_ts.day} "
                f"{MOIS_FR[int(date_ts.month)]} (tous postes, historique)"
            ),
        )

st.subheader("Record de température max par station")
stations = load_temp_max_par_station()
fig_map = px.scatter_map(
    stations,
    lat="latitude",
    lon="longitude",
    hover_name="station_nom",
    hover_data=["temp_max_record"],
    color="temp_max_record",
    color_continuous_scale="RdYlBu_r",
    zoom=7,
    height=500,
)
fig_map.update_layout(
    margin=dict(l=0, r=0, t=0, b=40),
    coloraxis_colorbar=dict(yanchor="middle", y=0.5, len=0.55),
)
st.plotly_chart(fig_map, use_container_width=True)
st.markdown("<div style='margin-bottom: 1.25rem'></div>", unsafe_allow_html=True)

st.subheader("Normale climatique (jour calendaire)")
if not normale_vue_disponible():
    st.warning(
        "Vue `v_climat_normale_jour` absente — relancez le DAG `gold_meteo_nord`."
    )
else:
    col_mois, col_jour = st.columns(2)
    with col_mois:
        mois_normale = st.selectbox(
            "Mois",
            list(MOIS_FR.keys()),
            index=7,
            format_func=lambda m: MOIS_FR[m],
            key="normale_mois",
        )
    max_jour = calendar.monthrange(2024, mois_normale)[1]
    with col_jour:
        jour_normale = st.selectbox(
            "Jour",
            list(range(1, max_jour + 1)),
            index=0,
            key="normale_jour",
        )

    normale = load_normale_jour(int(mois_normale), int(jour_normale))
    if normale.empty:
        st.info(f"Aucune normale pour le {jour_normale} {MOIS_FR[mois_normale]}.")
    else:
        fig_normale = _barres_normale_avec_fourchette(normale, "Température (°C)")
        fig_normale.update_layout(
            title=(
                f"{jour_normale} {MOIS_FR[mois_normale]} — "
                f"{len(normale)} stations (moyenne + fourchette min/max par station)"
            ),
        )
        st.plotly_chart(fig_normale, use_container_width=True)
        st.caption(
            f"Pour chaque poste (NUM_POSTE) : moyenne des TN (bleu) et TX (orange) sur tous les "
            f"{jour_normale} {MOIS_FR[mois_normale]} de l'historique ; "
            f"traits = min et max observés ce jour-là. "
            f"Si le même nom apparaît plusieurs fois, ce sont des postes distincts "
            f"(ex. plusieurs STEENVOORDE)."
        )
        with st.expander("Données par station"):
            st.dataframe(normale, use_container_width=True, hide_index=True)

st.subheader("Calendrier thermique (heatmap)")
stations_cal = load_stations_calendrier()
if stations_cal.empty:
    st.warning(
        "Vue `v_climat_jour_station` absente — relancez le DAG `gold_meteo_nord`."
    )
else:
    stations_cal = stations_cal.copy()
    stations_cal["libelle"] = _libelle_station(stations_cal)
    station_ids_cal = stations_cal["station_id"].astype(int).tolist()
    default_cal_idx = next(
        (
            i
            for i, row in enumerate(stations_cal.itertuples(index=False))
            if str(row.station_nom).upper() == DEFAULT_STATION.upper()
        ),
        0,
    )
    col_st, col_an, col_met = st.columns([2, 1, 1])
    with col_st:
        station_id_cal = st.selectbox(
            "Station",
            options=station_ids_cal,
            index=default_cal_idx,
            format_func=lambda sid: stations_cal.loc[
                stations_cal["station_id"] == sid, "libelle"
            ].iloc[0],
            key="cal_station",
        )
    station_id_cal = int(station_id_cal)
    station_nom_cal = str(
        stations_cal.loc[stations_cal["station_id"] == station_id_cal, "station_nom"].iloc[0]
    )
    annees_cal = load_annees_calendrier(station_id_cal)
    with col_an:
        annee_cal = st.selectbox("Année", annees_cal, index=0, key="cal_annee")
    with col_met:
        metrique_cal = st.selectbox(
            "Variable",
            list(METRIQUES_CALENDRIER.keys()),
            format_func=lambda k: METRIQUES_CALENDRIER[k][0],
            key="cal_metrique",
        )

    cal_df = load_calendrier_station(station_id_cal, int(annee_cal))
    if cal_df.empty:
        st.info(f"Aucune donnée pour {station_nom_cal} en {annee_cal}.")
    else:
        label, cmap, zmid = METRIQUES_CALENDRIER[metrique_cal]
        fig_cal = _fig_calendrier_heatmap(cal_df, metrique_cal, label, cmap, zmid)
        fig_cal.update_layout(
            title=f"{station_nom_cal} — {annee_cal} — {label}",
        )
        st.plotly_chart(fig_cal, use_container_width=True)
        st.caption(
            "Chaque case = un jour (semaine ISO × jour de la semaine). "
            "Survolez pour la date exacte via l'axe temporel des données."
        )

st.subheader("Précipitations")
if not vue_disponible("v_climat_pluie_mois"):
    st.warning(
        "Vues `v_climat_pluie_mois` / `v_climat_pluie_annee` absentes — "
        "relancez le DAG `gold_meteo_nord`."
    )
else:
    stations_pluie = load_stations_pluie()
    annees_pluie_reseau = load_annees_pluie_reseau()
    if stations_pluie.empty or not annees_pluie_reseau:
        st.info("Aucune donnée de pluie disponible.")
    else:
        stations_pluie = stations_pluie.copy()
        stations_pluie["libelle"] = _libelle_station(stations_pluie)
        station_ids_pluie = stations_pluie["station_id"].astype(int).tolist()
        default_pluie_idx = next(
            (
                i
                for i, row in enumerate(stations_pluie.itertuples(index=False))
                if str(row.station_nom).upper() == DEFAULT_STATION.upper()
            ),
            0,
        )
        col_pl_st, col_pl_an, col_pl_map = st.columns([2, 1, 1])
        with col_pl_st:
            station_id_pluie = st.selectbox(
                "Station",
                options=station_ids_pluie,
                index=default_pluie_idx,
                format_func=lambda sid: stations_pluie.loc[
                    stations_pluie["station_id"] == sid, "libelle"
                ].iloc[0],
                key="pluie_station",
            )
        station_id_pluie = int(station_id_pluie)
        station_nom_pluie = str(
            stations_pluie.loc[
                stations_pluie["station_id"] == station_id_pluie, "station_nom"
            ].iloc[0]
        )
        annees_pluie_st = load_annees_pluie(station_id_pluie)
        with col_pl_an:
            annee_pluie = st.selectbox(
                "Année (série mensuelle)",
                annees_pluie_st or annees_pluie_reseau,
                index=0,
                key="pluie_annee",
            )
        with col_pl_map:
            annee_pluie_carte = st.selectbox(
                "Année (carte réseau)",
                annees_pluie_reseau,
                index=0,
                key="pluie_annee_carte",
            )

        pluie_mois = load_pluie_mois(station_id_pluie, int(annee_pluie))
        if pluie_mois.empty:
            st.info(f"Aucune pluie enregistrée pour {station_nom_pluie} en {annee_pluie}.")
        else:
            cumul_annuel = float(pluie_mois["rr_sum_mm"].sum())
            jours_pluie = int(pluie_mois["nb_jours_pluie"].sum())
            k1, k2, k3 = st.columns(3)
            with k1:
                st.metric("Cumul annuel", f"{cumul_annuel:.1f} mm")
            with k2:
                st.metric("Jours avec pluie (RR > 0)", jours_pluie)
            with k3:
                mois_max = int(pluie_mois.loc[pluie_mois["rr_sum_mm"].idxmax(), "mois"])
                st.metric(
                    "Mois le plus pluvieux",
                    MOIS_FR[mois_max],
                    f"{pluie_mois['rr_sum_mm'].max():.1f} mm",
                )

            plot_pluie = pluie_mois.copy()
            plot_pluie["mois_lib"] = plot_pluie["mois"].map(MOIS_FR)
            fig_pluie = px.bar(
                plot_pluie,
                x="mois_lib",
                y="rr_sum_mm",
                labels={"mois_lib": "Mois", "rr_sum_mm": "Cumul (mm)"},
                title=f"{station_nom_pluie} — cumul mensuel {annee_pluie}",
                color_discrete_sequence=[COLOR_PLUIE],
            )
            fig_pluie.update_layout(
                height=380,
                showlegend=False,
                xaxis_categoryorder="array",
                xaxis_categoryarray=[MOIS_FR[m] for m in range(1, 13) if m in plot_pluie["mois"].values],
            )
            st.plotly_chart(fig_pluie, use_container_width=True)

        pluie_reseau = load_pluie_annee_reseau(int(annee_pluie_carte))
        if pluie_reseau.empty:
            st.info(f"Aucune donnée pluie réseau pour {annee_pluie_carte}.")
        else:
            fig_pluie_map = px.scatter_map(
                pluie_reseau,
                lat="latitude",
                lon="longitude",
                hover_name="station_nom",
                hover_data=["rr_sum_annuel", "nb_jours_pluie"],
                color="rr_sum_annuel",
                color_continuous_scale="Blues",
                size="rr_sum_annuel",
                size_max=18,
                zoom=7,
                height=480,
                labels={"rr_sum_annuel": "Cumul annuel (mm)"},
                title=f"Cumul annuel des précipitations — {annee_pluie_carte}",
            )
            fig_pluie_map.update_layout(
                margin=dict(l=0, r=0, t=48, b=40),
                coloraxis_colorbar=dict(yanchor="middle", y=0.5, len=0.55),
            )
            st.plotly_chart(fig_pluie_map, use_container_width=True)
            st.caption(
                "Taille et couleur des points ∝ cumul annuel (mm). "
                "Inclut les postes pluviométriques sans thermométrie."
            )

st.subheader("Jours chauds et froids (vs normale)")
if not vue_disponible("v_climat_chaud_froid_annee"):
    st.warning(
        "Vue `v_climat_chaud_froid_annee` absente — relancez le DAG `gold_meteo_nord`."
    )
else:
    annees_cf = load_annees_chaud_froid()
    if not annees_cf:
        st.info("Aucun comptage disponible (postes thermo requis).")
    else:
        annee_cf = st.selectbox("Année", annees_cf, index=0, key="chaud_froid_annee")
        cf_df = load_chaud_froid_annee(int(annee_cf))
        cf_df = cf_df[
            (cf_df["nb_jours_chauds"] > 0) | (cf_df["nb_jours_froids"] > 0)
        ].copy()
        if cf_df.empty:
            st.info(f"Aucun jour chaud/froid en {annee_cf} avec les seuils actuels.")
        else:
            st.caption(
                f"**Chaud** : TX > normale du jour + {SEUIL_ECART_CHAUD:.0f} °C — "
                f"**Froid** : TN < normale du jour {SEUIL_ECART_FROID:.0f} °C "
                "(postes avec thermométrie uniquement)."
            )
            total_chauds = int(cf_df["nb_jours_chauds"].sum())
            total_froids = int(cf_df["nb_jours_froids"].sum())
            c1, c2 = st.columns(2)
            with c1:
                st.metric("Jours chauds (réseau thermo)", total_chauds)
            with c2:
                st.metric("Jours froids (réseau thermo)", total_froids)

            cf_plot = cf_df.copy()
            cf_plot["libelle"] = _libelle_station(cf_plot)
            cf_long = cf_plot.melt(
                id_vars=["libelle"],
                value_vars=["nb_jours_chauds", "nb_jours_froids"],
                var_name="type",
                value_name="nb_jours",
            )
            cf_long["type"] = cf_long["type"].map(
                {
                    "nb_jours_chauds": "Jours chauds",
                    "nb_jours_froids": "Jours froids",
                }
            )
            fig_cf = px.bar(
                cf_long,
                x="libelle",
                y="nb_jours",
                color="type",
                barmode="group",
                color_discrete_map={
                    "Jours chauds": COLOR_CHAUD,
                    "Jours froids": COLOR_FROID,
                },
                labels={"libelle": "Station", "nb_jours": "Nombre de jours", "type": ""},
                title=f"Comptage par station — {annee_cf}",
            )
            n_st = len(cf_plot)
            fig_cf.update_layout(
                height=min(900, max(420, 28 * n_st)),
                xaxis_tickangle=-45,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, x=1),
                margin=dict(t=80, b=120),
            )
            st.plotly_chart(fig_cf, use_container_width=True)

            cf_geo = cf_df.dropna(subset=["latitude", "longitude"])
            if not cf_geo.empty:
                fig_cf_map = px.scatter_map(
                    cf_geo,
                    lat="latitude",
                    lon="longitude",
                    hover_name="station_nom",
                    hover_data=["nb_jours_chauds", "nb_jours_froids"],
                    color="nb_jours_chauds",
                    color_continuous_scale="YlOrRd",
                    size="nb_jours_chauds",
                    size_max=20,
                    zoom=7,
                    height=420,
                    title=f"Jours chauds par station — {annee_cf}",
                )
                fig_cf_map.update_layout(margin=dict(l=0, r=0, t=48, b=40))
                st.plotly_chart(fig_cf_map, use_container_width=True)

st.subheader("Évolution des températures max")
station_names = load_station_names()
years = load_years()

if not station_names or not years:
    st.warning("Lancez les DAG `meteo_nord` puis `gold_meteo_nord` pour créer les vues.")
    st.stop()

default_station_idx = next(
    (i for i, n in enumerate(station_names) if n.upper() == DEFAULT_STATION.upper()),
    0,
)
col_ville, col_annee = st.columns(2)
with col_ville:
    station_nom = st.selectbox("Ville", station_names, index=default_station_idx)
with col_annee:
    annee = st.selectbox("Année", years, index=0)

mois_debut, mois_fin = st.slider(
    "Période (mois)",
    min_value=1,
    max_value=12,
    value=(5, 8),
    help="Réduit l'échelle pour une lecture plus claire (ex. mai–août).",
)

evo = load_evolution(station_nom, int(annee))
if evo.empty:
    st.info("Aucune donnée pour cette ville et cette année.")
else:
    evo["date_mesure"] = pd.to_datetime(evo["date_mesure"])
    if mois_debut <= mois_fin:
        mask = evo["date_mesure"].dt.month.between(mois_debut, mois_fin)
    else:
        mask = (evo["date_mesure"].dt.month >= mois_debut) | (
            evo["date_mesure"].dt.month <= mois_fin
        )
    evo = evo.loc[mask]

    plot_df = evo.melt(
        id_vars=["date_mesure", "annee_record"],
        value_vars=["temp_max_n", "temp_max_n1", "temp_max_record"],
        var_name="serie",
        value_name="temperature",
    )
    labels = {
        "temp_max_n": str(annee),
        "temp_max_n1": str(int(annee) - 1),
        "temp_max_record": "Record (jour calendaire)",
    }
    plot_df["serie"] = plot_df["serie"].map(labels)

    fig_line = px.line(
        plot_df,
        x="date_mesure",
        y="temperature",
        color="serie",
        labels={"date_mesure": "Date", "temperature": "Temp. max (°C)", "serie": "Série"},
        title=f"{station_nom} — {annee} vs {int(annee) - 1} vs record",
    )
    fig_line.update_traces(mode="lines", line=dict(width=2))
    fig_line.update_layout(
        height=620,
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="top",
            y=-0.22,
            xanchor="center",
            x=0.5,
        ),
        margin=dict(l=48, r=24, t=48, b=100),
    )
    fig_line.update_xaxes(
        rangeslider=dict(visible=False),
        tickformat="%d %b",
        dtick="M1",
        tickangle=-45,
    )
    st.plotly_chart(
        fig_line,
        use_container_width=True,
        config={"scrollZoom": True, "displayModeBar": True},
    )

    with st.expander("Données détaillées"):
        st.dataframe(evo)

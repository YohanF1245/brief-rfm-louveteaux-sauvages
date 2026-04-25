import os

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st


def _cfg(name: str, default: str = "") -> str:
    if name in st.secrets:
        return str(st.secrets[name])
    return os.getenv(name, default)


@st.cache_data(ttl=60)
def load_steam_reviews() -> pd.DataFrame:
    connection = psycopg2.connect(
        host=_cfg("APP_DB_HOST", "postgres-db"),
        port=int(_cfg("APP_DB_PORT", "5432")),
        user=_cfg("APP_DB_USER", ""),
        password=_cfg("APP_DB_PASSWORD", ""),
        dbname=_cfg("APP_DB_NAME", "rfm"),
    )
    try:
        df = pd.read_sql_query(
            """
            SELECT
                review_id,
                review_text,
                polarity,
                created_at
            FROM public.steam_reviews_sentiment
            ORDER BY created_at DESC;
            """,
            connection,
        )
    finally:
        connection.close()
    return df


st.set_page_config(page_title="Steam Reviews", layout="wide")

header_left, header_right = st.columns([4, 1], vertical_alignment="top")
with header_left:
    st.title("Steam Reviews")
    st.caption("Visualisation des avis recuperees par le DAG Steam.")
with header_right:
    st.markdown("<div style='height: 0.75rem;'></div>", unsafe_allow_html=True)
    refresh_clicked = st.button("Actualiser les donnees", use_container_width=True)

if refresh_clicked:
    load_steam_reviews.clear()

try:
    reviews_df = load_steam_reviews()
except Exception as error:
    st.error(f"Erreur de connexion/lecture PostgreSQL: {error}")
    st.info("Verifie APP_DB_HOST/APP_DB_USER/APP_DB_PASSWORD/APP_DB_NAME.")
    st.stop()

if reviews_df.empty:
    st.warning("Aucune donnee disponible. Lance le DAG steam_reviews_sentiment_dag dans Airflow.")
    st.stop()

reviews_df["created_at"] = pd.to_datetime(reviews_df["created_at"], errors="coerce")
reviews_df["review_length"] = reviews_df["review_text"].fillna("").str.len()

left_col, right_col = st.columns([1, 3], gap="large")
with left_col:
    st.markdown("### Filtres")
    polarity_options = sorted(reviews_df["polarity"].dropna().unique().tolist())
    selected_polarities = st.multiselect(
        "Polarite",
        options=polarity_options,
        default=polarity_options,
    )
    search_text = st.text_input("Mot cle")

filtered = reviews_df.copy()
if selected_polarities:
    filtered = filtered[filtered["polarity"].isin(selected_polarities)]
if search_text:
    filtered = filtered[filtered["review_text"].fillna("").str.contains(search_text, case=False)]

total_reviews = len(filtered)
positive_count = int((filtered["polarity"] == "positive").sum())
negative_count = int((filtered["polarity"] == "negative").sum())
avg_length = float(filtered["review_length"].mean()) if total_reviews else 0.0

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total avis", f"{total_reviews}")
m2.metric("Positifs", f"{positive_count}")
m3.metric("Negatifs", f"{negative_count}")
m4.metric("Longueur moyenne", f"{avg_length:.0f} chars")

with right_col:
    st.markdown("### Repartition des polarites")
    polarity_chart_df = (
        filtered.groupby("polarity", as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )
    if not polarity_chart_df.empty:
        chart = (
            alt.Chart(polarity_chart_df)
            .mark_bar()
            .encode(
                x=alt.X("polarity:N", title="Polarite"),
                y=alt.Y("count:Q", title="Nombre d'avis"),
                tooltip=[
                    alt.Tooltip("polarity:N", title="Polarite"),
                    alt.Tooltip("count:Q", title="Nombre"),
                ],
            )
            .properties(height=260)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Aucune donnee apres filtre.")

st.markdown("---")
st.markdown("### Avis Steam")
st.dataframe(
    filtered[["review_id", "polarity", "created_at", "review_text"]],
    use_container_width=True,
    hide_index=True,
)

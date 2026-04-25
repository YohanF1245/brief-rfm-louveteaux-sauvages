import os

import pandas as pd
import psycopg2
import streamlit as st


def _cfg(name: str, default: str = "") -> str:
    if name in st.secrets:
        return str(st.secrets[name])
    return os.getenv(name, default)


@st.cache_data(ttl=60)
def load_llm_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    connection = psycopg2.connect(
        host=_cfg("APP_DB_HOST", "postgres-db"),
        port=int(_cfg("APP_DB_PORT", "5432")),
        user=_cfg("APP_DB_USER", ""),
        password=_cfg("APP_DB_PASSWORD", ""),
        dbname=_cfg("APP_DB_NAME", "rfm"),
    )
    try:
        analysis_df = pd.read_sql_query(
            """
            SELECT
                a.analysis_id,
                a.review_id,
                a.model_used,
                a.sentiment,
                a.confidence,
                a.criticite,
                a.serieux,
                a.sarcasm_detected,
                a.noise_level,
                a.is_contructive,
                a.created_at,
                a.updated_at,
                r.polarity AS source_polarity,
                r.review_text
            FROM public.review_llm_analysis a
            LEFT JOIN public.steam_reviews_sentiment r
              ON r.review_id = a.review_id
            ORDER BY a.updated_at DESC;
            """,
            connection,
        )
        keywords_df = pd.read_sql_query(
            """
            SELECT
                k.id,
                k.analysis_id,
                k.category,
                k.keyword,
                k.polarity,
                k.created_at
            FROM public.review_llm_analysis_keywords k
            ORDER BY k.id DESC;
            """,
            connection,
        )
    finally:
        connection.close()
    return analysis_df, keywords_df


st.set_page_config(page_title="Steam LLM Review", layout="wide")

left, right = st.columns([4, 1], vertical_alignment="top")
with left:
    st.title("Steam LLM Review")
    st.caption("Visualisation detaillee des analyses LLM par review et par modele.")
with right:
    st.markdown("<div style='height: 0.75rem;'></div>", unsafe_allow_html=True)
    refresh = st.button("Actualiser", use_container_width=True)

if refresh:
    load_llm_data.clear()

try:
    analysis_df, keywords_df = load_llm_data()
except Exception as error:
    st.error(f"Erreur de connexion/lecture PostgreSQL: {error}")
    st.stop()

if analysis_df.empty:
    st.warning("Aucune analyse LLM disponible. Lance d'abord le DAG llm_analysis_dag.")
    st.stop()

col_filters, col_data = st.columns([1, 3], gap="large")

with col_filters:
    st.markdown("### Filtres")
    models = sorted(analysis_df["model_used"].dropna().unique().tolist())
    selected_models = st.multiselect("Modeles", models, default=models)

    sentiments = sorted(analysis_df["sentiment"].dropna().unique().tolist())
    selected_sentiments = st.multiselect("Sentiment LLM", sentiments, default=sentiments)

    review_ids = analysis_df["review_id"].dropna().astype(str).unique().tolist()
    selected_review_id = st.selectbox("Review ID (optionnel)", [""] + sorted(review_ids))

    search_text = st.text_input("Recherche texte review")

filtered = analysis_df.copy()
if selected_models:
    filtered = filtered[filtered["model_used"].isin(selected_models)]
if selected_sentiments:
    filtered = filtered[filtered["sentiment"].isin(selected_sentiments)]
if selected_review_id:
    filtered = filtered[filtered["review_id"].astype(str) == selected_review_id]
if search_text:
    filtered = filtered[filtered["review_text"].fillna("").str.contains(search_text, case=False)]

filtered_keywords = keywords_df[keywords_df["analysis_id"].isin(filtered["analysis_id"])]

m1, m2, m3, m4 = st.columns(4)
m1.metric("Analyses", f"{len(filtered)}")
m2.metric("Reviews couvertes", f"{filtered['review_id'].nunique()}")
m3.metric("Modeles actifs", f"{filtered['model_used'].nunique()}")
m4.metric("Keywords", f"{len(filtered_keywords)}")

with col_data:
    st.markdown("### Resultats LLM")
    st.dataframe(
        filtered[
            [
                "analysis_id",
                "review_id",
                "model_used",
                "source_polarity",
                "sentiment",
                "confidence",
                "criticite",
                "serieux",
                "sarcasm_detected",
                "noise_level",
                "is_contructive",
                "updated_at",
                "review_text",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

st.markdown("---")
st.markdown("### Keywords extraits")
if filtered_keywords.empty:
    st.info("Aucun keyword pour la selection courante.")
else:
    st.dataframe(
        filtered_keywords[["id", "analysis_id", "category", "keyword", "polarity", "created_at"]],
        use_container_width=True,
        hide_index=True,
    )

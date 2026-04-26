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
def load_stats_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    connection = psycopg2.connect(
        host=_cfg("APP_DB_HOST", "postgres-db"),
        port=int(_cfg("APP_DB_PORT", "5432")),
        user=_cfg("APP_DB_USER", ""),
        password=_cfg("APP_DB_PASSWORD", ""),
        dbname=_cfg("APP_DB_NAME", "rfm"),
    )
    try:
        analyses_df = pd.read_sql_query(
            """
            SELECT
                a.analysis_id,
                a.review_id,
                a.model_used,
                a.sentiment,
                a.confidence,
                a.criticite,
                a.serieux,
                a.noise_level,
                a.sarcasm_detected,
                a.is_contructive,
                r.polarity AS source_polarity
            FROM public.review_llm_analysis a
            INNER JOIN public.steam_reviews_sentiment r
              ON r.review_id = a.review_id;
            """,
            connection,
        )
        reviews_df = pd.read_sql_query(
            """
            SELECT
                review_id,
                review_text,
                polarity AS source_polarity,
                created_at
            FROM public.steam_reviews_sentiment;
            """,
            connection,
        )
        try:
            errors_df = pd.read_sql_query(
                """
                SELECT
                    review_id,
                    model_used,
                    code_error,
                    error_text,
                    created_at
                FROM public.review_llm_analysis_errors;
                """,
                connection,
            )
        except Exception:
            errors_df = pd.DataFrame(
                columns=["review_id", "model_used", "code_error", "error_text", "created_at"]
            )
    finally:
        connection.close()
    return analyses_df, reviews_df, errors_df


st.set_page_config(page_title="Steam LLM Stats", layout="wide")
st.title("Steam LLM Stats")
st.caption("Comparaison des modeles: correspondance, variabilite et accords.")

if st.button("Actualiser les donnees"):
    load_stats_data.clear()

try:
    analyses_df, reviews_df, errors_df = load_stats_data()
except Exception as error:
    st.error(f"Erreur de connexion/lecture PostgreSQL: {error}")
    st.stop()

if analyses_df.empty:
    st.warning("Aucune analyse disponible pour calculer les statistiques.")
    st.stop()

REQUIRED_MODELS_COUNT = 6
models = sorted(analyses_df["model_used"].dropna().unique().tolist())
selected_models = st.multiselect("Modeles", options=models, default=models)
show_complete_only = st.toggle(
    f"Ne garder que les commentaires analyses sur {REQUIRED_MODELS_COUNT} LLM",
    value=False,
)
work = (
    analyses_df[analyses_df["model_used"].isin(selected_models)].copy()
    if selected_models
    else analyses_df.copy()
)

present_models = (
    analyses_df.groupby("review_id")["model_used"]
    .agg(lambda values: sorted({str(v) for v in values if pd.notna(v)}))
    .reset_index(name="models_present")
)
present_models["analyses_count"] = present_models["models_present"].apply(len)
review_coverage = reviews_df.merge(present_models, on="review_id", how="left")
review_coverage["models_present"] = review_coverage["models_present"].apply(
    lambda v: v if isinstance(v, list) else []
)
review_coverage["analyses_count"] = review_coverage["analyses_count"].fillna(0).astype(int)
review_coverage["models_missing"] = review_coverage["models_present"].apply(
    lambda present: [m for m in models if m not in present]
)
if errors_df.empty:
    errors_by_review = pd.DataFrame(
        columns=["review_id", "codes_erreur", "errors_count", "last_error_at", "dernier_error_text"]
    )
else:
    errors_df["code_error"] = errors_df["code_error"].astype("Int64")
    errors_by_review = (
        errors_df.sort_values("created_at")
        .groupby("review_id", as_index=False)
        .agg(
            codes_erreur=("code_error", lambda s: sorted({int(v) for v in s.dropna()})),
            errors_count=("review_id", "count"),
            last_error_at=("created_at", "max"),
            dernier_error_text=("error_text", "last"),
        )
    )
    errors_by_review["codes_erreur_txt"] = errors_by_review["codes_erreur"].apply(
        lambda values: ", ".join(str(v) for v in values)
    )
review_coverage = review_coverage.merge(errors_by_review, on="review_id", how="left")
review_coverage["codes_erreur"] = review_coverage["codes_erreur"].apply(
    lambda values: values if isinstance(values, list) else []
)
review_coverage["codes_erreur_txt"] = review_coverage.get("codes_erreur_txt", pd.Series(index=review_coverage.index)).fillna("")
review_coverage["errors_count"] = review_coverage.get("errors_count", pd.Series(index=review_coverage.index)).fillna(0).astype(int)
review_coverage["dernier_error_text"] = review_coverage.get("dernier_error_text", pd.Series(index=review_coverage.index)).fillna("")

if show_complete_only:
    complete_review_ids = review_coverage[
        review_coverage["analyses_count"] >= REQUIRED_MODELS_COUNT
    ]["review_id"]
    work = work[work["review_id"].isin(complete_review_ids)].copy()

if work.empty:
    st.info("Aucune ligne apres filtre.")
    st.stop()

work["is_match_source"] = work["sentiment"] == work["source_polarity"]

per_model = (
    work.groupby("model_used", as_index=False)
    .agg(
        analyses=("analysis_id", "count"),
        reviews=("review_id", "nunique"),
        match_rate=("is_match_source", "mean"),
        confidence_mean=("confidence", "mean"),
        confidence_std=("confidence", "std"),
        criticite_mean=("criticite", "mean"),
        criticite_std=("criticite", "std"),
        serieux_mean=("serieux", "mean"),
        serieux_std=("serieux", "std"),
        noise_level_mean=("noise_level", "mean"),
        sarcasm_rate=("sarcasm_detected", "mean"),
    )
)
per_model["match_pct"] = (per_model["match_rate"] * 100).round(2)
per_model["sarcasm_pct"] = (per_model["sarcasm_rate"] * 100).round(2)

majority_per_review = (
    work.groupby(["review_id", "sentiment"], as_index=False)
    .size()
    .rename(columns={"size": "votes"})
    .sort_values(["review_id", "votes"], ascending=[True, False])
    .drop_duplicates(subset=["review_id"], keep="first")
    .rename(columns={"sentiment": "majority_sentiment"})
)[["review_id", "majority_sentiment"]]

agreement_frame = work.merge(majority_per_review, on="review_id", how="left")
agreement_frame["agrees_with_majority"] = (
    agreement_frame["sentiment"] == agreement_frame["majority_sentiment"]
)
agreement_per_model = (
    agreement_frame.groupby("model_used", as_index=False)["agrees_with_majority"]
    .mean()
    .rename(columns={"agrees_with_majority": "agreement_rate"})
)
agreement_per_model["agreement_pct"] = (agreement_per_model["agreement_rate"] * 100).round(2)

review_std = (
    work.groupby("review_id", as_index=False)
    .agg(
        confidence_std=("confidence", "std"),
        criticite_std=("criticite", "std"),
        serieux_std=("serieux", "std"),
    )
    .fillna(0.0)
)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Analyses totales", f"{len(work)}")
k2.metric("Reviews uniques", f"{work['review_id'].nunique()}")
k3.metric("Modeles compares", f"{work['model_used'].nunique()}")
k4.metric("Match global source", f"{(work['is_match_source'].mean() * 100):.2f}%")

st.markdown("### Correspondance sentiment/source par modele")
bar_match = (
    alt.Chart(per_model)
    .mark_bar()
    .encode(
        x=alt.X("model_used:N", title="Modele", sort="-y"),
        y=alt.Y("match_pct:Q", title="% correspondance source"),
        tooltip=[
            alt.Tooltip("model_used:N", title="Modele"),
            alt.Tooltip("match_pct:Q", title="% correspondance", format=".2f"),
            alt.Tooltip("analyses:Q", title="Analyses"),
        ],
    )
    .properties(height=280)
)
st.altair_chart(bar_match, use_container_width=True)

st.markdown("### Accord avec majorite inter-modeles")
bar_agreement = (
    alt.Chart(agreement_per_model)
    .mark_bar()
    .encode(
        x=alt.X("model_used:N", title="Modele", sort="-y"),
        y=alt.Y("agreement_pct:Q", title="% accord majorite"),
        tooltip=[
            alt.Tooltip("model_used:N", title="Modele"),
            alt.Tooltip("agreement_pct:Q", title="% accord", format=".2f"),
        ],
    )
    .properties(height=280)
)
st.altair_chart(bar_agreement, use_container_width=True)

st.markdown("### Tableau comparatif modeles")
st.dataframe(
    per_model[
        [
            "model_used",
            "analyses",
            "reviews",
            "match_pct",
            "confidence_mean",
            "confidence_std",
            "criticite_mean",
            "criticite_std",
            "serieux_mean",
            "serieux_std",
            "noise_level_mean",
            "sarcasm_pct",
        ]
    ].sort_values("match_pct", ascending=False),
    use_container_width=True,
    hide_index=True,
)

st.markdown("### Variabilite inter-modeles par review (ecart-type)")
st.dataframe(
    review_std.sort_values("confidence_std", ascending=False),
    use_container_width=True,
    hide_index=True,
)

st.markdown(f"### Reviews incompletes (< {REQUIRED_MODELS_COUNT} analyses)")
incomplete_reviews = review_coverage[review_coverage["analyses_count"] < REQUIRED_MODELS_COUNT].copy()
incomplete_reviews["modeles_manquants"] = incomplete_reviews["models_missing"].apply(
    lambda values: ", ".join(values)
)
incomplete_reviews["review_preview"] = incomplete_reviews["review_text"].fillna("").str.slice(0, 200)
error_code_options = sorted(
    {code for codes in incomplete_reviews["codes_erreur"] for code in codes}
)
selected_error_codes = st.multiselect(
    "Filtrer les reviews incompletes par code d'erreur",
    options=error_code_options,
    default=[],
)
if selected_error_codes:
    incomplete_reviews = incomplete_reviews[
        incomplete_reviews["codes_erreur"].apply(
            lambda values: any(code in values for code in selected_error_codes)
        )
    ].copy()

st.dataframe(
    incomplete_reviews[
        [
            "review_id",
            "analyses_count",
            "modeles_manquants",
            "codes_erreur_txt",
            "errors_count",
            "last_error_at",
            "dernier_error_text",
            "review_preview",
            "source_polarity",
            "created_at",
        ]
    ].sort_values(["analyses_count", "review_id"], ascending=[True, True]),
    use_container_width=True,
    hide_index=True,
)

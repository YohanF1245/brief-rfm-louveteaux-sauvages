import os

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st


def _cfg(name: str, default: str = "") -> str:
    if name in st.secrets:
        return str(st.secrets[name])
    return os.getenv(name, default)


_FR_MONTHS = {
    "janv": 1,
    "fevr": 2,
    "fev": 2,
    "mars": 3,
    "avr": 4,
    "mai": 5,
    "juin": 6,
    "juil": 7,
    "aout": 8,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


def _parse_review_date_fr(value: str) -> pd.Timestamp:
    if value is None:
        return pd.NaT
    text = str(value).strip().lower()
    text = (
        text.replace("é", "e")
        .replace("è", "e")
        .replace("ê", "e")
        .replace("à", "a")
        .replace("û", "u")
        .replace(".", "")
    )
    parts = text.split()
    if len(parts) != 3:
        return pd.NaT
    day_raw, month_raw, year_raw = parts
    if not day_raw.isdigit() or not year_raw.isdigit():
        return pd.NaT
    month = _FR_MONTHS.get(month_raw)
    if month is None:
        return pd.NaT
    try:
        return pd.Timestamp(year=int(year_raw), month=month, day=int(day_raw))
    except ValueError:
        return pd.NaT


@st.cache_data(ttl=60)
def load_reviews_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    connection = psycopg2.connect(
        host=_cfg("APP_DB_HOST", "postgres-db"),
        port=int(_cfg("APP_DB_PORT", "5432")),
        user=_cfg("APP_DB_USER", ""),
        password=_cfg("APP_DB_PASSWORD", ""),
        dbname=_cfg("APP_DB_NAME", "rfm"),
    )
    try:
        reviews_df = pd.read_sql_query(
            """
            SELECT
                review_uid,
                author,
                rating,
                review_date_raw,
                experience_date_raw,
                review_text,
                source,
                created_at,
                updated_at
            FROM public.avis_verifies_reviews
            ORDER BY updated_at DESC;
            """,
            connection,
        )
        responses_df = pd.read_sql_query(
            """
            SELECT
                review_uid,
                response_rank,
                response_header_raw,
                response_text_raw,
                created_at,
                updated_at
            FROM public.avis_verifies_review_responses
            ORDER BY updated_at DESC;
            """,
            connection,
        )
    finally:
        connection.close()
    return reviews_df, responses_df


st.set_page_config(page_title="Avis Verifie POC", layout="wide")

header_left, header_right = st.columns([4, 1], vertical_alignment="top")
with header_left:
    st.title("Avis Verifie POC")
    st.caption("Visualisation des avis et des reponses stockes en base.")
with header_right:
    st.markdown("<div style='height: 0.75rem;'></div>", unsafe_allow_html=True)
    refresh_clicked = st.button("Actualiser les donnees", use_container_width=True)

if refresh_clicked:
    load_reviews_data.clear()

try:
    reviews_df, responses_df = load_reviews_data()
except Exception as error:
    st.error(f"Erreur de connexion/lecture PostgreSQL: {error}")
    st.info("Verifie APP_DB_HOST/APP_DB_USER/APP_DB_PASSWORD/APP_DB_NAME.")
    st.stop()

if reviews_df.empty:
    st.warning("Aucune donnee disponible. Lance le DAG de scraping dans Airflow.")
    st.stop()

filtered = reviews_df.copy()
filtered["review_date_parsed"] = filtered["review_date_raw"].apply(_parse_review_date_fr)

left_col, right_col = st.columns([1, 2], gap="large")

with left_col:
    st.markdown("### Filtres")
    st.markdown(
        """
<style>
/* Colore les etiquettes selectionnees du multiselect "Note" */
div[data-baseweb="tag"]:nth-of-type(1) { background-color: #b91c1c !important; color: white !important; }
div[data-baseweb="tag"]:nth-of-type(2) { background-color: #ea580c !important; color: white !important; }
div[data-baseweb="tag"]:nth-of-type(3) { background-color: #f59e0b !important; color: black !important; }
div[data-baseweb="tag"]:nth-of-type(4) { background-color: #65a30d !important; color: white !important; }
div[data-baseweb="tag"]:nth-of-type(5) { background-color: #16a34a !important; color: white !important; }
</style>
""",
        unsafe_allow_html=True,
    )
    rating_values = sorted(filtered["rating"].dropna().unique().tolist())
    rating_filter = st.multiselect("1) Note", options=rating_values, default=rating_values)
    search_text = st.text_input("2) Mot cle")

    min_date = filtered["review_date_parsed"].min()
    max_date = filtered["review_date_parsed"].max()
    start_date = None
    end_date = None
    if pd.notna(min_date) and pd.notna(max_date):
        c_start, c_end = st.columns(2)
        with c_start:
            start_date = st.date_input(
                "Date debut",
                value=min_date.date(),
                min_value=min_date.date(),
                max_value=max_date.date(),
            )
        with c_end:
            end_date = st.date_input(
                "Date fin",
                value=max_date.date(),
                min_value=min_date.date(),
                max_value=max_date.date(),
            )
        if start_date > end_date:
            st.warning("La date debut doit etre <= date fin.")
    else:
        st.info("Dates d'avis non disponibles pour le filtre.")

if rating_filter:
    filtered = filtered[filtered["rating"].isin(rating_filter)]
if search_text:
    filtered = filtered[filtered["review_text"].fillna("").str.contains(search_text, case=False)]

if start_date is not None and end_date is not None and start_date <= end_date:
    filtered = filtered[
        filtered["review_date_parsed"].between(
            pd.Timestamp(start_date),
            pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1),
        )
    ]

filtered_responses = responses_df[responses_df["review_uid"].isin(filtered["review_uid"])]
filtered_total_reviews = len(filtered)
filtered_total_responses = len(filtered_responses)
filtered_avg_rating = filtered["rating"].dropna().mean() if filtered_total_reviews else 0
filtered_responded_reviews = filtered_responses["review_uid"].nunique() if filtered_total_responses else 0
filtered_response_rate = (
    filtered_responded_reviews / filtered_total_reviews * 100 if filtered_total_reviews else 0
)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total avis", f"{filtered_total_reviews}")
c2.metric("Total reponses", f"{filtered_total_responses}")
c3.metric("Note moyenne", f"{filtered_avg_rating:.2f}/5" if filtered_total_reviews else "0.00/5")
c4.metric("Taux de reponse", f"{filtered_response_rate:.1f}%")

with right_col:
    st.markdown("### Evolution hebdomadaire des notes")
    dated = filtered.dropna(subset=["review_date_parsed", "rating"]).copy()
    if not dated.empty:
        last_date = dated["review_date_parsed"].max()
        week_start = last_date - pd.Timedelta(days=7)
        last_week_avg = dated.loc[dated["review_date_parsed"] >= week_start, "rating"].mean()
        st.metric("Moyenne note (7 derniers jours)", f"{last_week_avg:.2f}/5")

        weekly_ratings = (
            dated.set_index("review_date_parsed")
            .resample("W-MON")["rating"]
            .mean()
            .reset_index()
            .rename(columns={"review_date_parsed": "semaine", "rating": "note_moyenne"})
        )
        min_note = float(weekly_ratings["note_moyenne"].min())
        max_note = float(weekly_ratings["note_moyenne"].max())
        y_min = max(0.0, min_note - 1.0)
        y_max = min(6.0, max_note + 1.0)

        chart = (
            alt.Chart(weekly_ratings)
            .mark_line()
            .encode(
                x=alt.X("semaine:T", title="Semaine"),
                y=alt.Y(
                    "note_moyenne:Q",
                    title="Note moyenne",
                    scale=alt.Scale(domain=[y_min, y_max], nice=False, clamp=True),
                ),
                tooltip=[
                    alt.Tooltip("semaine:T", title="Semaine"),
                    alt.Tooltip("note_moyenne:Q", title="Note", format=".2f"),
                ],
            )
            .properties(height=260)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Pas assez de dates exploitables pour calculer l'evolution des notes.")

st.markdown("---")
st.markdown("### Avis")
st.dataframe(
    filtered[
        [
            "review_uid",
            "author",
            "rating",
            "review_date_raw",
            "experience_date_raw",
            "review_text",
            "updated_at",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)

st.markdown("### Reponses")
if filtered_total_responses:
    st.dataframe(
        filtered_responses[
            [
                "review_uid",
                "response_rank",
                "response_header_raw",
                "response_text_raw",
                "updated_at",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("Aucune reponse trouvee pour le moment.")

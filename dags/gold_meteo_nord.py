"""Couche gold : vues d'analyse sur public.climat_data (après le DAG meteo_nord)."""

from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator

CONN_ID = "DATA-DB"
SCHEMA = "public"

V_TEMP_MAX_PAR_STATION = f"""
CREATE OR REPLACE VIEW {SCHEMA}.v_temp_max_par_station AS
SELECT
    station_id,
    MAX(station_nom) AS station_nom,
    MAX(temp_max) AS temp_max_record,
    MAX(latitude) AS latitude,
    MAX(longitude) AS longitude
FROM {SCHEMA}.climat_data
WHERE temp_max IS NOT NULL
GROUP BY station_id;
"""

INDEX_DDL = [
    f"""
    CREATE INDEX IF NOT EXISTS idx_climat_station_date
        ON {SCHEMA}.climat_data (station_id, date_mesure);
    """,
    f"""
    CREATE INDEX IF NOT EXISTS idx_climat_station_temp
        ON {SCHEMA}.climat_data (station_id, temp_max DESC NULLS LAST, date_mesure DESC);
    """,
]

V_CLIMAT_EVOLUTION_ANNEE = f"""
CREATE OR REPLACE VIEW {SCHEMA}.v_climat_evolution_annee AS
WITH daily AS (
    SELECT
        station_id,
        station_nom,
        date_mesure::date AS date_mesure,
        EXTRACT(YEAR FROM date_mesure)::int AS annee,
        EXTRACT(MONTH FROM date_mesure)::int AS mois,
        EXTRACT(DAY FROM date_mesure)::int AS jour,
        temp_max
    FROM {SCHEMA}.climat_data
    WHERE temp_max IS NOT NULL
),
record_jour AS (
    SELECT DISTINCT ON (station_id, mois, jour)
        station_id,
        mois,
        jour,
        temp_max AS temp_max_record,
        annee AS annee_record
    FROM daily
    ORDER BY station_id, mois, jour, temp_max DESC, annee DESC
)
SELECT
    d.station_id,
    d.station_nom,
    d.annee,
    d.date_mesure,
    d.mois,
    d.jour,
    d.temp_max AS temp_max_n,
    n1.temp_max AS temp_max_n1,
    r.temp_max_record,
    r.annee_record
FROM daily d
LEFT JOIN daily n1
    ON d.station_id = n1.station_id
   AND d.mois = n1.mois
   AND d.jour = n1.jour
   AND n1.annee = d.annee - 1
LEFT JOIN record_jour r
    ON d.station_id = r.station_id
   AND d.mois = r.mois
   AND d.jour = r.jour;
"""

V_CLIMAT_NORMALE_JOUR = f"""
CREATE OR REPLACE VIEW {SCHEMA}.v_climat_normale_jour AS
SELECT
    station_id,
    MAX(station_nom) AS station_nom,
    EXTRACT(MONTH FROM date_mesure)::int AS mois,
    EXTRACT(DAY FROM date_mesure)::int AS jour,
    ROUND(AVG(temp_min)::numeric, 2) AS tn_moy,
    ROUND(MIN(temp_min)::numeric, 2) AS tn_min_hist,
    ROUND(MAX(temp_min)::numeric, 2) AS tn_max_hist,
    ROUND(AVG(temp_max)::numeric, 2) AS tx_moy,
    ROUND(MIN(temp_max)::numeric, 2) AS tx_min_hist,
    ROUND(MAX(temp_max)::numeric, 2) AS tx_max_hist,
    COUNT(temp_min) AS nb_annees_tn,
    COUNT(temp_max) AS nb_annees_tx,
    MAX(latitude) AS latitude,
    MAX(longitude) AS longitude
FROM {SCHEMA}.climat_data
WHERE temp_max IS NOT NULL
GROUP BY
    station_id,
    EXTRACT(MONTH FROM date_mesure),
    EXTRACT(DAY FROM date_mesure);
"""

V_CLIMAT_JOUR_STATION = f"""
CREATE OR REPLACE VIEW {SCHEMA}.v_climat_jour_station AS
SELECT
    c.station_id,
    MAX(c.station_nom) AS station_nom,
    c.date_mesure::date AS date_mesure,
    EXTRACT(YEAR FROM c.date_mesure)::int AS annee,
    EXTRACT(MONTH FROM c.date_mesure)::int AS mois,
    EXTRACT(DAY FROM c.date_mesure)::int AS jour,
    c.temp_min,
    c.temp_max,
    n.tn_moy AS tn_moy_normale,
    n.tx_moy AS tx_moy_normale,
    ROUND((c.temp_min - n.tn_moy)::numeric, 2) AS ecart_tn_normale,
    ROUND((c.temp_max - n.tx_moy)::numeric, 2) AS ecart_tx_normale
FROM {SCHEMA}.climat_data c
LEFT JOIN {SCHEMA}.v_climat_normale_jour n
  ON n.station_id = c.station_id
 AND n.mois = EXTRACT(MONTH FROM c.date_mesure)::int
 AND n.jour = EXTRACT(DAY FROM c.date_mesure)::int
WHERE c.temp_max IS NOT NULL
GROUP BY
    c.station_id,
    c.date_mesure,
    c.temp_min,
    c.temp_max,
    n.tn_moy,
    n.tx_moy;
"""

# Seuils vs normale du jour calendaire (même station).
SEUIL_ECART_CHAUD = 2.0
SEUIL_ECART_FROID = -2.0

V_CLIMAT_PLUIE_MOIS = f"""
CREATE OR REPLACE VIEW {SCHEMA}.v_climat_pluie_mois AS
SELECT
    station_id,
    MAX(station_nom) AS station_nom,
    EXTRACT(YEAR FROM date_mesure)::int AS annee,
    EXTRACT(MONTH FROM date_mesure)::int AS mois,
    ROUND(SUM(quantite_precipitations)::numeric, 1) AS rr_sum_mm,
    COUNT(*) FILTER (WHERE quantite_precipitations > 0) AS nb_jours_pluie,
    MAX(latitude) AS latitude,
    MAX(longitude) AS longitude
FROM {SCHEMA}.climat_data
WHERE quantite_precipitations IS NOT NULL
GROUP BY
    station_id,
    EXTRACT(YEAR FROM date_mesure),
    EXTRACT(MONTH FROM date_mesure);
"""

V_CLIMAT_PLUIE_ANNEE = f"""
CREATE OR REPLACE VIEW {SCHEMA}.v_climat_pluie_annee AS
SELECT
    station_id,
    MAX(station_nom) AS station_nom,
    annee,
    ROUND(SUM(rr_sum_mm)::numeric, 1) AS rr_sum_annuel,
    SUM(nb_jours_pluie)::int AS nb_jours_pluie,
    MAX(latitude) AS latitude,
    MAX(longitude) AS longitude
FROM {SCHEMA}.v_climat_pluie_mois
GROUP BY station_id, annee;
"""

V_CLIMAT_CHAUD_FROID_ANNEE = f"""
CREATE OR REPLACE VIEW {SCHEMA}.v_climat_chaud_froid_annee AS
SELECT
    j.station_id,
    MAX(j.station_nom) AS station_nom,
    j.annee,
    COUNT(*) FILTER (WHERE j.ecart_tx_normale > {SEUIL_ECART_CHAUD}) AS nb_jours_chauds,
    COUNT(*) FILTER (WHERE j.ecart_tn_normale < {SEUIL_ECART_FROID}) AS nb_jours_froids,
    MAX(n.latitude) AS latitude,
    MAX(n.longitude) AS longitude
FROM {SCHEMA}.v_climat_jour_station j
LEFT JOIN (
    SELECT station_id, MAX(latitude) AS latitude, MAX(longitude) AS longitude
    FROM {SCHEMA}.v_climat_normale_jour
    GROUP BY station_id
) n ON n.station_id = j.station_id
GROUP BY j.station_id, j.annee;
"""

# Ordre de création : les vues dépendantes en dernier.
GOLD_VIEW_ORDER = (
    "v_temp_max_par_station",
    "v_climat_evolution_annee",
    "v_climat_normale_jour",
    "v_climat_pluie_mois",
    "v_climat_jour_station",
    "v_climat_pluie_annee",
    "v_climat_chaud_froid_annee",
)

GOLD_VIEWS = {
    "v_temp_max_par_station": V_TEMP_MAX_PAR_STATION,
    "v_climat_evolution_annee": V_CLIMAT_EVOLUTION_ANNEE,
    "v_climat_normale_jour": V_CLIMAT_NORMALE_JOUR,
    "v_climat_pluie_mois": V_CLIMAT_PLUIE_MOIS,
    "v_climat_jour_station": V_CLIMAT_JOUR_STATION,
    "v_climat_pluie_annee": V_CLIMAT_PLUIE_ANNEE,
    "v_climat_chaud_froid_annee": V_CLIMAT_CHAUD_FROID_ANNEE,
}


def refresh_gold_views(**context) -> None:
    """Recrée les vues gold (DROP puis CREATE) pour rester rejouable."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            for ddl in INDEX_DDL:
                cur.execute(ddl)
            for name in reversed(GOLD_VIEW_ORDER):
                cur.execute(f"DROP VIEW IF EXISTS {SCHEMA}.{name} CASCADE;")
                print(f"Vue {SCHEMA}.{name} supprimée si existante.")
            for name in GOLD_VIEW_ORDER:
                cur.execute(GOLD_VIEWS[name])
                print(f"Vue {SCHEMA}.{name} créée.")
        conn.commit()
    finally:
        conn.close()


with DAG(
    dag_id="gold_meteo_nord",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["meteo", "postgres", "gold"],
    doc_md="Exécuter après `meteo_nord` pour publier les vues d'analyse.",
) as dag:
    refresh_gold_views_task = PythonOperator(
        task_id="refresh_gold_views",
        python_callable=refresh_gold_views,
    )

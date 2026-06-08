"""
Climat Nord (59) : documentation ETL Airflow + vues gold + visualisations Streamlit.
"""

from __future__ import annotations

import html as html_lib
import re

import streamlit as st
import streamlit.components.v1 as components

MERMAID_ARCH = """
flowchart LR
  SRC[".gz / data.gouv"] --> MN["meteo_nord"]
  SRC --> MQ["meteo_nord_quotidien"]
  MN --> CD["climat_data"]
  MQ --> CD
  CD --> GOLD["gold_meteo_nord"]
  GOLD --> V["vues SQL"]
  V --> ST["Streamlit 050"]
  CD --> ST
"""

MERMAID_LINEAGE = """
flowchart TB
  CD[(climat_data)]

  CD --> VTM[v_temp_max_par_station]
  CD --> VEV[v_climat_evolution_annee]
  CD --> VNJ[v_climat_normale_jour]
  CD --> VPM[v_climat_pluie_mois]

  VNJ --> VJS[v_climat_jour_station]
  VJS --> VCF[v_climat_chaud_froid_annee]
  VNJ -.-> VCF

  VPM --> VPA[v_climat_pluie_annee]

  CD --> ST[050 analyse]
  VTM --> ST
  VEV --> ST
  VNJ --> ST
  VJS --> ST
  VPM --> ST
  VPA --> ST
  VCF --> ST
"""

st.set_page_config(page_title="Climat Nord — Documentation", layout="wide")

TITLE_COLOR = "#1a5276"
HEADER_COLOR = "#117a65"

st.markdown(
    f"""
    <style>
      h1.st-doc-title {{ color: {TITLE_COLOR}; margin-bottom: 0.2rem; }}
      h2.st-doc-header {{ color: {HEADER_COLOR}; margin-top: 1.1rem; }}
      .st-doc-toc a {{ text-decoration: none; }}
      .st-doc-toc a:hover {{ text-decoration: underline; }}
      code {{ font-size: 0.9em; }}
    </style>
    """,
    unsafe_allow_html=True,
)


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", text.strip().lower()).strip("-")
    return slug or "section"


def title_anchor(text: str, anchor_id: str | None = None) -> None:
    anchor = anchor_id or _slugify(text)
    st.markdown(
        f'<h1 id="{anchor}" class="st-doc-title">{text}</h1>',
        unsafe_allow_html=True,
    )


def header_anchor(text: str, anchor_id: str | None = None) -> None:
    anchor = anchor_id or _slugify(text)
    st.markdown(
        f'<h2 id="{anchor}" class="st-doc-header">{text}</h2>',
        unsafe_allow_html=True,
    )


def _mermaid(diagram: str, height: int = 220) -> None:
    """Rendu Mermaid via mermaid.js (Streamlit ne parse pas les blocs ```mermaid)."""
    code = html_lib.escape(diagram.strip())
    components.html(
        f"""
<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8" />
  <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
  <script>
    mermaid.initialize({{
      startOnLoad: true,
      theme: "dark",
      flowchart: {{ useMaxWidth: true, htmlLabels: true }},
    }});
  </script>
  <style>
    body {{
      margin: 0;
      padding: 12px 8px;
      background: transparent;
      overflow: auto;
    }}
    .mermaid {{
      display: flex;
      justify-content: center;
    }}
  </style>
</head>
<body>
  <pre class="mermaid">{code}</pre>
</body>
</html>
        """,
        height=height,
        scrolling=True,
    )


title_anchor("Climat Nord — Documentation ETL & visualisation", anchor_id="climat-nord-doc")

st.markdown(
    """
<div class="st-doc-toc">

- [Objectif et source](#objectif-et-source)
- [Architecture](#architecture)
- [Pipeline ETL (DAGs)](#pipeline-etl)
- [Table `climat_data`](#table-climat-data)
- [Couche gold (vues SQL)](#couche-gold)
- [Lineage des vues](#lineage-vues)
- [Page d'analyse Streamlit](#page-analyse)
- [Qualité des données](#qualite-donnees)
- [Exploitation au quotidien](#exploitation)

</div>
""",
    unsafe_allow_html=True,
)

try:
    st.page_link(
        "pages/050_CLIMAT_NORD_ANALYSE.py",
        label="Ouvrir la page d'analyse (050)",
    )
except Exception:
    st.info("La page d'analyse est **050 — Climat Nord Analyse** dans le menu latéral.")

header_anchor("Objectif et source", anchor_id="objectif-et-source")
st.markdown(
    """
Ce pipeline ingère les **données quotidiennes Météo-France** au format **RR-T-Vent**
(précipitations, températures min/max, gel, coordonnées) pour le **département du Nord (59)**.

**Sources :**
- **Chargement initial** : fichiers locaux `dags/data/raw_climat_data/`
  - `50-24.gz` — historique 1950–2024
  - `25-26.gz` — période récente (ex. 2025–2026)
- **Mise à jour quotidienne** : ressource [data.gouv.fr](https://www.data.gouv.fr/)
  `Q_59_latest-2025-2026_RR-T-Vent.csv.gz` (DAG `meteo_nord_quotidien`, 7 h).

**Public cible de la viz :** comprendre la variabilité thermique et pluviométrique du réseau MF
sur le 59 (~101 postes, dont ~28 avec thermométrie complète).
"""
)

header_anchor("Architecture", anchor_id="architecture")
st.markdown(
    """
| Composant | Rôle |
|-----------|------|
| **PostgreSQL `postgres-db`** | Base applicative `rfm`, table `public.climat_data` + vues gold |
| **Airflow** (connexion **`DATA-DB`**) | Ingestion, transformation, upsert, création des vues |
| **Streamlit** (secrets **`APP_DB_*`**) | Lecture seule des vues pour les graphiques |
| **`meteo_nord_common.py`** | Transformations et upsert partagés entre DAGs |

Les DAG **écrivent** ; Streamlit **lit** la même base (souvent via le service Docker `postgres-db:5432`).
"""
)

_mermaid(MERMAID_ARCH, height=200)
st.caption("Schéma rendu avec Mermaid.js (CDN) — pas d’équivalent natif Streamlit / GitHub Markdown.")

header_anchor("Pipeline ETL (DAGs)", anchor_id="pipeline-etl")

st.markdown("#### 1. `meteo_nord` — chargement initial")
st.markdown(
    """
| Étape | Détail |
|-------|--------|
| **Entrée** | Fusion des deux `.gz` locaux |
| **Transform** | `transform_dataframe()` : renommage colonnes MF, dates, `temp_min_at` / `temp_max_at` |
| **Chargement** | **`DROP TABLE` + `CREATE` + `INSERT`** sur `public.climat_data` |
| **Planification** | Manuel (`schedule=None`) |

À lancer **une fois** (ou pour repartir de zéro). **Destructif** : efface la table existante.
"""
)

st.markdown("#### 2. `meteo_nord_quotidien` — rafraîchissement")
st.markdown(
    """
| Étape | Détail |
|-------|--------|
| **Téléchargement** | API data.gouv → fichier `Q_59_latest-2025-2026_RR-T-Vent.csv.gz` |
| **Transform** | Même logique que l'initial |
| **Chargement** | **`UPSERT`** sur `(station_id, date_mesure)` — pas de DROP |
| **Planification** | `0 7 * * *` (tous les jours à 7 h) |

Permet d'avoir le **dernier jour** publié par Météo-France sans recharger tout l'historique.
"""
)

st.markdown("#### 3. `gold_meteo_nord` — vues d'analyse")
st.markdown(
    """
| Étape | Détail |
|-------|--------|
| **Index** | `idx_climat_station_date`, `idx_climat_station_temp` |
| **Vues** | `DROP VIEW … CASCADE` puis `CREATE` (rejouable si le schéma change) |
| **Planification** | Manuel — à exécuter **après** chaque gros changement de données ou de définition de vue |

**Important :** si Streamlit affiche « vue absente », c'est en général que ce DAG n'a pas été relancé
depuis l'ajout de nouvelles vues (`v_climat_pluie_*`, `v_climat_chaud_froid_annee`, etc.).
"""
)

header_anchor("Table `climat_data`", anchor_id="table-climat-data")
st.markdown(
    """
Grain : **une ligne par poste (`station_id`) et par jour (`date_mesure`)**.

| Colonne | Description |
|---------|-------------|
| `station_id`, `station_nom` | Identifiant et libellé MF (`NUM_POSTE`, `NOM_USUEL`) |
| `date_mesure` | Jour de mesure |
| `temp_min`, `temp_max` | TN / TX (°C) — souvent `NULL` sur postes **pluvio seuls** |
| `quantite_precipitations` | RR (mm) |
| `temp_min_at`, `temp_max_at` | Horodatage des extrêmes |
| `duree_gel` | Durée de gel |
| `latitude`, `longitude`, `altitude` | Métadonnées poste |
| `qualite_*` | Codes qualité MF (≠ valeur météo ; ex. `0` = code, pas 0 °C) |

Clé primaire : `(station_id, date_mesure)`.
"""
)

header_anchor("Couche gold (vues SQL)", anchor_id="couche-gold")
st.markdown(
    """
| Vue | Usage principal |
|-----|-----------------|
| `v_temp_max_par_station` | Record TX absolu + coordonnées (carte des records) |
| `v_climat_evolution_annee` | TX jour J vs N-1 vs record calendaire (courbes) |
| `v_climat_normale_jour` | Moyenne TN/TX par **(station, mois, jour)** + min/max historiques |
| `v_climat_jour_station` | Jour + écarts TN/TX vs normale du jour calendaire |
| `v_climat_pluie_mois` | Cumul mensuel RR + nb jours avec pluie |
| `v_climat_pluie_annee` | Cumul annuel RR (agrégat mensuel) |
| `v_climat_chaud_froid_annee` | Comptage jours **chauds** (écart TX > +2 °C) et **froids** (écart TN < −2 °C) |

Fichier source : `dags/gold_meteo_nord.py`.
"""
)

header_anchor("Lineage des vues", anchor_id="lineage-vues")
st.markdown(
    """
Dépendances entre la table brute et les vues (ordre de création dans `gold_meteo_nord`).
"""
)
_mermaid(MERMAID_LINEAGE, height=480)
st.caption(
    "Flèche pleine : `SELECT` direct. Pointillé : jointure coordonnées via `v_climat_normale_jour`."
)

header_anchor("Page d'analyse Streamlit", anchor_id="page-analyse")
st.markdown(
    f"""
La page **[050 — Climat Nord Analyse](pages/050_CLIMAT_NORD_ANALYSE.py)** (`050_CLIMAT_NORD_ANALYSE.py`)
interroge Postgres via `APP_DB_*` (cache `@st.cache_data`, TTL 30–120 s).

| Section | Données | Visualisation |
|---------|---------|---------------|
| **KPIs dernier jour** | `climat_data` + `v_climat_normale_jour` | Métriques réseau (min/max du jour vs normales) |
| **Records TX** | `v_temp_max_par_station` | Carte choroplèthe |
| **Normale calendaire** | `v_climat_normale_jour` | Barres TN/TX + fourchette min–max par station |
| **Calendrier thermique** | `v_climat_jour_station` | Heatmap semaine ISO × jour |
| **Précipitations** | `v_climat_pluie_mois`, `v_climat_pluie_annee` | Barres mensuelles + carte cumul annuel |
| **Jours chauds / froids** | `v_climat_chaud_froid_annee` | Barres groupées + carte |
| **Évolution** | `v_climat_evolution_annee` | Courbes N / N-1 / record |

**Détails UX :**
- Layout **wide** ; thème sombre dans `streamlit/.streamlit/config.toml`
- Libellés station `NOM (id)` si doublons (ex. plusieurs postes « STEENVOORDE »)
- Bouton **Actualiser** pour vider le cache après un run Airflow
"""
)

header_anchor("Qualité des données", anchor_id="qualite-donnees")
st.markdown(
    """
Script local : `scripts/check_climat_data_quality.py`

- Compare les **fichiers `.gz` bruts** à la transformation type DAG
- Option `--postgres` : contrôle la table chargée
- Environ **67 % de TN/TX NULL** sur le brut = postes **pluviométriques sans thermométrie**,
  pas un bug d'ingestion

Avant d'interpréter une « moyenne réseau », filtrer les postes avec `temp_max IS NOT NULL`.
"""
)

header_anchor("Exploitation au quotidien", anchor_id="exploitation")
st.markdown(
    """
**Ordre recommandé :**

1. `meteo_nord` — premier remplissage (ou reset complet)
2. `meteo_nord_quotidien` — chaque jour (ou manuel pour rattraper)
3. `gold_meteo_nord` — après ingestion ou changement de vues
4. Streamlit **050** → **Actualiser**

**Dépannage rapide :**

| Symptôme | Action |
|----------|--------|
| Vues introuvables | Relancer `gold_meteo_nord` |
| Dernier jour en retard | Lancer `meteo_nord_quotidien` (pas `meteo_daily`) |
| KPI / graphiques figés | Bouton Actualiser ou *Clear cache* Streamlit |
| Erreur `cannot change name of view column` | Déjà corrigé : le gold fait DROP + CREATE |

**Connexions :**
- Airflow : `DATA-DB` → `postgresql://…@postgres-db:5432/rfm`
- Streamlit : `APP_DB_HOST=postgres-db`, même base `rfm`
"""
)

st.divider()
st.caption(
    "Documentation projet brief RFM — pipeline climat Nord. "
    "Données : Météo-France / data.gouv.fr — usage pédagogique."
)

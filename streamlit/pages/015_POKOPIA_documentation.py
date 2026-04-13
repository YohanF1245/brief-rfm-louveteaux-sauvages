"""
Pokopia : documentation stack + schéma ER (Graphviz). Sans accès PostgreSQL.
"""

from __future__ import annotations

from pathlib import Path

import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="RFM Documentation — Pokopia (doc)",
    layout="wide",
)

_DOT_PATH = Path(__file__).resolve().parents[1] / "data" / "pokopia_er.dot"

# Styles page : éviter que le premier bloc (iframe hero) soit rogné sous la barre Streamlit.
_PAGE_STYLE = """
<style>
  .block-container { padding-top: 1.5rem !important; }
  [data-testid="stAppViewContainer"] .main .block-container {
    padding-top: 1.5rem !important;
  }
</style>
"""


def _hero_html() -> str:
    return """
<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8" />
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/animate.css/4.1.1/animate.min.css" />
  <link href="https://fonts.googleapis.com/css2?family=Manrope:wght@600;800&display=swap" rel="stylesheet">
  <style>
    html { box-sizing: border-box; }
    *, *::before, *::after { box-sizing: inherit; }
    body {
      margin: 0;
      font-family: Manrope, sans-serif;
      background: linear-gradient(125deg,#090f22,#1a0f38 50%,#0b2238);
      color: #e8ecff;
      padding: 28px 22px 22px;
      border-radius: 20px;
      overflow: visible;
    }
    .titre {
      font-size: clamp(1.5rem, 3.8vw, 2.45rem);
      font-weight: 800;
      line-height: 1.2;
      margin: 0 0 8px;
      padding: 6px 0 2px;
      background: linear-gradient(90deg,#ff4fd8,#64f9ff,#ffd86b);
      background-clip: text;
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }
    .soustitre { margin: 0; color: #9fb4ff; letter-spacing: .12em; font-size: .78rem;
      text-transform: uppercase; }
    .badge { display: inline-block; margin-top: 12px; padding: 6px 12px; border-radius: 999px;
      border: 1px solid rgba(255,255,255,.2); font-size: .75rem; color: #dffcff;
      background: rgba(100,249,255,.08); }
  </style>
</head>
<body>
  <h1 class="titre animate__animated animate__fadeIn">Pokopia — Documentation</h1>
  <p class="soustitre animate__animated animate__fadeIn animate__delay-1s">
    Stack, modèle de données, rappels légaux
  </p>
  <span class="badge animate__animated animate__pulse animate__infinite animate__slow">
    Onglet séparé du planificateur (page suivante dans le menu)
  </span>
</body>
</html>
"""


st.markdown(_PAGE_STYLE, unsafe_allow_html=True)

# Hauteur suffisante + pas de fade depuis le haut (évite lettres coupées dans l’iframe).
components.html(_hero_html(), height=248, scrolling=False)

st.markdown("## Stack (aperçu)")
st.markdown(
    """
- **Scraping** Serebii (fiches Pokopia) pour numéro, nom, spécialités, habitat idéal, favoris — **à but scolaire uniquement** (projet d’apprentissage ; ne pas répliquer en production sans respecter le site source et le cadre légal).
- **CSV** interne pour les thèmes cadeaux et les objets (combos « opti » par thème).
- **DAG Airflow** `pokopia_scrape_dag` qui fusionne les deux sources et **alimente PostgreSQL** (schéma ``pokopia``).

### Droits PostgreSQL (à la main)

- Le **DAG** (connexion **DATA-DB**) crée et remplit le schéma sous le **rôle Postgres** associé à cette connexion (chez toi : le login réel généré par ta plateforme, à ne pas commiter dans la doc).
- **Streamlit** lit la même base avec un **autre** rôle (celui de **APP_DB_***, lecture seule sur `pokopia`).

À exécuter **une fois** (ou après changement de schéma), connecté en superuser ou en **propriétaire** des objets. Remplace `pokopia_dag_owner` et `pokopia_app_reader` par **tes** noms de rôles ; en cas de tirets ou caractères spéciaux, entoure-les de **guillemets doubles** en SQL.

```sql
GRANT USAGE ON SCHEMA pokopia TO pokopia_app_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA pokopia TO pokopia_app_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE pokopia_dag_owner IN SCHEMA pokopia
  GRANT SELECT ON TABLES TO pokopia_app_reader;
```

Le DAG ne fait **aucun** GRANT automatique : il se contente de **TRUNCATE + INSERT** pour que ces droits restent valides d’un run à l’autre.

### Entités (résumé)

| Entité | Rôle |
|--------|------|
| `pokemon` | Espèce : numéro Pokopia, nom, FK habitat idéal. |
| `specialty`, `ideal_habitat`, `favorite` | Référentiels. |
| `pokemon_specialty`, `pokemon_favorite` | Liaisons N–N (+ `ordre`). |
| `gift_theme`, `category`, `type`, `item` | Données objets / CSV. |
"""
)

st.divider()
st.markdown("### Modèle relationnel (Graphviz)")

if not _DOT_PATH.is_file():
    st.warning(f"Fichier DOT introuvable : `{_DOT_PATH}`.")
else:
    dot_src = _DOT_PATH.read_text(encoding="utf-8")
    try:
        st.graphviz_chart(dot_src)
    except Exception as e:
        st.error(f"Impossible de rendre le graphe (Graphviz / binaire « dot ») : {e}")
        st.code(dot_src, language="dot")

st.caption(
    "Fichier source du graphe : `streamlit/data/pokopia_er.dot` — description statique du schéma `pokopia`."
)

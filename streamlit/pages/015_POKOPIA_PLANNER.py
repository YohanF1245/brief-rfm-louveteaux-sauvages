"""
Planificateur Pokopia : schéma Graphviz + données lues depuis PostgreSQL (schéma pokopia).
"""

from __future__ import annotations

import os
from collections import defaultdict
from html import escape
from pathlib import Path

import psycopg2
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="RFM Documentation — Pokopia Planner",
    layout="wide",
)

_DOT_PATH = Path(__file__).resolve().parents[1] / "data" / "pokopia_er.dot"


def _cfg(name: str, default: str = "") -> str:
    if name in st.secrets:
        return str(st.secrets[name])
    return os.getenv(name, default)


def _db_conn():
    return psycopg2.connect(
        host=_cfg("APP_DB_HOST", "postgres-db"),
        port=int(_cfg("APP_DB_PORT", "5432")),
        user=_cfg("APP_DB_USER", ""),
        password=_cfg("APP_DB_PASSWORD", ""),
        dbname=_cfg("APP_DB_NAME", "rfm"),
    )


@st.cache_data(ttl=60, show_spinner=False)
def _load_pokemon_rows() -> list[dict]:
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT nom, num
                FROM pokopia.pokemon
                ORDER BY num NULLS LAST, nom
                """
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return [{"nom": r[0], "num": r[1]} for r in rows]


@st.cache_data(ttl=60, show_spinner=False)
def _load_favorites_map(noms: tuple[str, ...]) -> dict[str, list[str]]:
    if not noms:
        return {}
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pokemon_nom, favorite_valeur
                FROM pokopia.pokemon_favorite
                WHERE pokemon_nom = ANY(%s)
                ORDER BY pokemon_nom, ordre
                """,
                (list(noms),),
            )
            raw = cur.fetchall()
    finally:
        conn.close()
    out: dict[str, list[str]] = defaultdict(list)
    for nom, fav in raw:
        out[nom].append(fav)
    return dict(out)


def _hero_html() -> str:
    return """
<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8" />
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/animate.css/4.1.1/animate.min.css" />
  <link href="https://fonts.googleapis.com/css2?family=Manrope:wght@600;800&display=swap" rel="stylesheet">
  <style>
    body { margin:0; font-family:Manrope, sans-serif;
      background: linear-gradient(125deg,#090f22,#1a0f38 50%,#0b2238);
      color:#e8ecff; padding:16px 20px; border-radius:20px; }
    .titre {
      font-size:clamp(1.6rem,4vw,2.6rem); font-weight:800; margin:0 0 6px;
      background:linear-gradient(90deg,#ff4fd8,#64f9ff,#ffd86b);
      -webkit-background-clip:text; -webkit-text-fill-color:transparent;
    }
    .soustitre { margin:0; color:#9fb4ff; letter-spacing:.12em; font-size:.78rem;
      text-transform:uppercase; }
    .badge { display:inline-block; margin-top:12px; padding:6px 12px; border-radius:999px;
      border:1px solid rgba(255,255,255,.2); font-size:.75rem; color:#dffcff;
      background:rgba(100,249,255,.08); }
  </style>
</head>
<body>
  <h1 class="titre animate__animated animate__fadeInDown">Pokopia Party Planner</h1>
  <p class="soustitre animate__animated animate__fadeInUp animate__delay-1s">
    Schéma BDD + préférences (lecture PostgreSQL)
  </p>
  <span class="badge animate__animated animate__pulse animate__infinite animate__slow">
    DAG → schéma « pokopia » — même base que le reste du projet
  </span>
</body>
</html>
"""


def _favorites_matrix_html(rows: list[dict]) -> str:
    if not rows:
        return "<p style='color:#aab;'>Sélectionne au moins un Pokémon.</p>"
    max_len = max(len(r.get("favorites") or []) for r in rows)
    heads = "".join(
        f"<th><span class='th-num'>#{escape(r['num'])}</span><br/>{escape(r['nom'])}</th>"
        for r in rows
    )
    body_rows = []
    for i in range(max_len):
        tds = []
        for r in rows:
            favs = r.get("favorites") or []
            cell = favs[i] if i < len(favs) else ""
            tds.append(f"<td>{escape(cell) if cell else '—'}</td>")
        body_rows.append("<tr>" + "".join(tds) + "</tr>")
    return f"""
<div class="matrix-wrap animate__animated animate__zoomIn">
  <table class="matrix">
    <thead><tr>{heads}</tr></thead>
    <tbody>{"".join(body_rows)}</tbody>
  </table>
</div>
"""


def _matrix_shell(inner: str) -> str:
    return f"""
<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8" />
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/animate.css/4.1.1/animate.min.css" />
  <style>
    body {{ margin:0; padding:12px 8px;
      background: radial-gradient(circle at 10% 0%, rgba(255,79,216,.15), transparent 40%),
                  radial-gradient(circle at 90% 20%, rgba(100,249,255,.12), transparent 38%),
                  linear-gradient(160deg,#0a0f24,#151538);
      font-family: system-ui, sans-serif; color:#e8ecff; border-radius:16px; }}
    .matrix-wrap {{ overflow-x:auto; }}
    table.matrix {{ border-collapse:collapse; width:100%; min-width:520px; }}
    table.matrix th, table.matrix td {{
      border:1px solid rgba(255,255,255,.12); padding:10px 12px; text-align:left;
      vertical-align:top; font-size:0.92rem; }}
    table.matrix th {{
      background: linear-gradient(180deg, rgba(255,79,216,.18), rgba(100,249,255,.08));
      color:#fff; font-weight:700; }}
    table.matrix tr:nth-child(even) td {{ background: rgba(255,255,255,.03); }}
    table.matrix td {{ color:#dfe7ff; }}
    .th-num {{ font-size:0.75rem; color:#9fb4ff; font-weight:600; }}
  </style>
</head>
<body>{inner}</body>
</html>
"""


components.html(_hero_html(), height=200, scrolling=False)

st.markdown(
    """
<style>
  .block-container { padding-top: 1rem !important; }
</style>
""",
    unsafe_allow_html=True,
)

st.markdown("## Stack (rappel)")
st.markdown(
    """
- **Scraping** Serebii (fiches Pokopia) pour numéro, nom, spécialités, habitat idéal, favoris — **à but scolaire uniquement** (projet d’apprentissage ; ne pas répliquer en production sans respecter le site source et le cadre légal).
- **CSV** interne pour les thèmes cadeaux et les objets (combos « opti » par thème).
- **DAG Airflow** qui fusionne les deux sources et **alimente PostgreSQL** (schéma ``pokopia`` : tables + FK + liaisons N–N avec ordre d’affichage).

Cette page affiche le **modèle entités–associations** via **Graphviz**, puis le planificateur lit **uniquement** la base (plus de JSON local).
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

st.divider()
st.markdown("### Planificateur")

try:
    base_rows = _load_pokemon_rows()
except Exception as e:
    st.error(
        "Connexion à PostgreSQL ou schéma `pokopia` introuvable. "
        "Vérifie les variables `APP_DB_*` (ou secrets Streamlit) et exécute le DAG `pokopia_scrape_dag`."
    )
    st.caption(str(e))
    st.stop()

if not base_rows:
    st.warning(
        "La table `pokopia.pokemon` est vide. Lance le DAG **pokopia_scrape_dag** pour charger les données."
    )
    st.stop()

filtre = st.text_input(
    "Filtrer la liste (ex. **mew** → Mew, Mewtwo)",
    value="",
    placeholder="mew, pika, …",
    key="pokopia_filter",
)

q = (filtre or "").strip().lower()
if q:
    filtered = [r for r in base_rows if q in (r.get("nom") or "").lower()]
else:
    filtered = base_rows

noms = [r["nom"] for r in filtered]
selected_labels = st.multiselect(
    "Choisis jusqu’à **4** Pokémon",
    options=noms,
    default=[],
    max_selections=4,
)

selected_meta = [r for r in filtered if r.get("nom") in selected_labels]
if selected_meta:
    fav_map = _load_favorites_map(tuple(r["nom"] for r in selected_meta))
    selected = [
        {"nom": r["nom"], "num": r["num"], "favorites": fav_map.get(r["nom"], [])}
        for r in selected_meta
    ]
else:
    selected = []

if selected:
    inner = _favorites_matrix_html(selected)
    h = min(620, 140 + max(len(r.get("favorites") or []) for r in selected) * 52)
    components.html(_matrix_shell(inner), height=int(h), scrolling=True)
else:
    st.info("Sélectionne un ou plusieurs Pokémon pour afficher le tableau des préférences « stuff ».")

st.caption(
    "Source : tables `pokopia.*` remplies par le DAG (favoris ordonnés via `pokemon_favorite.ordre`)."
)

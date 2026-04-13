"""
Pokopia : planificateur (lecture PostgreSQL schéma pokopia).
"""

from __future__ import annotations

import os
from collections import defaultdict
from html import escape

import psycopg2
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(
    page_title="RFM Documentation — Pokopia Planner",
    layout="wide",
)


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


# Ordre d’affichage aligné sur pokopia_scrape_lib (chair → relaxation, etc.)
_CATEGORY_ORDER: tuple[str, ...] = ("relaxation", "decoration", "tot")
_CATEGORY_LABELS: dict[str, str] = {
    "relaxation": "Relaxation (siège)",
    "decoration": "Décoration",
    "tot": "Petits objets & jouets",
}


@st.cache_data(ttl=60, show_spinner=False)
def _load_items_by_category() -> dict[str, list[dict[str, str]]]:
    """Charge les items avec jointure type → category, groupés par catégorie."""
    conn = _db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.valeur AS category_valeur,
                       i.gift_theme_valeur,
                       i.type_valeur,
                       i.name
                FROM pokopia.item i
                JOIN pokopia.type t ON t.valeur = i.type_valeur
                JOIN pokopia.category c ON c.valeur = t.category_valeur
                ORDER BY c.valeur, i.gift_theme_valeur, i.type_valeur, i.name
                """
            )
            raw = cur.fetchall()
    finally:
        conn.close()
    by_cat: dict[str, list[dict[str, str]]] = defaultdict(list)
    for cat, gift_theme, type_v, name in raw:
        by_cat[cat].append(
            {
                "gift_theme": gift_theme,
                "type": type_v,
                "name": name,
            }
        )
    return dict(by_cat)


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
    Préférences cadeaux — lecture base
  </p>
  <span class="badge animate__animated animate__pulse animate__infinite animate__slow">
    La doc + le schéma ER sont sur l’onglet « Pokopia documentation »
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


def _items_by_category_html(by_category: dict[str, list[dict[str, str]]]) -> str:
    if not by_category:
        return "<p class='muted'>Aucun item dans <code>pokopia.item</code> (vérifie le DAG et items.csv).</p>"
    blocks: list[str] = []
    seen: set[str] = set()
    for cat in _CATEGORY_ORDER:
        items = by_category.get(cat) or []
        seen.add(cat)
        if not items:
            continue
        label = _CATEGORY_LABELS.get(cat, escape(cat))
        lis = "".join(
            f"<li><span class='iname'>{escape(it['name'])}</span>"
            f"<span class='imeta'> — thème « {escape(it['gift_theme'])} » · type {escape(it['type'])}</span></li>"
            for it in items
        )
        blocks.append(
            f"<section class='cat-block animate__animated animate__fadeInUp'>"
            f"<h3 class='cat-title'>{label}</h3><ul class='item-list'>{lis}</ul></section>"
        )
    for cat in sorted(set(by_category) - seen):
        items = by_category[cat]
        label = _CATEGORY_LABELS.get(cat, escape(cat))
        lis = "".join(
            f"<li><span class='iname'>{escape(it['name'])}</span>"
            f"<span class='imeta'> — thème « {escape(it['gift_theme'])} » · type {escape(it['type'])}</span></li>"
            for it in items
        )
        blocks.append(
            f"<section class='cat-block'><h3 class='cat-title'>{label}</h3>"
            f"<ul class='item-list'>{lis}</ul></section>"
        )
    return "".join(blocks)


def _items_catalog_shell(inner: str) -> str:
    return f"""
<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8" />
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/animate.css/4.1.1/animate.min.css" />
  <style>
    body {{ margin:0; padding:14px 12px;
      background: radial-gradient(circle at 15% 0%, rgba(255,214,107,.12), transparent 42%),
                  radial-gradient(circle at 85% 10%, rgba(100,249,255,.1), transparent 40%),
                  linear-gradient(165deg,#0c1028,#121832);
      font-family: system-ui, sans-serif; color:#e8ecff; border-radius:16px; }}
    .muted {{ color:#8a9bc4; font-size:0.9rem; }}
    .cat-block {{ margin-bottom:22px; }}
    .cat-block:last-child {{ margin-bottom:4px; }}
    .cat-title {{
      font-size:1.05rem; font-weight:800; margin:0 0 10px;
      padding-bottom:6px; border-bottom:1px solid rgba(255,255,255,.15);
      background:linear-gradient(90deg,#ffd86b,#64f9ff);
      -webkit-background-clip:text; -webkit-text-fill-color:transparent; }}
    ul.item-list {{ list-style:none; padding:0; margin:0; }}
    ul.item-list li {{
      padding:8px 10px; margin:4px 0; border-radius:10px;
      background:rgba(255,255,255,.04);
      border:1px solid rgba(255,255,255,.08); font-size:0.9rem; }}
    .iname {{ font-weight:600; color:#fff; }}
    .imeta {{ color:#9fb4ff; font-size:0.82rem; }}
  </style>
</head>
<body>{inner}</body>
</html>
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

st.markdown("### Planificateur")

try:
    base_rows = _load_pokemon_rows()
except Exception as e:
    err = str(e).lower()
    st.error(
        "Impossible de lire `pokopia.pokemon`. Vérifie `APP_DB_*` / secrets Streamlit et que le DAG "
        "**pokopia_scrape_dag** a bien chargé les données."
    )
    st.caption(str(e))
    if "permission denied" in err:
        st.info(
            "Le rôle Postgres utilisé par Streamlit (**APP_DB_***) doit avoir **USAGE** sur le schéma `pokopia` "
            "et **SELECT** sur les tables : voir l’onglet **Pokopia documentation** pour le SQL **GRANT** à exécuter à la main "
            "(propriétaire des tables = le rôle de la connexion **DATA-DB** du DAG, distinct du compte Streamlit)."
        )
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

st.markdown("### Catalogue cadeaux par catégorie")
st.caption(
    "Items issus de `pokopia.item` + `type` + `category` (relaxation = siège, décoration = moyen, petits objets = TOT)."
)

try:
    items_by_cat = _load_items_by_category()
except Exception as e:
    st.warning(f"Lecture des items impossible : {e}")
    items_by_cat = {}

n_items = sum(len(v) for v in items_by_cat.values())
inner_items = _items_by_category_html(items_by_cat)
# Hauteur dynamique : en-têtes + lignes (~36px par item + marges)
items_h = min(900, 120 + n_items * 34 + len(items_by_cat) * 48)
components.html(_items_catalog_shell(inner_items), height=int(items_h), scrolling=True)

st.caption(
    "Source : tables `pokopia.*` (favoris via `pokemon_favorite.ordre`, items via jointure item → type → category)."
)

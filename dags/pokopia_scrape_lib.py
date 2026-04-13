#!/usr/bin/env python3
"""
Logique scraping Pokopia (Serebii + items) pour Airflow et scripts CLI.

**Scraping** : mis en œuvre **à but exclusivement scolaire** (exercice pédagogique).
Hors projet de formation, respecter les conditions d'usage du site tiers
(robots.txt, charge raisonnable, droits d'auteur) et ne pas reproduire ce mode
opératoire en production sans cadre légal adapté.

Le fichier items.csv doit vivre sous ``dags/data/pokemon_pokopia/`` pour être
visible dans les conteneurs (seul ``./dags`` est monté sur ``/opt/airflow/dags``).
Une copie peut exister dans ``pokemon/items.csv`` pour exécution hors Docker.

Modèle relationnel (3FN) — espèces (Serebii) :
  - pokemon (PK nom, num, FK ideal_habitat_valeur -> ideal_habitat)
  - specialty (PK valeur)
  - ideal_habitat (PK valeur)
  - favorite (PK valeur) — libellés « Favorites » sur la fiche Pokémon
  - pokemon_specialty (PK pokemon_nom + ordre ; FK specialty) — N-N
  - pokemon_favorite (PK pokemon_nom + ordre ; FK favorite) — N-N

Modèle objets (items.csv) :
  - gift_theme (PK valeur) — 1re colonne du CSV (axe de préférence / thème)
  - category (PK valeur) — relaxation | decoration | tot
  - type (PK valeur, FK category_valeur -> category) — chair | medium | small
  - item (PK gift_theme_valeur, type_valeur, name) — FK gift_theme, FK type

Liaison favorite (scraping) <-> gift_theme (CSV) : vocabulaires différents
(Serebii « Soft stuff », CSV « Soft Stuff », etc.). Une table de correspondance
pourra être ajoutée plus tard (favorite_valeur <-> gift_theme_valeur).

Dépendances : pip install requests beautifulsoup4

Usage (module) :
  ``python pokopia_scrape_lib.py`` depuis le répertoire ``dags/``, ou via le DAG
  ``pokopia_scrape_dag``.
"""

from __future__ import annotations

import csv
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

BASE = "https://www.serebii.net"
LIST_URL = f"{BASE}/pokemonpokopia/availablepokemon.shtml"
POKEDEX_PREFIX = "/pokemonpokopia/pokedex/"
LIMIT = 10

SLUG_RE = re.compile(r"^/pokemonpokopia/pokedex/([^/]+)\.shtml$")

# Colonnes items.csv -> type.valeur (la catégorie décorative vient de la table type)
ITEM_CSV_COLUMNS = (
    ("chair_item", "chair"),
    ("medium_item", "medium"),
    ("small_item", "small"),
)

# Référentiel type -> category (une seule ligne par type)
ITEM_TYPE_ROWS: list[dict[str, str]] = [
    {"valeur": "chair", "category_valeur": "relaxation"},
    {"valeur": "medium", "category_valeur": "decoration"},
    {"valeur": "small", "category_valeur": "tot"},
]

ITEM_CATEGORY_ROWS: list[dict[str, str]] = [
    {"valeur": "relaxation"},
    {"valeur": "decoration"},
    {"valeur": "tot"},
]

def resolve_items_csv() -> Path:
    """Priorité : copie sous ``dags/data/`` (Docker), sinon ``pokemon/items.csv``."""
    dags_dir = Path(__file__).resolve().parent
    project_root = dags_dir.parent
    candidates = [
        dags_dir / "data" / "pokemon_pokopia" / "items.csv",
        project_root / "pokemon" / "items.csv",
    ]
    for p in candidates:
        if p.is_file():
            return p
    raise FileNotFoundError(
        "items.csv introuvable. Déposez le fichier dans "
        "dags/data/pokemon_pokopia/items.csv (recommandé pour Airflow)."
    )


@dataclass
class PokemonRow:
    """Données brutes par espèce après parsing de la fiche."""

    nom: str
    num: str
    specialties: list[str] = field(default_factory=list)
    ideal_habitat: str = ""
    favorites: list[str] = field(default_factory=list)


def _require_deps() -> None:
    try:
        import requests  # noqa: F401
        from bs4 import BeautifulSoup  # noqa: F401
    except ImportError as e:
        raise RuntimeError(
            "Dépendances manquantes : pip install requests beautifulsoup4. "
            "Docker : définir _PIP_ADDITIONAL_REQUIREMENTS (voir docker-compose.dev.yaml)."
        ) from e


def format_num(n: int) -> str:
    if n < 1000:
        return f"{n:03d}"
    return str(n)


def fetch_html(url: str) -> str:
    import requests

    r = requests.get(
        url,
        timeout=30,
        headers={
            "User-Agent": "brief-rfm-pokopia-scraper/0.1 (contact: local dev; +https://www.serebii.net/robots.txt)"
        },
    )
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def first_pokedex_slugs_from_list(html: str, limit: int) -> list[str]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    h2 = soup.find("h2", string=re.compile(r"List of Available", re.I))
    if not h2:
        raise RuntimeError("Bloc liste introuvable (h2 « List of Available »).")
    table = h2.find_next("table")
    if not table:
        raise RuntimeError("Tableau liste introuvable après le h2.")

    slugs: list[str] = []
    seen: set[str] = set()
    for tr in table.find_all("tr"):
        tds = tr.find_all("td", recursive=False)
        if len(tds) < 3:
            continue
        name_cell = tds[2]
        a = name_cell.find("a", href=True)
        if not a:
            continue
        m = SLUG_RE.match(a["href"])
        if not m:
            continue
        slug = m.group(1)
        if slug in seen:
            continue
        seen.add(slug)
        slugs.append(slug)
        if len(slugs) >= limit:
            break
    return slugs


def parse_detail(html: str) -> PokemonRow:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    if not h1:
        raise RuntimeError("h1 introuvable sur la fiche.")
    m = re.match(r"#\s*(\d+)\s+(.+)", h1.get_text(strip=True))
    if not m:
        raise RuntimeError(f"h1 inattendu : {h1.get_text(strip=True)!r}")
    num_raw, nom = m.group(1), m.group(2).strip()
    num = format_num(int(num_raw))

    specialty_labels: list[str] = []
    ideal_habitat = ""
    favorites: list[str] = []

    for td in soup.find_all("td", class_=lambda c: c and "foo" in c.split()):
        if td.get_text(strip=True) == "Specialty":
            header_tr = td.find_parent("tr")
            break
    else:
        header_tr = None

    if header_tr:
        data_tr = header_tr.find_next_sibling("tr")
        if data_tr:
            cells = data_tr.find_all("td", class_=lambda c: c and "cen" in c.split())
            if len(cells) >= 3:
                spec_cell, hab_cell, fav_cell = cells[0], cells[1], cells[2]
                for u in spec_cell.select("table u"):
                    t = u.get_text(strip=True)
                    if t and t not in specialty_labels:
                        specialty_labels.append(t)
                hu = hab_cell.find("u")
                if hu:
                    ideal_habitat = hu.get_text(strip=True)
                for u in fav_cell.find_all("u"):
                    t = u.get_text(strip=True)
                    if t:
                        favorites.append(t)

    return PokemonRow(
        nom=nom,
        num=num,
        specialties=specialty_labels,
        ideal_habitat=ideal_habitat,
        favorites=favorites,
    )


def build_normalized_tables(rows: Iterable[PokemonRow]) -> dict[str, list[dict]]:
    rows = list(rows)
    specialty_vals = sorted(
        {s for r in rows for s in r.specialties},
        key=str.lower,
    )
    habitat_vals = sorted(
        {r.ideal_habitat for r in rows if r.ideal_habitat},
        key=str.lower,
    )
    favorite_vals = sorted(
        {f for r in rows for f in r.favorites},
        key=str.lower,
    )

    table_pokemon = []
    for r in rows:
        table_pokemon.append(
            {
                "nom": r.nom,
                "num": r.num,
                "ideal_habitat_valeur": r.ideal_habitat or None,
            }
        )

    table_specialty = [{"valeur": v} for v in specialty_vals]
    table_ideal_habitat = [{"valeur": v} for v in habitat_vals]
    table_favorite = [{"valeur": v} for v in favorite_vals]

    pokemon_specialty = []
    for r in rows:
        for i, s in enumerate(r.specialties, start=1):
            pokemon_specialty.append(
                {"pokemon_nom": r.nom, "specialty_valeur": s, "ordre": i}
            )

    pokemon_favorite = []
    for r in rows:
        for i, f in enumerate(r.favorites, start=1):
            pokemon_favorite.append(
                {"pokemon_nom": r.nom, "favorite_valeur": f, "ordre": i}
            )

    return {
        "pokemon": table_pokemon,
        "specialty": table_specialty,
        "ideal_habitat": table_ideal_habitat,
        "favorite": table_favorite,
        "pokemon_specialty": pokemon_specialty,
        "pokemon_favorite": pokemon_favorite,
    }


def build_item_tables_from_csv(csv_path: Path) -> dict[str, list[dict]]:
    """Construit gift_theme, category, type et item à partir de items.csv."""
    if not csv_path.is_file():
        raise FileNotFoundError(f"Fichier introuvable : {csv_path}")

    gift_themes: list[dict] = []
    items: list[dict] = []
    seen_themes: set[str] = set()

    with csv_path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        expected = {"favorite_category", "chair_item", "medium_item", "small_item"}
        if reader.fieldnames is None or not expected.issubset(set(reader.fieldnames)):
            raise ValueError(
                f"En-tête items.csv inattendu : {reader.fieldnames!r} (attendu {expected})"
            )
        for row in reader:
            theme = (row.get("favorite_category") or "").strip()
            if not theme:
                continue
            if theme not in seen_themes:
                seen_themes.add(theme)
                gift_themes.append({"valeur": theme})

            for col_name, type_valeur in ITEM_CSV_COLUMNS:
                name = (row.get(col_name) or "").strip()
                if not name:
                    continue
                items.append(
                    {
                        "gift_theme_valeur": theme,
                        "name": name,
                        "type_valeur": type_valeur,
                    }
                )

    return {
        "gift_theme": gift_themes,
        "category": list(ITEM_CATEGORY_ROWS),
        "type": list(ITEM_TYPE_ROWS),
        "item": items,
    }


def merge_all_tables(
    pokemon_tables: dict[str, list[dict]],
    item_tables: dict[str, list[dict]],
) -> dict[str, list[dict]]:
    out = dict(pokemon_tables)
    out.update(item_tables)
    return out


def print_er_overview() -> None:
    """Résumé textuel du graphe entités / associations."""
    print("=== Vue ER (texte) ===")
    print(
        """
  [ideal_habitat]<- (FK) --[pokemon]-- (N-N) --[pokemon_specialty]-->[specialty]
                    |
                    +-- (N-N) --[pokemon_favorite]-->[favorite]
                    .
  [category]<- (FK) --[type]<- (FK) --[item]-- (FK) -->[gift_theme]
                        (name)

  Pont futur (hors tables pour l'instant) : relier [favorite] et [gift_theme]
  quand la table de correspondance des libellés existera.
""".strip()
    )


def print_schema(tables: dict[str, list[dict]]) -> None:
    print("=== Table pokemon (PK = nom | num | FK ideal_habitat_valeur -> ideal_habitat) ===")
    for row in tables["pokemon"]:
        print(
            f"  nom={row['nom']!r}  num={row['num']!r}  "
            f"ideal_habitat_valeur={row['ideal_habitat_valeur']!r}"
        )

    print("\n=== Table specialty (PK = valeur) ===")
    for row in tables["specialty"]:
        print(f"  valeur={row['valeur']!r}")

    print("\n=== Table ideal_habitat (PK = valeur) ===")
    for row in tables["ideal_habitat"]:
        print(f"  valeur={row['valeur']!r}")

    print("\n=== Table favorite (PK = valeur) ===")
    for row in tables["favorite"]:
        print(f"  valeur={row['valeur']!r}")

    print(
        "\n=== Table pokemon_specialty (PK = (pokemon_nom, specialty_valeur) | N-N) ==="
    )
    for row in tables["pokemon_specialty"]:
        print(
            f"  pokemon_nom={row['pokemon_nom']!r}  specialty_valeur={row['specialty_valeur']!r}  "
            f"ordre={row['ordre']!r}"
        )

    print(
        "\n=== Table pokemon_favorite (PK = (pokemon_nom, ordre) | N-N) ==="
    )
    for row in tables["pokemon_favorite"]:
        print(
            f"  pokemon_nom={row['pokemon_nom']!r}  favorite_valeur={row['favorite_valeur']!r}  "
            f"ordre={row['ordre']!r}"
        )

    if "gift_theme" in tables:
        print("\n=== Table gift_theme (PK = valeur | thèmes cadeaux items.csv) ===")
        for row in tables["gift_theme"]:
            print(f"  valeur={row['valeur']!r}")

    if "category" in tables:
        print("\n=== Table category (PK = valeur) ===")
        for row in tables["category"]:
            print(f"  valeur={row['valeur']!r}")

    if "type" in tables:
        print(
            "\n=== Table type (PK = valeur | FK category_valeur -> category) ==="
        )
        for row in tables["type"]:
            print(
                f"  valeur={row['valeur']!r}  category_valeur={row['category_valeur']!r}"
            )

    if "item" in tables:
        print(
            "\n=== Table item (PK = (gift_theme_valeur, type_valeur, name) | "
            "FK gift_theme_valeur -> gift_theme, FK type_valeur -> type) ==="
        )
        for row in tables["item"]:
            print(
                f"  theme={row['gift_theme_valeur']!r}  type_valeur={row['type_valeur']!r}  "
                f"name={row['name']!r}"
            )


def run_pipeline(*, limit: int = 10, items_csv: Path | None = None) -> dict[str, list[dict]]:
    """Scrape Serebii, fusionne les tables objets depuis items.csv, retourne toutes les tables."""
    _require_deps()
    list_html = fetch_html(LIST_URL)
    slugs = first_pokedex_slugs_from_list(list_html, limit)
    if len(slugs) < limit:
        print(f"Avertissement : seulement {len(slugs)} entrées trouvées.", file=sys.stderr)

    rows: list[PokemonRow] = []
    for slug in slugs:
        url = f"{BASE}{POKEDEX_PREFIX}{slug}.shtml"
        detail_html = fetch_html(url)
        rows.append(parse_detail(detail_html))

    pokemon_tables = build_normalized_tables(rows)
    path = items_csv if items_csv is not None else resolve_items_csv()
    try:
        item_tables = build_item_tables_from_csv(path)
    except (FileNotFoundError, ValueError) as e:
        print(f"Avertissement items.csv : {e}", file=sys.stderr)
        item_tables = {}

    return merge_all_tables(pokemon_tables, item_tables)


def write_tables_to_staging(tables: dict[str, list[dict]], out_dir: Path) -> list[str]:
    """Écrit une feuille CSV par table (noms de fichiers = clés du dictionnaire)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    for name, rows in sorted(tables.items()):
        if not rows:
            continue
        path = out_dir / f"{name}.csv"
        keys = list(rows[0].keys())
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        written.append(str(path))
    return written


def main() -> None:
    tables = run_pipeline(limit=LIMIT, items_csv=None)
    print_er_overview()
    print()
    print_schema(tables)


if __name__ == "__main__":
    main()

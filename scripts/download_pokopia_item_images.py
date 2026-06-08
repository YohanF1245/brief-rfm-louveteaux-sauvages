#!/usr/bin/env python3
"""
Télécharge les images des objets présents dans items.csv.

Règles de construction de l'URL image :
- base_url + "/" + nom_item_normalise + ".png"
- normalisation : lower + suppression des espaces
- "No item available" est ignoré

Usage:
python scripts/download_pokopia_item_images.py --base-url "https://example.com/items"
"""

from __future__ import annotations

import argparse
import csv
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

DEFAULT_CSV_PATH = Path("dags/data/pokemon_pokopia/items.csv")
DEFAULT_OUTPUT_DIR = Path("streamlit/data/pokopia_item_images")
DEFAULT_ERROR_LOG = Path("logs/pokopia_item_images_errors.log")
ITEM_COLUMNS = ("chair_item", "medium_item", "small_item")
SKIP_VALUE = "no item available"


def normalize_item_name(item_name: str) -> str:
    """Convertit en lower et retire les espaces."""
    return item_name.strip().lower().replace(" ", "")


def iter_items_from_csv(csv_path: Path) -> set[str]:
    """Extrait les noms d'objets uniques depuis les colonnes items."""
    items: set[str] = set()
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            for col in ITEM_COLUMNS:
                raw = (row.get(col) or "").strip()
                if not raw:
                    continue
                if raw.lower() == SKIP_VALUE:
                    continue
                items.add(raw)
    return items


def build_image_url(base_url: str, item_name: str) -> str:
    slug = normalize_item_name(item_name)
    return f"{base_url.rstrip('/')}/{slug}.png"


def download_image(url: str, destination: Path) -> None:
    with urllib.request.urlopen(url, timeout=20) as response:
        status = getattr(response, "status", 200)
        if status >= 400:
            raise urllib.error.HTTPError(url, status, "HTTP error", response.headers, None)
        content = response.read()
    destination.write_bytes(content)


def append_error(log_path: Path, message: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(message + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Télécharge les images d'items depuis items.csv."
    )
    parser.add_argument("--base-url", required=True, help="URL de base des images.")
    parser.add_argument(
        "--csv-path",
        default=str(DEFAULT_CSV_PATH),
        help=f"Chemin du CSV (défaut: {DEFAULT_CSV_PATH}).",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help=f"Dossier de sortie (défaut: {DEFAULT_OUTPUT_DIR}).",
    )
    parser.add_argument(
        "--error-log",
        default=str(DEFAULT_ERROR_LOG),
        help=f"Fichier de log d'erreurs (défaut: {DEFAULT_ERROR_LOG}).",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv_path)
    output_dir = Path(args.output_dir)
    error_log = Path(args.error_log)

    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV introuvable: {csv_path}")

    items = sorted(iter_items_from_csv(csv_path), key=str.lower)
    output_dir.mkdir(parents=True, exist_ok=True)

    success_count = 0
    error_count = 0

    for item_name in items:
        url = build_image_url(args.base_url, item_name)
        file_name = Path(urllib.parse.urlparse(url).path).name
        destination = output_dir / file_name
        try:
            download_image(url, destination)
            success_count += 1
        except Exception as exc:  # noqa: BLE001
            error_count += 1
            append_error(error_log, f"{item_name}\t{url}\t{exc}")

    print(f"Images téléchargées: {success_count}")
    print(f"Erreurs: {error_count}")
    if error_count:
        print(f"Voir le log: {error_log}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

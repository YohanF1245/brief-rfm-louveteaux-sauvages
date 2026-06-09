"""Helpers Streamlit WCL (résultats ClickHouse vides / sans colonnes)."""

from __future__ import annotations

import pandas as pd


def column_values(df: pd.DataFrame, column: str) -> list[str]:
    """Extrait une colonne ; retourne [] si DataFrame vide ou colonne absente."""
    if df.empty or column not in df.columns:
        return []
    return df[column].dropna().astype(str).tolist()

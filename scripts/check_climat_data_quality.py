#!/usr/bin/env python3
"""
Contrôle qualité des données climat Météo-France (RR-T-Vent), en local.

Compare les fichiers gzip bruts, une transformation type DAG `meteo_nord`,
et optionnellement la table Postgres `public.climat_data`.

Usage:
    python scripts/check_climat_data_quality.py
    python scripts/check_climat_data_quality.py --postgres
    python scripts/check_climat_data_quality.py --json reports/climat_qc.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_FILES = (
    ROOT / "dags/data/raw_climat_data/50-24.gz",
    ROOT / "dags/data/raw_climat_data/25-26.gz",
)

COLS_RAW = (
    "NUM_POSTE",
    "NOM_USUEL",
    "AAAAMMJJ",
    "RR",
    "TN",
    "TX",
    "QTN",
    "QTX",
)

SENTINELLES_TEMP = (-999.0, -9999.0)
PLAGE_TN = (-50.0, 50.0)
PLAGE_TX = (-50.0, 55.0)

LIBELLE_QUALITE = {
    0: "protégée (validée définitivement)",
    1: "validée",
    2: "douteuse",
    9: "filtrée",
}


@dataclass
class RapportFichier:
    chemin: str
    lignes: int = 0
    stations: int = 0
    pct_tn_null: float | None = None
    pct_tx_null: float | None = None
    tn_zero: int = 0
    tx_zero: int = 0
    tn_sentinelles: int = 0
    tx_sentinelles: int = 0
    tn_min: float | None = None
    tn_max: float | None = None
    tx_min: float | None = None
    tx_max: float | None = None
    stations_avec_tx: int = 0
    stations_sans_tx: int = 0
    pct_stations_thermo: float | None = None
    tn_zero_ete: int = 0
    tx_lt_tn: int = 0
    doublons_station_date: int = 0
    alertes: list[str] = field(default_factory=list)


@dataclass
class RapportGlobal:
    fichiers: list[RapportFichier] = field(default_factory=list)
    fusion_lignes: int | None = None
    fusion_pct_tn_null: float | None = None
    fusion_chevauchement_cles: int | None = None
    postgres: dict | None = None
    verdict: str = ""


def _lire_brut(chemin: Path) -> pd.DataFrame:
    if not chemin.is_file():
        raise FileNotFoundError(chemin)
    return pd.read_csv(
        chemin,
        sep=";",
        compression="gzip",
        usecols=lambda c: c in COLS_RAW,
        low_memory=False,
    )


def _to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _analyser_fichier(chemin: Path) -> tuple[RapportFichier, pd.DataFrame, set[tuple[int, int]]]:
    df = _lire_brut(chemin)
    tn = _to_num(df["TN"])
    tx = _to_num(df["TX"])

    rapport = RapportFichier(chemin=str(chemin))
    rapport.lignes = len(df)
    rapport.stations = int(df["NUM_POSTE"].nunique())
    rapport.pct_tn_null = round(100 * tn.isna().mean(), 1) if len(tn) else None
    rapport.pct_tx_null = round(100 * tx.isna().mean(), 1) if len(tx) else None
    rapport.tn_zero = int((tn == 0).sum())
    rapport.tx_zero = int((tx == 0).sum())
    rapport.tn_sentinelles = int(tn.isin(SENTINELLES_TEMP).sum())
    rapport.tx_sentinelles = int(tx.isin(SENTINELLES_TEMP).sum())

    if tn.notna().any():
        rapport.tn_min = float(tn.min())
        rapport.tn_max = float(tn.max())
    if tx.notna().any():
        rapport.tx_min = float(tx.min())
        rapport.tx_max = float(tx.max())

    par_station = tx.notna().groupby(df["NUM_POSTE"]).any()
    rapport.stations_avec_tx = int(par_station.sum())
    rapport.stations_sans_tx = int((~par_station).sum())
    if rapport.stations:
        rapport.pct_stations_thermo = round(
            100 * rapport.stations_avec_tx / rapport.stations, 1
        )

    dates = pd.to_datetime(df["AAAAMMJJ"].astype(str), format="%Y%m%d", errors="coerce")
    ete = dates.dt.month.isin([6, 7, 8])
    rapport.tn_zero_ete = int(((tn == 0) & ete).sum())

    both = tn.notna() & tx.notna()
    rapport.tx_lt_tn = int((tx < tn)[both].sum())

    cles = list(zip(df["NUM_POSTE"].astype(int), df["AAAAMMJJ"].astype(int)))
    rapport.doublons_station_date = int(len(cles) - len(set(cles)))

    hors_plage_tn = tn.notna() & ((tn < PLAGE_TN[0]) | (tn > PLAGE_TN[1]))
    hors_plage_tx = tx.notna() & ((tx < PLAGE_TX[0]) | (tx > PLAGE_TX[1]))
    if hors_plage_tn.any():
        rapport.alertes.append(
            f"{int(hors_plage_tn.sum())} TN hors plage {PLAGE_TN} (vérifier échelle 1/10 ?)"
        )
    if rapport.tn_sentinelles or rapport.tx_sentinelles:
        rapport.alertes.append(
            f"Sentinelles -999 (TN={rapport.tn_sentinelles}, TX={rapport.tx_sentinelles})"
        )
    if rapport.tn_zero_ete > 50:
        rapport.alertes.append(
            f"{rapport.tn_zero_ete} TN=0 en été (juin–août) — à croiser avec QTN"
        )
    if rapport.tx_lt_tn > 0:
        rapport.alertes.append(f"{rapport.tx_lt_tn} lignes avec TX < TN")
    if rapport.pct_tn_null and rapport.pct_tn_null > 60:
        rapport.alertes.append(
            f"~{rapport.pct_tn_null}% TN vides : attendu si postes pluvio-only nombreux"
        )

    return rapport, df, set(cles)


def _simuler_transform(df: pd.DataFrame) -> pd.DataFrame:
    out = df.rename(
        columns={
            "NUM_POSTE": "station_id",
            "NOM_USUEL": "station_nom",
            "AAAAMMJJ": "date_mesure",
            "RR": "quantite_precipitations",
            "TN": "temp_min",
            "TX": "temp_max",
            "QTN": "qualite_temp_min",
            "QTX": "qualite_temp_max",
        }
    ).copy()
    out["date_mesure"] = pd.to_datetime(out["date_mesure"].astype(str), format="%Y%m%d")
    for col in ("temp_min", "temp_max", "quantite_precipitations"):
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("Float64")
    return out


def _verifier_transform(df_brut: pd.DataFrame) -> list[str]:
    avant_tn = _to_num(df_brut["TN"]).isna().sum()
    apres = _simuler_transform(df_brut)
    apres_tn = apres["temp_min"].isna().sum()
    if apres_tn > avant_tn:
        return [
            f"Perte TN à la transformation : {avant_tn} → {apres_tn} NULL (+{apres_tn - avant_tn})"
        ]
    return ["Transformation : aucune perte TN/TX vs brut (astype OK)."]


def _analyser_postgres() -> dict:
    try:
        import psycopg2
    except ImportError as exc:
        raise RuntimeError("psycopg2 requis pour --postgres (pip install psycopg2-binary)") from exc

    conn = psycopg2.connect(
        host=os.getenv("APP_DB_HOST", "localhost"),
        port=int(os.getenv("APP_DB_PORT", "5432")),
        user=os.getenv("APP_DB_USER", os.getenv("POSTGRES_USER", "postgres")),
        password=os.getenv("APP_DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "")),
        dbname=os.getenv("APP_DB_NAME", os.getenv("POSTGRES_DB", "rfm")),
    )
    try:
        df = pd.read_sql_query(
            """
            SELECT station_id, date_mesure, temp_min, temp_max, qualite_temp_min
            FROM public.climat_data
            """,
            conn,
        )
    finally:
        conn.close()

    tn = df["temp_min"]
    tx = df["temp_max"]
    par = df.groupby("station_id").agg(jours_tx=("temp_max", lambda s: s.notna().sum()))
    return {
        "lignes": len(df),
        "stations": int(df["station_id"].nunique()),
        "pct_temp_min_null": round(100 * tn.isna().mean(), 1),
        "pct_temp_max_null": round(100 * tx.isna().mean(), 1),
        "temp_min_zero": int((tn == 0).sum()),
        "temp_max_zero": int((tx == 0).sum()),
        "temp_min_min": float(tn.min()) if tn.notna().any() else None,
        "temp_min_max": float(tn.max()) if tn.notna().any() else None,
        "temp_max_max": float(tx.max()) if tx.notna().any() else None,
        "stations_sans_tx": int((par["jours_tx"] == 0).sum()),
        "stations_avec_tx": int((par["jours_tx"] > 0).sum()),
        "temp_min_zero_ete": int(
            ((tn == 0) & df["date_mesure"].dt.month.isin([6, 7, 8])).sum()
        ),
        "note_qualite": "qualite_temp_min=0 = donnée protégée MF, pas TN à 0 °C",
    }


def _afficher_rapport_fichier(r: RapportFichier) -> None:
    print(f"\n{'=' * 60}")
    print(f"Fichier : {r.chemin}")
    print(f"  Lignes      : {r.lignes:,}")
    print(f"  Stations    : {r.stations}")
    print(f"  TN null     : {r.pct_tn_null}%  |  TX null : {r.pct_tx_null}%")
    print(f"  TN = 0      : {r.tn_zero:,}  |  TX = 0 : {r.tx_zero:,}")
    if r.tn_min is not None:
        print(f"  Plage TN    : {r.tn_min} … {r.tn_max} °C")
    if r.tx_min is not None:
        print(f"  Plage TX    : {r.tx_min} … {r.tx_max} °C")
    print(
        f"  Stations thermo : {r.stations_avec_tx} ({r.pct_stations_thermo}%)"
        f"  |  sans TX : {r.stations_sans_tx}"
    )
    print(f"  TN=0 en été   : {r.tn_zero_ete}  |  TX<TN : {r.tx_lt_tn}")
    print(f"  Doublons (poste, date) : {r.doublons_station_date}")
    for a in r.alertes:
        print(f"  ! {a}")


def _afficher_top_stations(df: pd.DataFrame, n: int) -> None:
    tx = _to_num(df["TX"])
    stats = (
        df.assign(_tx=tx)
        .groupby(["NUM_POSTE", "NOM_USUEL"], observed=True)
        .agg(jours=("AAAAMMJJ", "count"), jours_tx=("_tx", lambda s: s.notna().sum()))
        .reset_index()
    )
    stats["pct_tx"] = (100 * stats["jours_tx"] / stats["jours"]).round(1)
    print(f"\n--- Top {n} stations (volume) ---")
    for _, row in stats.nlargest(n, "jours").iterrows():
        nom = str(row["NOM_USUEL"])[:40]
        print(f"  {nom:40}  jours={row['jours']:6}  TX={row['pct_tx']:5}%")

    sans = stats[stats["jours_tx"] == 0].nlargest(5, "jours")
    if len(sans):
        print("\n--- Postes pluvio-only (0 % TX, les plus actifs) ---")
        for _, row in sans.iterrows():
            nom = str(row["NOM_USUEL"])[:40]
            print(f"  {nom:40}  jours={row['jours']:6}")


def _echantillon_tn_zero(df: pd.DataFrame) -> None:
    tn = _to_num(df["TN"])
    mask = tn == 0
    if not mask.any():
        return
    print("\n--- Échantillon TN=0 (QTN) ---")
    for _, row in df.loc[mask, ["NOM_USUEL", "AAAAMMJJ", "TN", "QTN"]].head(5).iterrows():
        q = row.get("QTN")
        lib = LIBELLE_QUALITE.get(int(q), "?") if pd.notna(q) else "?"
        nom = str(row["NOM_USUEL"])[:30]
        print(f"  {nom:30} {row['AAAAMMJJ']}  TN={row['TN']}  QTN={q} ({lib})")


def main() -> int:
    parser = argparse.ArgumentParser(description="QC données climat RR-T-Vent (local).")
    parser.add_argument("--raw", nargs="+", type=Path, default=list(DEFAULT_RAW_FILES))
    parser.add_argument("--postgres", action="store_true")
    parser.add_argument("--json", type=Path, default=None)
    parser.add_argument("--top-stations", type=int, default=10)
    args = parser.parse_args()

    rapport = RapportGlobal()
    cles_list: list[set[tuple[int, int]]] = []
    dfs: list[pd.DataFrame] = []

    print("Contrôle qualité climat Nord (RR-T-Vent)")
    print("QTN/QTX :", ", ".join(f"{k}={v}" for k, v in LIBELLE_QUALITE.items()))

    for chemin in args.raw:
        try:
            r, df, cles = _analyser_fichier(chemin)
        except FileNotFoundError:
            print(f"\n[ABSENT] {chemin}", file=sys.stderr)
            continue
        rapport.fichiers.append(r)
        cles_list.append(cles)
        dfs.append(df)
        _afficher_rapport_fichier(r)
        for m in _verifier_transform(df):
            print(f"  >> {m}")
        _echantillon_tn_zero(df)
        _afficher_top_stations(df, args.top_stations)

    if len(dfs) >= 2:
        overlap = cles_list[0]
        for s in cles_list[1:]:
            overlap &= s
        fusion = pd.concat(dfs, ignore_index=True)
        tn = _to_num(fusion["TN"])
        rapport.fusion_lignes = len(fusion)
        rapport.fusion_pct_tn_null = round(100 * tn.isna().mean(), 1)
        rapport.fusion_chevauchement_cles = len(overlap)
        print(f"\n{'=' * 60}\nFusion (extract DAG)")
        print(f"  Lignes : {rapport.fusion_lignes:,}  |  TN null : {rapport.fusion_pct_tn_null}%")
        print(f"  Chevauchement clés entre fichiers : {rapport.fusion_chevauchement_cles}")

    if args.postgres:
        print(f"\n{'=' * 60}\nPostgres public.climat_data")
        try:
            pg = _analyser_postgres()
            rapport.postgres = pg
            for k, v in pg.items():
                print(f"  {k}: {v}")
            if rapport.fusion_pct_tn_null is not None:
                ecart = abs(pg["pct_temp_min_null"] - rapport.fusion_pct_tn_null)
                if ecart < 1.0:
                    rapport.verdict = "Postgres ≈ brut : NULL probablement pas dû à l'ingestion."
                else:
                    rapport.verdict = f"Écart brut/Postgres {ecart:.1f} pt sur % TN null."
                print(f"\n  Verdict : {rapport.verdict}")
        except Exception as exc:
            print(f"  [ERREUR] {exc}", file=sys.stderr)
            return 1
    elif rapport.fichiers:
        p = rapport.fichiers[0].pct_tn_null
        rapport.verdict = f"~{p}% TN vides dans le brut (réseau mixte pluvio/thermo)."
        print(f"\nVerdict : {rapport.verdict}")

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(
            json.dumps(
                {
                    "fichiers": [asdict(f) for f in rapport.fichiers],
                    "fusion_lignes": rapport.fusion_lignes,
                    "fusion_pct_tn_null": rapport.fusion_pct_tn_null,
                    "fusion_chevauchement_cles": rapport.fusion_chevauchement_cles,
                    "postgres": rapport.postgres,
                    "verdict": rapport.verdict,
                },
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        print(f"\nJSON : {args.json}")

    return 0 if rapport.fichiers else 1


if __name__ == "__main__":
    sys.exit(main())

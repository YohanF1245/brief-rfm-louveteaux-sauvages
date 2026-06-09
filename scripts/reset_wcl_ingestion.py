#!/usr/bin/env python3
"""Repasse les reports WCL en ``pending`` pour ré-ingest (ex. buffs viewBy: Source)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))

from warcraftlogs_lake import (  # noqa: E402
    BRONZE_PATHS,
    read_delta_df,
    reset_ingestion_status_to_pending,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset ingestion_state WCL → pending")
    parser.add_argument(
        "--status",
        default="ok,error",
        help="Statuts à reset (défaut: ok,error). Ex: ok seulement",
    )
    parser.add_argument(
        "--report",
        action="append",
        dest="reports",
        metavar="CODE",
        help="Limiter à un ou plusieurs report_code (répéter l'option)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Affiche le nombre de reports concernés sans écrire",
    )
    args = parser.parse_args()

    from_statuses = tuple(s.strip() for s in args.status.split(",") if s.strip())
    if not from_statuses:
        print("Aucun statut valide.")
        return 1

    if args.dry_run:
        df = read_delta_df(BRONZE_PATHS["ingestion_state"])
        if df.empty:
            print("ingestion_state vide.")
            return 0
        mask = df["status"].astype(str).isin(from_statuses)
        if args.reports:
            wanted = {c.strip() for c in args.reports if c.strip()}
            mask &= df["report_code"].astype(str).isin(wanted)
        print(f"Dry-run : {int(mask.sum())} report(s) seraient repassés en pending.")
        return 0

    n = reset_ingestion_status_to_pending(
        from_statuses=from_statuses,
        report_codes=args.reports,
    )
    print(f"Terminé : {n} report(s) en pending. Lance warcraftlogs_guild_nightmares.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Assets Airflow — chaîne WCL pilotée par les données (asset-aware scheduling).

Graphe de déclenchement :

    warcraftlogs_guild_nightmares ──► WCL_BRONZE ─┐
    warcraftlogs_guild_roster     ──► WCL_ROSTER ─┤(OR)
                                                  ▼
    warcraftlogs_silver  (dbt silver) ──► WCL_SILVER
                                                  ▼
    warcraftlogs_gold    (dbt gold)   ──► WCL_GOLD

Un asset est marqué « updated » uniquement si la task productrice réussit ;
le DAG consommateur est alors planifié automatiquement (pas de cron dbt).
"""

from __future__ import annotations

from airflow.sdk import Asset

# Bronze events/masterData/playerDetails (DAG warcraftlogs_guild_nightmares)
WCL_BRONZE = Asset("wcl_bronze")

# Bronze roster guilde (DAG warcraftlogs_guild_roster)
WCL_ROSTER = Asset("wcl_bronze_roster")

# Couche silver ClickHouse (DAG warcraftlogs_silver)
WCL_SILVER = Asset("wcl_silver")

# Couche gold ClickHouse (DAG warcraftlogs_gold)
WCL_GOLD = Asset("wcl_gold")

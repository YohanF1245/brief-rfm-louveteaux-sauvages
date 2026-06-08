-- Gold analytique : DPS boss (table ``gold.wcl_boss_dps``, alimentée par le DAG WCL)
-- Prérequis : DAG ``warcraftlogs_guild_nightmares`` → tâches dbt_silver + dbt_gold

SELECT
    report_start_at,
    zone_name,
    report_title,
    fight_name,
    outcome,
    duration_sec,
    keystone_level,
    player_name,
    class_name,
    spec_name,
    item_level,
    round(dps, 0) AS dps
FROM gold.wcl_boss_dps
ORDER BY dps DESC
LIMIT 30;

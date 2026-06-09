-- Vérification couche silver (dbt → ClickHouse)
-- Play : https://ymfo1nom.com/play  |  tunnel SSH : localhost:8123
-- Prérequis : DAG warcraftlogs_lakehouse_dbt (tâche dbt_silver) OK

-- 1) Tables silver présentes
SELECT database, name, total_rows, total_bytes
FROM system.tables
WHERE database IN ('silver', 'gold_silver')
  AND name LIKE 'wcl_%'
ORDER BY database, name;

-- 2) Comptages par table (schéma silver attendu)
SELECT 'wcl_reports' AS table_name, count() AS n FROM silver.wcl_reports
UNION ALL SELECT 'wcl_fights', count() FROM silver.wcl_fights
UNION ALL SELECT 'wcl_player_fight_metrics', count() FROM silver.wcl_player_fight_metrics
UNION ALL SELECT 'wcl_ingestion_state', count() FROM silver.wcl_ingestion_state
ORDER BY n DESC;

-- 3) Si vide : anciennes tables au mauvais schéma (bug dbt corrigé)
SELECT 'gold_silver.wcl_reports' AS table_name, count() AS n FROM gold_silver.wcl_reports
UNION ALL SELECT 'gold_silver.wcl_fights', count() FROM gold_silver.wcl_fights
UNION ALL SELECT 'gold_silver.wcl_player_fight_metrics', count() FROM gold_silver.wcl_player_fight_metrics
UNION ALL SELECT 'gold_silver.wcl_ingestion_state', count() FROM gold_silver.wcl_ingestion_state
ORDER BY n DESC;

-- 4) Métriques ingérées (dps, buffs, casts, …)
SELECT metric, count() AS rows
FROM silver.wcl_player_fight_metrics
GROUP BY metric
ORDER BY rows DESC;

-- 5) Échantillon reports + zone
SELECT
    report_code,
    guild_name,
    zone_name,
    title,
    report_start_at
FROM silver.wcl_reports
ORDER BY report_start_at DESC
LIMIT 10;

-- 6) Échantillon DPS boss (jointure silver)
SELECT
    r.zone_name,
    f.fight_name,
    m.player_name,
    m.entry_name,
    round(m.rate_per_sec, 0) AS dps
FROM silver.wcl_player_fight_metrics AS m
INNER JOIN silver.wcl_fights AS f
    ON m.report_code = f.report_code AND m.fight_id = f.fight_id
INNER JOIN silver.wcl_reports AS r
    ON m.report_code = r.report_code
WHERE m.metric = 'dps' AND f.is_boss = 1
ORDER BY dps DESC
LIMIT 20;

-- 7) Buffs : volume global
SELECT
    count() AS buff_rows,
    countDistinct(report_code) AS reports,
    countDistinct(concat(report_code, ':', toString(fight_id))) AS fights,
    countDistinct(player_name) AS players,
    countDistinct(entry_name) AS distinct_buff_names
FROM silver.wcl_player_fight_metrics
WHERE metric = 'buffs';

-- 8) Top noms de buffs (discovery — ajuster patterns gold si vide en §9)
SELECT
    entry_name,
    count() AS rows,
    round(avg(coalesce(active_time_ms, entry_active_time_ms, total_amount, 0)) / 1000, 1) AS avg_uptime_sec
FROM silver.wcl_player_fight_metrics
WHERE metric = 'buffs' AND entry_name != ''
GROUP BY entry_name
ORDER BY rows DESC
LIMIT 50;

-- 9) Consommables classés (même logique que gold.wcl_raid_consumables_viz)
SELECT
    multiIf(
        match(lower(entry_name), 'well fed|feast|food|delicious|refrigerated|culinary|biscuit|skewer|steak|chops|fillet|sugar|bread|dessert|pierogi|sausage|omelet|omelette|goulash|stew|soup|pudding|cookie|cake|pie|tart|roast|ribs|meat|fish|banquet|bountiful|fated'),
        'food',
        match(lower(entry_name), 'flask'),
        'flask',
        match(lower(entry_name), 'oil|whetstone|sharpening|weightstone|mana oil|ironclaw'),
        'oil',
        'other_buff'
    ) AS consumable_type,
    count() AS rows
FROM silver.wcl_player_fight_metrics AS m
INNER JOIN silver.wcl_fights AS f
    ON m.report_code = f.report_code AND m.fight_id = f.fight_id
WHERE m.metric = 'buffs'
  AND f.is_boss = 1
  AND coalesce(f.keystone_level, 0) = 0
GROUP BY consumable_type
ORDER BY rows DESC;

-- 10) Échantillon uptime food / flacon / huile (boss raid)
SELECT
    r.zone_name,
    f.fight_name,
    m.player_name,
    m.entry_name,
    round(greatest(
        toFloat64(coalesce(m.active_time_ms, 0)),
        toFloat64(coalesce(m.total_amount, 0))
    ) / 1000, 1) AS uptime_sec,
    round(toFloat64(f.duration_ms) / 1000, 1) AS fight_duration_sec
FROM silver.wcl_player_fight_metrics AS m
INNER JOIN silver.wcl_fights AS f
    ON m.report_code = f.report_code AND m.fight_id = f.fight_id
INNER JOIN silver.wcl_reports AS r
    ON m.report_code = r.report_code
WHERE m.metric = 'buffs'
  AND f.is_boss = 1
  AND coalesce(f.keystone_level, 0) = 0
  AND (
      match(lower(m.entry_name), 'flask')
      OR match(lower(m.entry_name), 'well fed|feast|food')
      OR match(lower(m.entry_name), 'oil|whetstone|sharpening')
  )
ORDER BY r.report_start_at DESC
LIMIT 30;

-- 11) Si buff_rows = 0 : bronze a-t-il des buffs ? (via vues legacy)
-- SELECT metric, count() FROM v_wcl_fight_player_stats WHERE metric = 'buffs' GROUP BY metric;

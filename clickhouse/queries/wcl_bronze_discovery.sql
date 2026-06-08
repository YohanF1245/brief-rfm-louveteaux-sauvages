-- =============================================================================
-- Découverte schéma bronze WCL — pour concevoir la couche silver
-- Remplace lake-prod / TON_MDP (MINIO_ROOT_*)
-- =============================================================================

-- 1) Colonnes de chaque table Delta (DESCRIBE)
DESCRIBE deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/guild_reports',
    'lake-prod', 'TON_MDP'
);

DESCRIBE deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/fights',
    'lake-prod', 'TON_MDP'
);

DESCRIBE deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/fight_player_stats',
    'lake-prod', 'TON_MDP'
);

DESCRIBE deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/ingestion_state',
    'lake-prod', 'TON_MDP'
);

DESCRIBE deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/fight_tables_raw',
    'lake-prod', 'TON_MDP'
);

DESCRIBE deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/reports_raw',
    'lake-prod', 'TON_MDP'
);

-- 2) Volumes bronze
SELECT 'guild_reports' AS t, count() AS n
FROM deltaLake('http://minio:9000/lake/bronze/warcraftlogs/guild_reports', 'lake-prod', 'TON_MDP')
UNION ALL SELECT 'fights', count()
FROM deltaLake('http://minio:9000/lake/bronze/warcraftlogs/fights', 'lake-prod', 'TON_MDP')
UNION ALL SELECT 'fight_player_stats', count()
FROM deltaLake('http://minio:9000/lake/bronze/warcraftlogs/fight_player_stats', 'lake-prod', 'TON_MDP')
UNION ALL SELECT 'fight_tables_raw', count()
FROM deltaLake('http://minio:9000/lake/bronze/warcraftlogs/fight_tables_raw', 'lake-prod', 'TON_MDP')
UNION ALL SELECT 'reports_raw', count()
FROM deltaLake('http://minio:9000/lake/bronze/warcraftlogs/reports_raw', 'lake-prod', 'TON_MDP')
UNION ALL SELECT 'ingestion_state', count()
FROM deltaLake('http://minio:9000/lake/bronze/warcraftlogs/ingestion_state', 'lake-prod', 'TON_MDP');

-- 3) Toutes les clés JSON dans extra (fight_player_stats) — par métrique WCL
SELECT
    metric,
    arrayJoin(JSONExtractKeys(assumeNotNull(extra))) AS json_key,
    count() AS n
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/fight_player_stats',
    'lake-prod', 'TON_MDP'
)
WHERE extra IS NOT NULL AND extra != ''
GROUP BY metric, json_key
ORDER BY metric, n DESC;

-- 4) Types API bruts (fight_tables_raw)
SELECT
    data_type,
    count() AS rows,
    uniqExact(report_code) AS reports,
    round(avg(length(raw_json)), 0) AS avg_json_bytes
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/fight_tables_raw',
    'lake-prod', 'TON_MDP'
)
GROUP BY data_type
ORDER BY rows DESC;

-- 5) Échantillon extra (une clé = itemLevel sur dps)
SELECT
    metric,
    player_name,
    JSONExtractInt(extra, 'itemLevel') AS item_level,
    JSONExtractString(extra, 'name') AS entry_name,
    substring(extra, 1, 500) AS extra_sample
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/fight_player_stats',
    'lake-prod', 'TON_MDP'
)
WHERE metric IN ('dps', 'summary')
LIMIT 10;

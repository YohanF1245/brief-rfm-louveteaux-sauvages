-- Top DPS avec date du log, durée du fight, niveau de clé M+ et ilvl.
-- ClickHouse 25.8 : deltaLake(url, access_key, secret_key)

SELECT
    toDateTime64(r.start_time_ms / 1000, 3, 'UTC') AS report_date,
    r.zone_name,
    r.title AS report_title,
    f.fight_name,
    if(f.kill, 'kill', 'wipe') AS outcome,
    round(f.duration_ms / 1000, 1) AS fight_duration_sec,
    f.keystone_level,
    f.difficulty,
    s.player_name,
    s.class_name,
    JSONExtractInt(s.extra, 'item_level') AS item_level,
    round(s.rate_per_sec, 0) AS dps
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/fight_player_stats',
    'minioadmin', 'minioadmin'
) AS s
INNER JOIN deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/fights',
    'minioadmin', 'minioadmin'
) AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/guild_reports',
    'minioadmin', 'minioadmin'
) AS r ON s.report_code = r.report_code
WHERE s.metric = 'dps'
  AND f.is_boss = true
ORDER BY dps DESC
LIMIT 30;

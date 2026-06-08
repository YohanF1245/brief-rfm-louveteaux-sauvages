-- ClickHouse 25.8 : deltaLake(url, access_key, secret_key) — PAS de 'Parquet'.

SELECT log_source, owner_name, title, zone_name
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/guild_reports',
    'minioadmin',
    'minioadmin'
)
ORDER BY start_time_ms DESC
LIMIT 20;

{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

SELECT
    report_code,
    guild_id,
    guild_name,
    server_region,
    server_slug,
    title,
    zone_name,
    owner_name,
    toInt64OrNull(toString(owner_user_id)) AS owner_user_id,
    log_source,
    visibility,
    toDateTime64(start_time_ms / 1000, 3, 'UTC') AS report_start_at,
    toDateTime64(end_time_ms / 1000, 3, 'UTC') AS report_end_at,
    toDateTime64(fetched_at, 3, 'UTC') AS bronze_fetched_at,
    now() AS _silver_loaded_at
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/guild_reports',
    '{{ env_var("MINIO_ROOT_USER", "minioadmin") }}',
    '{{ env_var("MINIO_ROOT_PASSWORD", "minioadmin") }}'
)

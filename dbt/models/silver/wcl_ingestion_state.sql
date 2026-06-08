{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

SELECT
    report_code,
    status,
    title,
    toInt32OrNull(toString(ingestion_attempts)) AS ingestion_attempts,
    toDateTime64(start_time_ms / 1000, 3, 'UTC') AS report_start_at,
    parseDateTime64BestEffortOrNull(toString(synced_at), 3, 'UTC') AS synced_at,
    substring(last_error, 1, 500) AS last_error_preview,
    parseDateTime64BestEffortOrNull(toString(fetched_at), 3, 'UTC') AS bronze_fetched_at,
    now() AS _silver_loaded_at
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/ingestion_state',
    '{{ env_var("MINIO_ROOT_USER", "minioadmin") }}',
    '{{ env_var("MINIO_ROOT_PASSWORD", "minioadmin") }}'
)

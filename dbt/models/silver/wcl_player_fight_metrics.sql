{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

/*
  Silver métriques joueur : colonnes bronze + champs JSON extra (camelCase WCL).
  Clés extra les plus utiles : itemLevel, id, name, total, activeTime, type, spec.
  Requête discovery : clickhouse/queries/wcl_bronze_discovery.sql §3
*/
SELECT
    report_code,
    toInt32(fight_id) AS fight_id,
    player_name,
    metric,
    toInt64OrNull(toString(player_id)) AS player_id,
    class_name,
    spec_name,
    toFloat64OrNull(toString(total_amount)) AS total_amount,
    toInt64OrNull(toString(active_time_ms)) AS active_time_ms,
    toFloat64OrNull(toString(rate_per_sec)) AS rate_per_sec,
    JSONExtractInt(extra, 'itemLevel') AS item_level,
    JSONExtractString(extra, 'name') AS entry_name,
    JSONExtractInt(extra, 'id') AS entry_id,
    JSONExtractInt(extra, 'total') AS entry_total,
    JSONExtractInt(extra, 'activeTime') AS entry_active_time_ms,
    extra AS extra_json,
    toDateTime64(fetched_at, 3, 'UTC') AS bronze_fetched_at,
    now() AS _silver_loaded_at
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/fight_player_stats',
    '{{ env_var("MINIO_ROOT_USER", "minioadmin") }}',
    '{{ env_var("MINIO_ROOT_PASSWORD", "minioadmin") }}'
)

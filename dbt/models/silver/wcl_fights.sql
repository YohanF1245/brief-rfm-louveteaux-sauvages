{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

/*
  ``start_time_ms`` / ``end_time_ms`` sont RELATIFS au début du report
  (même référentiel que ``events.timestamp_ms``). L'heure absolue se calcule
  en gold : ``report_start_at + INTERVAL start_time_ms MILLISECOND``.
*/
SELECT
    report_code,
    toInt32(fight_id) AS fight_id,
    toInt32(encounter_id) AS encounter_id,
    fight_name,
    toInt64(start_time_ms) AS start_time_ms,
    toInt64(end_time_ms) AS end_time_ms,
    toInt64OrNull(toString(duration_ms)) AS duration_ms,
    toFloat64(duration_ms) / 1000.0 AS duration_sec,
    toUInt8(kill) AS is_kill,
    toInt32OrNull(toString(difficulty)) AS difficulty,
    toInt32OrNull(toString(size)) AS raid_size,
    toFloat64OrNull(toString(boss_percentage)) AS boss_percentage,
    toInt32OrNull(toString(keystone_level)) AS keystone_level,
    toInt64OrNull(toString(keystone_time_ms)) AS keystone_time_ms,
    toUInt8(is_boss) AS is_boss,
    toDateTime64(fetched_at, 3, 'UTC') AS bronze_fetched_at,
    now() AS _silver_loaded_at
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/fights',
    '{{ env_var("MINIO_ROOT_USER", "minioadmin") }}',
    '{{ env_var("MINIO_ROOT_PASSWORD", "minioadmin") }}'
)

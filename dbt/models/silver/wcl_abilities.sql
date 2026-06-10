{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

/*
  Dimension sorts (``masterData.abilities``).
  ``ability_game_id`` joint ``wcl_events.ability_game_id``.
  ``name`` est dans la langue du log (cf. ``master_info.lang``) → les regex
  consommables (macro ``wcl_consumable_type``) couvrent FR + EN.
*/
SELECT
    report_code,
    toInt64(ability_game_id) AS ability_game_id,
    name,
    ability_type,
    icon,
    toDateTime64(fetched_at, 3, 'UTC') AS bronze_fetched_at,
    now() AS _silver_loaded_at
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/master_abilities',
    '{{ env_var("MINIO_ROOT_USER", "minioadmin") }}',
    '{{ env_var("MINIO_ROOT_PASSWORD", "minioadmin") }}'
)
WHERE ability_game_id IS NOT NULL

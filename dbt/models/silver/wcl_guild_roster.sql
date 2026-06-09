{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

/*
  Roster guilde WCL (API ``guild.members``).
  ``player_guid`` = ``canonical_id`` WCL — clé de jointure avec ``composition.guid`` des logs.
*/
SELECT
    toInt64OrNull(toString(guild_id)) AS guild_id,
    guild_name,
    server_region,
    server_slug,
    toInt64OrNull(toString(wcl_character_id)) AS wcl_character_id,
    toUInt64OrNull(toString(canonical_id)) AS canonical_id,
    toUInt64OrNull(toString(player_guid)) AS player_guid,
    character_name,
    toInt32OrNull(toString(class_id)) AS class_id,
    class_name,
    toInt32OrNull(toString(character_level)) AS character_level,
    toInt32OrNull(toString(guild_rank)) AS guild_rank,
    toDateTime64(fetched_at, 3, 'UTC') AS bronze_fetched_at,
    now() AS _silver_loaded_at
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/guild_roster',
    '{{ env_var("MINIO_ROOT_USER", "minioadmin") }}',
    '{{ env_var("MINIO_ROOT_PASSWORD", "minioadmin") }}'
)
WHERE character_name != ''
  AND player_guid IS NOT NULL
  AND player_guid > 0

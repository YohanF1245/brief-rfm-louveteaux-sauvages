{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

/*
  Mapping fight-local ``player_id`` → ``player_guid`` (WoW) depuis la metric ``summary``.
  Sous-requête : évite ILLEGAL_AGGREGATION ClickHouse (filtre avant GROUP BY).
*/
SELECT
    report_code,
    fight_id,
    player_id,
    any(player_name) AS player_name,
    max(wow_guid) AS player_guid
FROM (
    SELECT
        report_code,
        toInt32(fight_id) AS fight_id,
        toInt64OrNull(toString(player_id)) AS player_id,
        player_name,
        toUInt64OrNull(toString(JSONExtractUInt(extra, 'guid'))) AS wow_guid
    FROM deltaLake(
        'http://minio:9000/lake/bronze/warcraftlogs/fight_player_stats',
        '{{ env_var("MINIO_ROOT_USER", "minioadmin") }}',
        '{{ env_var("MINIO_ROOT_PASSWORD", "minioadmin") }}'
    )
    WHERE metric = 'summary'
      AND player_id IS NOT NULL
      AND JSONExtractUInt(extra, 'guid') > 0
      AND player_name != ''
      AND lower(player_name) != 'unknown'
) AS src
GROUP BY report_code, fight_id, player_id

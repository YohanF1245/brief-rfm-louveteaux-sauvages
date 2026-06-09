{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

/*
  Mapping fight-local ``player_id`` → ``player_guid`` (WoW) depuis la metric ``summary``.
  Exemple WCL composition : ``{"id": 1, "guid": 152344242, "name": "Kîmahri", ...}``.
*/
SELECT
    report_code,
    fight_id,
    player_id,
    any(player_name) AS player_name,
    max(player_guid) AS player_guid
FROM {{ ref('wcl_player_fight_metrics') }}
WHERE metric = 'summary'
  AND player_id IS NOT NULL
  AND player_guid IS NOT NULL
  AND player_guid > 0
  AND player_name != ''
  AND lower(player_name) != 'unknown'
GROUP BY report_code, fight_id, player_id

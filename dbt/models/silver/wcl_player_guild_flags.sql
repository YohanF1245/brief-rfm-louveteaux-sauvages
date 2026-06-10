{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

/*
  Flag membre guilde unifié : 1 ligne = joueur × report.

  Agrège ``wcl_player_details`` (rôle, playerDetails API) et
  ``wcl_actors`` (noms résolus depuis events / masterData) pour que le gold
  ne rate pas un membre guilde quand le GUID playerDetails ≠ roster mais
  ``master_actors.game_id`` matche, ou quand le nom diffère légèrement.
*/
SELECT
    report_code,
    player_name,
    max(is_guild_member) AS is_guild_member,
    anyIf(player_guild_name, is_guild_member = 1) AS player_guild_name,
    anyIf(roster_character_name, is_guild_member = 1) AS roster_character_name,
    anyIf(role, role IS NOT NULL AND role != '') AS role,
    now() AS _silver_loaded_at
FROM (
    SELECT
        report_code,
        player_name,
        is_guild_member,
        player_guild_name,
        roster_character_name,
        role
    FROM {{ ref('wcl_player_details') }}

    UNION ALL

    SELECT
        report_code,
        resolved_player_name AS player_name,
        is_guild_member,
        player_guild_name,
        roster_character_name,
        CAST(NULL AS Nullable(String)) AS role
    FROM {{ ref('wcl_actors') }}
    WHERE resolved_actor_type = 'Player'
      AND resolved_player_name != ''
) AS src
GROUP BY report_code, player_name

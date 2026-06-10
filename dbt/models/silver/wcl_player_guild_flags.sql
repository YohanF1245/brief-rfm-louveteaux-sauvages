{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

/*
  Flag membre guilde unifié : 1 ligne = joueur × report.

  Agrège ``wcl_player_details`` (rôle, playerDetails API) et
  ``wcl_actors`` (noms résolus depuis events / masterData).

  Logique d'agrégation : macro ``wcl_guild_flags_aggregate_sql`` (colonnes
  internes ``src_*`` pour éviter ILLEGAL_AGGREGATION ClickHouse).
*/
SELECT
    report_code,
    player_name,
    is_guild_member,
    player_guild_name,
    roster_character_name,
    role,
    now() AS _silver_loaded_at
FROM (
    {{ wcl_guild_flags_aggregate_sql(include_roster_character_name=true) }}
) AS gf

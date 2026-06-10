{{ config(
    materialized='table',
    schema='gold',
    tags=['warcraftlogs', 'gold', 'power_bi']
) }}

/*
  Présence des membres de guilde : 1 ligne = membre du roster × report.
  ``attended`` = le joueur apparaît dans le report (playerDetails).
  KPI : assiduité par joueur, effectif moyen par soirée, membres inactifs.

  NB : produit cartésien roster × reports — volumes faibles (dizaines de
  membres × centaines de reports), assumé pour simplifier le BI.
*/
WITH reports AS (
    SELECT
        report_code,
        report_start_at,
        toDate(report_start_at) AS report_date,
        title AS report_title,
        zone_name AS raid_or_dungeon,
        guild_name AS report_guild_name
    FROM {{ ref('wcl_reports') }}
),
roster AS (
    SELECT
        player_guid,
        any(character_name) AS character_name,
        any(class_name) AS class_name,
        any(guild_rank) AS guild_rank
    FROM {{ ref('wcl_guild_roster') }}
    GROUP BY player_guid
),
participation AS (
    SELECT DISTINCT report_code, player_guid
    FROM {{ ref('wcl_player_details') }}
    WHERE player_guid IS NOT NULL
)
SELECT
    rep.report_start_at AS report_start_at,
    rep.report_date AS report_date,
    rep.report_code AS report_code,
    rep.report_title AS report_title,
    rep.raid_or_dungeon AS raid_or_dungeon,
    rep.report_guild_name AS report_guild_name,
    ros.player_guid AS player_guid,
    ros.character_name AS character_name,
    ros.class_name AS class_name,
    ros.guild_rank AS guild_rank,
    if(p.player_guid IS NOT NULL, 1, 0) AS attended,
    now() AS _gold_loaded_at
FROM reports AS rep
CROSS JOIN roster AS ros
LEFT JOIN participation AS p
    ON rep.report_code = p.report_code AND ros.player_guid = p.player_guid

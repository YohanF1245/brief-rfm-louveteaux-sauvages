{{ config(
    materialized='table',
    schema='gold',
    tags=['warcraftlogs', 'gold', 'power_bi']
) }}

/*
  Gold WCL : DPS boss, grain joueur × fight × report.
  Jointure roster via ``player_guid`` (composition.summary.guid ↔ API guild.members).
*/
SELECT
    r.report_start_at,
    toDate(r.report_start_at) AS report_date,
    r.guild_name AS report_guild_name,
    gr.guild_name AS player_guild_name,
    r.zone_name,
    r.title AS report_title,
    f.fight_name,
    f.encounter_id,
    f.difficulty,
    f.raid_size,
    if(f.is_kill = 1, 'kill', 'wipe') AS outcome,
    f.duration_sec,
    f.keystone_level,
    coalesce(nullIf(gr.character_name, ''), g.player_name, s.player_name) AS player_name,
    s.class_name,
    s.spec_name,
    s.item_level,
    s.total_amount,
    round(s.rate_per_sec, 2) AS dps,
    s.player_id,
    g.player_guid,
    if(gr.player_guid IS NOT NULL, 1, 0) AS is_guild_member,
    s.report_code AS report_code,
    s.fight_id AS fight_id,
    now() AS _gold_loaded_at
FROM {{ ref('wcl_player_fight_metrics') }} AS s
INNER JOIN {{ ref('wcl_fights') }} AS f
    ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN {{ ref('wcl_reports') }} AS r
    ON s.report_code = r.report_code
LEFT JOIN {{ ref('wcl_fight_player_guids') }} AS g
    ON s.report_code = g.report_code
    AND s.fight_id = g.fight_id
    AND s.player_id = g.player_id
LEFT JOIN {{ ref('wcl_guild_roster') }} AS gr
    ON g.player_guid = gr.player_guid
WHERE s.metric = 'dps'
  AND f.is_boss = 1

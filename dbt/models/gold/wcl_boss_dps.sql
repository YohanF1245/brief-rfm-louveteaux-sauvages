{{ config(
    enabled=false,
    materialized='table',
    schema='gold',
    tags=['warcraftlogs', 'gold', 'power_bi']
) }}

/*
  OBSOLÈTE (enabled=false) : dépendait de wcl_player_fight_metrics (supprimé).
  À reconstruire depuis bronze ``events`` (type=damage) × ``master_actors``.

  Gold WCL : DPS boss, grain joueur × fight × report.
  Jointure roster via ``player_guid`` (composition.summary.guid ↔ API guild.members).
*/
SELECT
    r.report_start_at AS report_start_at,
    toDate(r.report_start_at) AS report_date,
    r.guild_name AS report_guild_name,
    gr.guild_name AS player_guild_name,
    r.zone_name AS zone_name,
    r.title AS report_title,
    f.fight_name AS fight_name,
    f.encounter_id AS encounter_id,
    f.difficulty AS difficulty,
    f.raid_size AS raid_size,
    if(f.is_kill = 1, 'kill', 'wipe') AS outcome,
    f.duration_sec AS duration_sec,
    f.keystone_level AS keystone_level,
    coalesce(nullIf(gr.character_name, ''), g.player_name, s.player_name) AS player_name,
    s.class_name AS class_name,
    s.spec_name AS spec_name,
    s.item_level AS item_level,
    s.total_amount AS total_amount,
    round(s.rate_per_sec, 2) AS dps,
    s.player_id AS player_id,
    g.player_guid AS player_guid,
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

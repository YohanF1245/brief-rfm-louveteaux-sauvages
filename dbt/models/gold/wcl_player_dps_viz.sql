{{ config(
    enabled=false,
    materialized='table',
    schema='gold',
    tags=['warcraftlogs', 'gold', 'power_bi']
) }}

/*
  OBSOLÈTE (enabled=false) : dépendait de wcl_player_fight_metrics (supprimé).
  À reconstruire depuis bronze ``events`` (type=damage) × ``master_actors``.

  Gold viz DPS : même grain que ``wcl_boss_dps`` + colonnes Streamlit / Power BI.
  Pas de ``ref(wcl_boss_dps)`` : évite les vues ClickHouse stale (alias ``s.``).
*/
SELECT
    r.report_start_at AS report_start_at,
    toDate(r.report_start_at) AS report_date,
    coalesce(nullIf(gr.character_name, ''), g.player_name, s.player_name) AS player_name,
    r.guild_name AS report_guild_name,
    r.guild_name AS guild_name,
    gr.guild_name AS player_guild_name,
    round(s.rate_per_sec, 2) AS dps,
    s.class_name AS class_name,
    s.spec_name AS spec_name,
    s.item_level AS item_level,
    r.zone_name AS zone_name,
    r.zone_name AS raid_or_dungeon,
    f.fight_name AS fight_name,
    f.fight_name AS boss_name,
    f.encounter_id AS encounter_id,
    f.difficulty AS difficulty,
    multiIf(
        f.keystone_level > 0, 'Mythic+',
        f.difficulty = 1, 'LFR',
        f.difficulty = 3, 'Normal',
        f.difficulty = 4, 'Heroic',
        f.difficulty = 5, 'Mythic',
        f.difficulty IS NOT NULL, concat('diff_', toString(f.difficulty)),
        'Unknown'
    ) AS difficulty_label,
    f.keystone_level AS keystone_level,
    if(f.keystone_level > 0, 'Mythic+', 'Raid') AS content_type,
    if(lower(coalesce(r.guild_name, '')) LIKE '%nightmares asylum%', 1, 0) AS is_nightmares_asylum,
    if(gr.player_guid IS NOT NULL, 1, 0) AS is_guild_member,
    g.player_guid AS player_guid,
    if(f.is_kill = 1, 'kill', 'wipe') AS outcome,
    f.duration_sec AS duration_sec,
    f.raid_size AS raid_size,
    r.title AS report_title,
    s.player_id AS player_id,
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

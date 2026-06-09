{{ config(
    materialized='table',
    schema='gold',
    tags=['warcraftlogs', 'gold', 'power_bi']
) }}

/*
  Gold viz : DPS boss enrichi pour Power BI / Streamlit.
  Grain : joueur × fight × report.
  Filtre membre guilde : ``is_guild_member = 1`` (jointure ``player_guid`` ↔ roster API).
*/
SELECT
    report_start_at,
    report_date,
    player_name,
    report_guild_name AS guild_name,
    player_guild_name,
    dps,
    class_name,
    spec_name,
    item_level,
    zone_name AS raid_or_dungeon,
    fight_name AS boss_name,
    encounter_id,
    difficulty,
    multiIf(
        keystone_level > 0, 'Mythic+',
        difficulty = 1, 'LFR',
        difficulty = 3, 'Normal',
        difficulty = 4, 'Heroic',
        difficulty = 5, 'Mythic',
        difficulty IS NOT NULL, concat('diff_', toString(difficulty)),
        'Unknown'
    ) AS difficulty_label,
    keystone_level,
    if(keystone_level > 0, 'Mythic+', 'Raid') AS content_type,
    if(lower(coalesce(report_guild_name, '')) LIKE '%nightmares asylum%', 1, 0) AS is_nightmares_asylum,
    is_guild_member,
    player_guid,
    outcome,
    duration_sec,
    raid_size,
    report_title,
    player_id,
    report_code,
    fight_id,
    now() AS _gold_loaded_at
FROM {{ ref('wcl_boss_dps') }}

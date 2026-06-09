{{ config(
    materialized='table',
    schema='gold',
    tags=['warcraftlogs', 'gold', 'power_bi']
) }}

/*
  Gold viz : DPS boss enrichi pour Power BI / Streamlit.
  Colonnes explicites depuis ``wcl_boss_dps`` (évite mismatch schéma ClickHouse post full-refresh).
*/
SELECT
    b.report_start_at,
    b.report_date,
    b.player_name,
    b.report_guild_name AS guild_name,
    b.player_guild_name,
    b.dps,
    b.class_name,
    b.spec_name,
    b.item_level,
    b.zone_name AS raid_or_dungeon,
    b.fight_name AS boss_name,
    b.encounter_id,
    b.difficulty,
    multiIf(
        b.keystone_level > 0, 'Mythic+',
        b.difficulty = 1, 'LFR',
        b.difficulty = 3, 'Normal',
        b.difficulty = 4, 'Heroic',
        b.difficulty = 5, 'Mythic',
        b.difficulty IS NOT NULL, concat('diff_', toString(b.difficulty)),
        'Unknown'
    ) AS difficulty_label,
    b.keystone_level,
    if(b.keystone_level > 0, 'Mythic+', 'Raid') AS content_type,
    if(lower(coalesce(b.report_guild_name, '')) LIKE '%nightmares asylum%', 1, 0) AS is_nightmares_asylum,
    b.is_guild_member,
    b.player_guid,
    b.outcome,
    b.duration_sec,
    b.raid_size,
    b.report_title,
    b.player_id,
    b.report_code,
    b.fight_id,
    now() AS _gold_loaded_at
FROM {{ ref('wcl_boss_dps') }} AS b

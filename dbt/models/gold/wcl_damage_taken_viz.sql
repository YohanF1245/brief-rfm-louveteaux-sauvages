{{ config(
    materialized='table',
    schema='gold',
    tags=['warcraftlogs', 'gold', 'power_bi']
) }}

/*
  DTPS détaillé : 1 ligne = joueur × source de dégâts × sort × fight.
  Répond à « qui prend des dégâts, de quoi, et sur quel sort » :
  dégâts évitables, tanks vs raid damage, comparaison entre pulls d'un boss.
*/
SELECT
    r.report_start_at,
    toDate(r.report_start_at) AS report_date,
    r.report_code,
    r.guild_name AS report_guild_name,
    r.zone_name AS raid_or_dungeon,
    f.fight_id,
    f.fight_name AS boss_name,
    f.is_boss,
    f.is_kill,
    {{ wcl_difficulty_label('f.difficulty', 'f.keystone_level') }} AS difficulty_label,
    f.keystone_level,
    f.duration_sec AS fight_duration_sec,
    tgt.resolved_player_name AS player_name,
    tgt.player_guid,
    tgt.resolved_class AS class_name,
    coalesce(gf.is_guild_member, 0) AS is_guild_member,
    coalesce(src.name, '(inconnu)') AS source_name,
    coalesce(src.sub_type, '') AS source_type,
    coalesce(ab.name, concat('ability_', toString(e.ability_game_id))) AS ability_name,
    sum(coalesce(e.amount, 0)) AS damage_taken,
    round(sum(coalesce(e.amount, 0)) / nullIf(f.duration_sec, 0), 0) AS dtps,
    sum(coalesce(e.absorbed, 0)) AS damage_absorbed,
    sum(coalesce(e.unmitigated_amount, 0)) AS unmitigated_damage,
    count() AS hits,
    countIf(coalesce(e.overkill, 0) > 0) AS killing_hits,
    max(coalesce(e.amount, 0)) AS max_hit,
    now() AS _gold_loaded_at
FROM {{ ref('wcl_events') }} AS e
INNER JOIN {{ ref('wcl_actors') }} AS tgt
    ON e.report_code = tgt.report_code AND e.target_id = tgt.actor_id
LEFT JOIN {{ ref('wcl_actors') }} AS src
    ON e.report_code = src.report_code AND e.source_id = src.actor_id
LEFT JOIN {{ ref('wcl_abilities') }} AS ab
    ON e.report_code = ab.report_code AND e.ability_game_id = ab.ability_game_id
INNER JOIN {{ ref('wcl_fights') }} AS f
    ON e.report_code = f.report_code AND e.fight_id = f.fight_id
INNER JOIN {{ ref('wcl_reports') }} AS r
    ON e.report_code = r.report_code
LEFT JOIN {{ ref('wcl_player_guild_flags') }} AS gf
    ON e.report_code = gf.report_code AND tgt.resolved_player_name = gf.player_name
WHERE e.event_type = 'damage'
  AND tgt.resolved_actor_type = 'Player'
  AND f.duration_sec > 0
GROUP BY
    r.report_start_at, report_date, r.report_code, report_guild_name,
    raid_or_dungeon, f.fight_id, boss_name, f.is_boss, f.is_kill,
    difficulty_label, f.keystone_level, fight_duration_sec,
    player_name, tgt.player_guid, class_name, is_guild_member,
    source_name, source_type, ability_name

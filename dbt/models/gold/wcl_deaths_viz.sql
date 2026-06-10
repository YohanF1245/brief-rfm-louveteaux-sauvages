{{ config(
    materialized='table',
    schema='gold',
    tags=['warcraftlogs', 'gold', 'power_bi']
) }}

/*
  Morts : 1 ligne = 1 mort de joueur.
  ``death_rank`` = ordre de mort dans le fight (1 = premier mort → souvent
  le pull qui « casse »). ``killing_ability`` / ``killer_name`` si présents
  dans l'event (nullable selon les logs).
  KPI : morts évitables, joueurs qui meurent tôt, wipes analysés.
*/
SELECT
    r.report_start_at,
    toDate(r.report_start_at) AS report_date,
    e.report_code,
    r.guild_name AS report_guild_name,
    r.zone_name AS raid_or_dungeon,
    f.fight_id,
    f.fight_name AS boss_name,
    f.is_boss,
    f.is_kill,
    {{ wcl_difficulty_label('f.difficulty', 'f.keystone_level') }} AS difficulty_label,
    f.duration_sec AS fight_duration_sec,
    a.resolved_player_name AS player_name,
    a.player_guid,
    a.resolved_class AS class_name,
    coalesce(gf.is_guild_member, 0) AS is_guild_member,
    round((e.timestamp_ms - f.start_time_ms) / 1000.0, 1) AS death_at_sec,
    round(100.0 * (e.timestamp_ms - f.start_time_ms) / nullIf(f.duration_ms, 0), 1) AS death_at_pct_of_fight,
    row_number() OVER (
        PARTITION BY e.report_code, e.fight_id
        ORDER BY e.timestamp_ms
    ) AS death_rank,
    ab.name AS killing_ability,
    killer.name AS killer_name,
    now() AS _gold_loaded_at
FROM {{ ref('wcl_events') }} AS e
INNER JOIN {{ ref('wcl_actors') }} AS a
    ON e.report_code = a.report_code AND e.target_id = a.actor_id
INNER JOIN {{ ref('wcl_fights') }} AS f
    ON e.report_code = f.report_code AND e.fight_id = f.fight_id
INNER JOIN {{ ref('wcl_reports') }} AS r
    ON e.report_code = r.report_code
LEFT JOIN {{ ref('wcl_abilities') }} AS ab
    ON e.report_code = ab.report_code AND e.ability_game_id = ab.ability_game_id
LEFT JOIN {{ ref('wcl_actors') }} AS killer
    ON e.report_code = killer.report_code AND e.killer_id = killer.actor_id
LEFT JOIN (
    SELECT report_code, player_name, max(is_guild_member) AS is_guild_member
    FROM {{ ref('wcl_player_details') }}
    GROUP BY report_code, player_name
) AS gf
    ON e.report_code = gf.report_code AND a.resolved_player_name = gf.player_name
WHERE e.event_type = 'death'
  AND a.resolved_actor_type = 'Player'
  AND f.duration_sec > 0

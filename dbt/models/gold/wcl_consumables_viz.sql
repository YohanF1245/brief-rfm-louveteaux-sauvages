{{ config(
    materialized='table',
    schema='gold',
    tags=['warcraftlogs', 'gold', 'power_bi']
) }}

/*
  Consommables : 1 ligne = joueur × consommable × fight.

  Deux familles :
  - Buffs (flask, food, augment_rune, weapon_buff) → ``uptime_ms`` exact,
    calculé par intervalles : état au pull (``wcl_pull_auras``, car un buff
    posé AVANT le pull n'émet pas d'applybuff pendant le fight) + transitions
    applybuff/removebuff, replié via arrayFold jusqu'à la fin du fight.
  - Casts (potion, healthstone) → ``casts`` (uptime NULL).

  KPI : présence au pull, uptime % par fight, uptime moyen par soirée,
  préparation des joueurs (% de pulls avec flask/food).
*/
WITH {{ wcl_guild_flags_cte() }},
consumable_abilities AS (
    SELECT
        report_code,
        ability_game_id,
        any(name) AS ability_name,
        {{ wcl_consumable_type('any(name)') }} AS consumable_type
    FROM {{ ref('wcl_abilities') }}
    GROUP BY report_code, ability_game_id
    HAVING consumable_type != ''
),
-- Transitions en combat (applybuff/refreshbuff = actif, removebuff = inactif)
buff_changes AS (
    SELECT
        e.report_code AS report_code,
        e.fight_id AS fight_id,
        e.target_id AS player_actor_id,
        e.ability_game_id AS ability_game_id,
        ca.ability_name AS ability_name,
        ca.consumable_type AS consumable_type,
        toInt64(e.timestamp_ms) AS ts,
        toUInt8(if(e.event_type = 'removebuff', 0, 1)) AS state
    FROM {{ ref('wcl_events') }} AS e
    INNER JOIN consumable_abilities AS ca
        ON e.report_code = ca.report_code AND e.ability_game_id = ca.ability_game_id
    WHERE e.event_type IN ('applybuff', 'refreshbuff', 'removebuff')
      AND ca.consumable_type IN ('flask', 'food', 'augment_rune', 'weapon_buff')
),
-- État au pull : aura déjà active au début du fight.
-- Alias explicites : avec 2+ JOINs dans un CTE, ClickHouse garde les noms
-- qualifiés (pa.report_code) en sortie → l'UNION/JOIN aval ne résout plus.
pull_seeds AS (
    SELECT
        pa.report_code AS report_code,
        pa.fight_id AS fight_id,
        pa.player_actor_id AS player_actor_id,
        pa.ability_game_id AS ability_game_id,
        coalesce(pa.aura_name, ca.ability_name, concat('ability_', toString(pa.ability_game_id))) AS ability_name,
        {{ wcl_consumable_type("coalesce(pa.aura_name, ca.ability_name, '')") }} AS consumable_type,
        toInt64(f.start_time_ms) AS ts,
        toUInt8(1) AS state
    FROM {{ ref('wcl_pull_auras') }} AS pa
    INNER JOIN {{ ref('wcl_fights') }} AS f
        ON pa.report_code = f.report_code AND pa.fight_id = f.fight_id
    LEFT JOIN consumable_abilities AS ca
        ON pa.report_code = ca.report_code AND pa.ability_game_id = ca.ability_game_id
    WHERE {{ wcl_consumable_type("coalesce(pa.aura_name, ca.ability_name, '')") }}
          IN ('flask', 'food', 'augment_rune', 'weapon_buff')
),
buff_uptime AS (
    SELECT
        c.report_code AS report_code,
        c.fight_id AS fight_id,
        c.player_actor_id AS player_actor_id,
        c.ability_game_id AS ability_game_id,
        any(c.ability_name) AS ability_name,
        any(c.consumable_type) AS consumable_type,
        max(c.state = 1 AND c.ts <= toInt64(f.start_time_ms)) AS present_at_pull,
        arrayFold(
            (acc, x) -> tuple(
                assumeNotNull(toInt64(acc.1)) + if(
                    acc.2 = 1,
                    greatest(
                        assumeNotNull(toInt64(x.1)) - assumeNotNull(toInt64(acc.3)),
                        toInt64(0)
                    ),
                    toInt64(0)
                ),
                toUInt8(x.2),
                assumeNotNull(toInt64(x.1))
            ),
            arrayPushBack(
                arraySort(
                    x -> x.1,
                    groupArray(tuple(assumeNotNull(toInt64(c.ts)), toUInt8(c.state)))
                ),
                tuple(toInt64(assumeNotNull(any(f.end_time_ms))), toUInt8(0))
            ),
            tuple(toInt64(0), toUInt8(0), toInt64(assumeNotNull(any(f.start_time_ms))))
        ).1 AS uptime_ms
    FROM (
        SELECT * FROM buff_changes
        UNION ALL
        SELECT * FROM pull_seeds
    ) AS c
    INNER JOIN {{ ref('wcl_fights') }} AS f
        ON c.report_code = f.report_code AND c.fight_id = f.fight_id
    GROUP BY c.report_code, c.fight_id, c.player_actor_id, c.ability_game_id
),
-- Potions / healthstones : comptage de casts en combat
cast_counts AS (
    SELECT
        e.report_code AS report_code,
        e.fight_id AS fight_id,
        e.source_id AS player_actor_id,
        e.ability_game_id AS ability_game_id,
        any(ca.ability_name) AS ability_name,
        any(ca.consumable_type) AS consumable_type,
        toUInt8(0) AS present_at_pull,
        CAST(NULL AS Nullable(Int64)) AS uptime_ms,
        count() AS casts
    FROM {{ ref('wcl_events') }} AS e
    INNER JOIN consumable_abilities AS ca
        ON e.report_code = ca.report_code AND e.ability_game_id = ca.ability_game_id
    WHERE e.event_type = 'cast'
      AND ca.consumable_type IN ('potion', 'healthstone')
    GROUP BY e.report_code, e.fight_id, e.source_id, e.ability_game_id
),
unioned AS (
    SELECT
        report_code, fight_id, player_actor_id, ability_game_id,
        ability_name, consumable_type, present_at_pull,
        CAST(uptime_ms AS Nullable(Int64)) AS uptime_ms,
        CAST(NULL AS Nullable(Int64)) AS casts
    FROM buff_uptime
    UNION ALL
    SELECT
        report_code, fight_id, player_actor_id, ability_game_id,
        ability_name, consumable_type, present_at_pull,
        uptime_ms,
        CAST(casts AS Nullable(Int64)) AS casts
    FROM cast_counts
)
SELECT
    r.report_start_at,
    toDate(r.report_start_at) AS report_date,
    u.report_code,
    r.guild_name AS report_guild_name,
    r.zone_name AS raid_or_dungeon,
    f.fight_id,
    f.fight_name AS boss_name,
    f.is_boss,
    f.is_kill,
    {{ wcl_difficulty_label('f.difficulty', 'f.keystone_level') }} AS difficulty_label,
    f.keystone_level,
    f.duration_sec AS fight_duration_sec,
    a.resolved_player_name AS player_name,
    a.player_guid,
    a.resolved_class AS class_name,
    coalesce(gf.is_guild_member, 0) AS is_guild_member,
    u.consumable_type,
    u.ability_name AS consumable_name,
    toUInt8(u.present_at_pull) AS present_at_pull,
    round(u.uptime_ms / 1000.0, 1) AS uptime_sec,
    round(100.0 * u.uptime_ms / nullIf(f.duration_ms, 0), 1) AS uptime_pct,
    u.casts,
    now() AS _gold_loaded_at
FROM unioned AS u
INNER JOIN {{ ref('wcl_fights') }} AS f
    ON u.report_code = f.report_code AND u.fight_id = f.fight_id
INNER JOIN {{ ref('wcl_reports') }} AS r
    ON u.report_code = r.report_code
INNER JOIN {{ ref('wcl_actors') }} AS a
    ON u.report_code = a.report_code AND u.player_actor_id = a.actor_id
LEFT JOIN guild_flags AS gf
    ON u.report_code = gf.report_code AND a.resolved_player_name = gf.player_name
WHERE a.resolved_actor_type = 'Player'
  AND f.duration_sec > 0

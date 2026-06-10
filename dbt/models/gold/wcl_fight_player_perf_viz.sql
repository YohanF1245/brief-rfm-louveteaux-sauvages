{{ config(
    materialized='table',
    schema='gold',
    tags=['warcraftlogs', 'gold', 'power_bi']
) }}

/*
  KPI central : 1 ligne = joueur × fight × report.
  Couvre : évolution DPS/HPS/DTPS, morts, interrupts, dispels, potions,
  ilvl & spé au pull, filtre membres de guilde, kill/wipe, difficulté.

  Conventions :
  - dégâts/soins attribués au JOUEUR RÉSOLU (pets remontés au propriétaire)
  - dps/hps/dtps = somme / durée du fight (temps total, pas "temps actif")
  - healing inclut les absorptions consommées (event ``absorbed``, crédité
    au lanceur du bouclier — approximation alignée WCL)
  - présence = union des joueurs vus en dégâts/soins/dégâts subis/combatantinfo
    (un healer sans dégâts reste présent)
*/
WITH player_dim AS (
    SELECT
        report_code,
        resolved_player_name AS player_name,
        any(player_guid) AS player_guid,
        any(resolved_class) AS class_name
    FROM {{ ref('wcl_actors') }}
    WHERE resolved_actor_type = 'Player'
    GROUP BY report_code, player_name
),
player_damage AS (
    SELECT
        e.report_code AS report_code,
        e.fight_id AS fight_id,
        a.resolved_player_name AS player_name,
        sum(coalesce(e.amount, 0)) AS damage_done,
        countIf(e.hit_type = 2) AS crit_hits,
        count() AS damage_events
    FROM {{ ref('wcl_events') }} AS e
    INNER JOIN {{ ref('wcl_actors') }} AS a
        ON e.report_code = a.report_code AND e.source_id = a.actor_id
    WHERE e.event_type = 'damage'
      AND a.resolved_actor_type = 'Player'
    GROUP BY e.report_code, e.fight_id, player_name
),
player_damage_taken AS (
    SELECT
        e.report_code AS report_code,
        e.fight_id AS fight_id,
        a.resolved_player_name AS player_name,
        sum(coalesce(e.amount, 0)) AS damage_taken,
        sum(coalesce(e.absorbed, 0)) AS damage_absorbed,
        count() AS hits_taken
    FROM {{ ref('wcl_events') }} AS e
    INNER JOIN {{ ref('wcl_actors') }} AS a
        ON e.report_code = a.report_code AND e.target_id = a.actor_id
    WHERE e.event_type = 'damage'
      AND a.resolved_actor_type = 'Player'
    GROUP BY e.report_code, e.fight_id, player_name
),
player_healing AS (
    SELECT
        e.report_code AS report_code,
        e.fight_id AS fight_id,
        a.resolved_player_name AS player_name,
        sum(coalesce(e.amount, 0)) AS healing_done,
        sum(coalesce(e.overheal, 0)) AS overheal
    FROM {{ ref('wcl_events') }} AS e
    INNER JOIN {{ ref('wcl_actors') }} AS a
        ON e.report_code = a.report_code AND e.source_id = a.actor_id
    WHERE e.event_type IN ('heal', 'absorbed')
      AND a.resolved_actor_type = 'Player'
    GROUP BY e.report_code, e.fight_id, player_name
),
player_counters AS (
    -- Alias explicites : avec 2+ JOINs dans un CTE, ClickHouse garde les noms
    -- qualifiés (e.report_code) en sortie → le JOIN aval ne résout plus la colonne
    SELECT
        e.report_code AS report_code,
        e.fight_id AS fight_id,
        a.resolved_player_name AS player_name,
        countIf(e.event_type = 'interrupt') AS interrupts,
        countIf(e.event_type = 'dispel') AS dispels,
        countIf(e.event_type = 'cast' AND {{ wcl_consumable_type('ab.name') }} = 'potion') AS potion_casts,
        countIf(e.event_type = 'cast' AND {{ wcl_consumable_type('ab.name') }} = 'healthstone') AS healthstone_casts
    FROM {{ ref('wcl_events') }} AS e
    INNER JOIN {{ ref('wcl_actors') }} AS a
        ON e.report_code = a.report_code AND e.source_id = a.actor_id
    LEFT JOIN {{ ref('wcl_abilities') }} AS ab
        ON e.report_code = ab.report_code AND e.ability_game_id = ab.ability_game_id
    WHERE e.event_type IN ('interrupt', 'dispel', 'cast')
      AND a.resolved_actor_type = 'Player'
    GROUP BY e.report_code, e.fight_id, player_name
),
player_deaths AS (
    SELECT
        e.report_code AS report_code,
        e.fight_id AS fight_id,
        a.resolved_player_name AS player_name,
        count() AS deaths
    FROM {{ ref('wcl_events') }} AS e
    INNER JOIN {{ ref('wcl_actors') }} AS a
        ON e.report_code = a.report_code AND e.target_id = a.actor_id
    WHERE e.event_type = 'death'
      AND a.resolved_actor_type = 'Player'
    GROUP BY e.report_code, e.fight_id, player_name
),
pull_info AS (
    SELECT
        pcom.report_code AS report_code,
        pcom.fight_id AS fight_id,
        a.resolved_player_name AS player_name,
        any(pcom.spec_id) AS spec_id,
        any(pcom.avg_item_level) AS avg_item_level
    FROM {{ ref('wcl_pull_combatants') }} AS pcom
    INNER JOIN {{ ref('wcl_actors') }} AS a
        ON pcom.report_code = a.report_code AND pcom.player_actor_id = a.actor_id
    GROUP BY pcom.report_code, pcom.fight_id, a.resolved_player_name
),
{{ wcl_guild_flags_cte() }},
fight_players AS (
    SELECT DISTINCT report_code, fight_id, player_name
    FROM (
        SELECT report_code, fight_id, player_name FROM player_damage
        UNION ALL
        SELECT report_code, fight_id, player_name FROM player_damage_taken
        UNION ALL
        SELECT report_code, fight_id, player_name FROM player_healing
        UNION ALL
        SELECT report_code, fight_id, player_name FROM pull_info
    )
)
SELECT
    r.report_start_at,
    toDate(r.report_start_at) AS report_date,
    addMilliseconds(r.report_start_at, f.start_time_ms) AS fight_start_at,
    r.report_code,
    r.guild_name AS report_guild_name,
    r.title AS report_title,
    r.zone_name AS raid_or_dungeon,
    f.fight_id,
    f.fight_name AS boss_name,
    f.is_boss,
    f.is_kill,
    f.difficulty,
    {{ wcl_difficulty_label('f.difficulty', 'f.keystone_level') }} AS difficulty_label,
    f.keystone_level,
    f.boss_percentage,
    f.duration_sec AS fight_duration_sec,
    if(coalesce(f.keystone_level, 0) > 0, 'mythic_plus', if(f.is_boss = 1, 'raid', 'trash')) AS content_type,
    fp.player_name,
    dim.player_guid,
    dim.class_name,
    pi.spec_id,
    pi.avg_item_level,
    gf.role,
    coalesce(gf.is_guild_member, 0) AS is_guild_member,
    gf.player_guild_name,
    if(lower(coalesce(r.guild_name, '')) LIKE '%nightmares asylum%', 1, 0) AS is_nightmares_asylum_report,
    coalesce(d.damage_done, 0) AS damage_done,
    round(coalesce(d.damage_done, 0) / nullIf(f.duration_sec, 0), 0) AS dps,
    coalesce(d.crit_hits, 0) AS crit_hits,
    coalesce(d.damage_events, 0) AS damage_events,
    coalesce(dt.damage_taken, 0) AS damage_taken,
    round(coalesce(dt.damage_taken, 0) / nullIf(f.duration_sec, 0), 0) AS dtps,
    coalesce(dt.damage_absorbed, 0) AS damage_absorbed,
    coalesce(dt.hits_taken, 0) AS hits_taken,
    coalesce(h.healing_done, 0) AS healing_done,
    round(coalesce(h.healing_done, 0) / nullIf(f.duration_sec, 0), 0) AS hps,
    coalesce(h.overheal, 0) AS overheal,
    coalesce(pd.deaths, 0) AS deaths,
    coalesce(cnt.interrupts, 0) AS interrupts,
    coalesce(cnt.dispels, 0) AS dispels,
    coalesce(cnt.potion_casts, 0) AS potion_casts,
    coalesce(cnt.healthstone_casts, 0) AS healthstone_casts,
    now() AS _gold_loaded_at
FROM fight_players AS fp
INNER JOIN {{ ref('wcl_fights') }} AS f
    ON fp.report_code = f.report_code AND fp.fight_id = f.fight_id
INNER JOIN {{ ref('wcl_reports') }} AS r
    ON fp.report_code = r.report_code
LEFT JOIN player_dim AS dim
    ON fp.report_code = dim.report_code AND fp.player_name = dim.player_name
LEFT JOIN player_damage AS d
    ON fp.report_code = d.report_code AND fp.fight_id = d.fight_id AND fp.player_name = d.player_name
LEFT JOIN player_damage_taken AS dt
    ON fp.report_code = dt.report_code AND fp.fight_id = dt.fight_id AND fp.player_name = dt.player_name
LEFT JOIN player_healing AS h
    ON fp.report_code = h.report_code AND fp.fight_id = h.fight_id AND fp.player_name = h.player_name
LEFT JOIN player_counters AS cnt
    ON fp.report_code = cnt.report_code AND fp.fight_id = cnt.fight_id AND fp.player_name = cnt.player_name
LEFT JOIN player_deaths AS pd
    ON fp.report_code = pd.report_code AND fp.fight_id = pd.fight_id AND fp.player_name = pd.player_name
LEFT JOIN pull_info AS pi
    ON fp.report_code = pi.report_code AND fp.fight_id = pi.fight_id AND fp.player_name = pi.player_name
LEFT JOIN guild_flags AS gf
    ON fp.report_code = gf.report_code AND fp.player_name = gf.player_name
WHERE f.duration_sec > 0

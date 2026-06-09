{{ config(
    materialized='table',
    schema='gold',
    tags=['warcraftlogs', 'gold', 'power_bi']
) }}

/*
  Gold consommables raid : uptime food / flacon / huile + potions en combat boss.
  Guilde Nightmares Asylum (filtrable via is_nightmares_asylum).
  Source : silver.wcl_player_fight_metrics (metric buffs + casts).
*/
WITH player_lookup AS (
    SELECT
        report_code,
        fight_id,
        player_id,
        any(player_name) AS player_name
    FROM (
        SELECT report_code, fight_id, player_id, player_name
        FROM {{ ref('wcl_player_fight_metrics') }}
        WHERE metric IN ('dps', 'summary', 'hps')
          AND player_id IS NOT NULL
          AND player_name != ''
    )
    GROUP BY report_code, fight_id, player_id
),
raw_consumables AS (
    SELECT
        m.report_code,
        m.fight_id,
        coalesce(pl.player_name, m.player_name) AS player_name,
        m.player_id,
        m.class_name,
        m.spec_name,
        m.metric,
        m.entry_name,
        m.total_amount,
        m.active_time_ms,
        multiIf(
            m.metric = 'casts'
                AND match(lower(m.entry_name), 'potion'),
            'potion',
            m.metric = 'buffs'
                AND match(
                    lower(m.entry_name),
                    'well fed|feast|food|delicious|refrigerated|culinary|biscuit|skewer|steak|chops|fillet|sugar|bread|dessert|pierogi|sausage|omelet|omelette|goulash|stew|soup|pudding|cookie|cake|pie|tart|roast|ribs|meat|fish|banquet|bountiful|fated'
                ),
            'food',
            m.metric = 'buffs' AND match(lower(m.entry_name), 'flask'),
            'flask',
            m.metric = 'buffs'
                AND match(
                    lower(m.entry_name),
                    'oil|whetstone|sharpening|weightstone|mana oil|ironclaw'
                ),
            'oil',
            ''
        ) AS consumable_type
    FROM {{ ref('wcl_player_fight_metrics') }} AS m
    LEFT JOIN player_lookup AS pl
        ON m.report_code = pl.report_code
        AND m.fight_id = pl.fight_id
        AND m.player_id = pl.player_id
    WHERE m.metric IN ('buffs', 'casts')
      AND m.entry_name != ''
),
classified AS (
    SELECT
        c.*,
        if(
            c.consumable_type = 'potion',
            toFloat64(c.total_amount),
            NULL
        ) AS potion_uses,
        if(
            c.consumable_type != 'potion',
            greatest(
                toFloat64(coalesce(c.active_time_ms, 0)),
                toFloat64(coalesce(c.total_amount, 0))
            ) / 1000.0,
            NULL
        ) AS uptime_sec
    FROM raw_consumables AS c
    WHERE c.consumable_type != ''
)
SELECT
    r.report_start_at,
    toDate(r.report_start_at) AS report_date,
    r.guild_name,
    r.zone_name AS raid_or_dungeon,
    f.fight_name AS boss_name,
    f.difficulty,
    multiIf(
        f.keystone_level > 0, 'Mythic+',
        f.difficulty = 1, 'LFR',
        f.difficulty = 3, 'Normal',
        f.difficulty = 4, 'Heroic',
        f.difficulty = 5, 'Mythic',
        f.difficulty IS NOT NULL, concat('diff_', toString(f.difficulty)),
        'Unknown'
    ) AS difficulty_label,
    f.keystone_level,
    toFloat64(f.duration_ms) / 1000.0 AS fight_duration_sec,
    c.player_name,
    c.class_name,
    c.spec_name,
    c.consumable_type,
    c.entry_name AS consumable_name,
    c.uptime_sec,
    if(
        f.duration_ms > 0 AND c.uptime_sec IS NOT NULL,
        round(100.0 * c.uptime_sec / (toFloat64(f.duration_ms) / 1000.0), 1),
        NULL
    ) AS uptime_pct,
    toInt64OrNull(toString(c.potion_uses)) AS potion_uses,
    if(lower(coalesce(r.guild_name, '')) LIKE '%nightmares asylum%', 1, 0) AS is_nightmares_asylum,
    c.report_code,
    c.fight_id,
    now() AS _gold_loaded_at
FROM classified AS c
INNER JOIN {{ ref('wcl_fights') }} AS f
    ON c.report_code = f.report_code AND c.fight_id = f.fight_id
INNER JOIN {{ ref('wcl_reports') }} AS r
    ON c.report_code = r.report_code
WHERE f.is_boss = 1
  AND coalesce(f.keystone_level, 0) = 0

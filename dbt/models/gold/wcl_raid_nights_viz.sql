{{ config(
    materialized='table',
    schema='gold',
    tags=['warcraftlogs', 'gold', 'power_bi']
) }}

/*
  Synthèse par soirée (1 ligne = 1 report) : volume de pulls, kills/wipes,
  temps passé en combat, effectif présent, meilleure progression par soirée.
  KPI : assiduité de la guilde, rythme de progression, efficacité des soirées.
*/
WITH {{ wcl_guild_flags_cte() }},
fight_stats AS (
    SELECT
        report_code,
        count() AS total_pulls,
        countIf(is_boss = 1) AS boss_pulls,
        countIf(is_boss = 1 AND is_kill = 1) AS boss_kills,
        countIf(is_boss = 1 AND is_kill = 0) AS boss_wipes,
        uniqExactIf(fight_name, is_boss = 1) AS distinct_bosses,
        countIf(coalesce(keystone_level, 0) > 0) AS mythic_plus_runs,
        round(sum(duration_sec) / 60.0, 1) AS combat_minutes,
        round(avgIf(duration_sec, is_boss = 1), 0) AS avg_boss_pull_sec,
        minIf(boss_percentage, is_boss = 1 AND is_kill = 0) AS best_wipe_boss_pct
    FROM {{ ref('wcl_fights') }}
    WHERE duration_sec > 0
    GROUP BY report_code
),
roster_stats AS (
    SELECT
        report_code,
        uniqExact(player_name) AS players_total,
        uniqExactIf(player_name, is_guild_member = 1) AS players_guild
    FROM guild_flags
    GROUP BY report_code
),
ilvl_stats AS (
    SELECT
        report_code,
        round(avg(max_item_level), 1) AS avg_max_item_level
    FROM {{ ref('wcl_player_details') }}
    GROUP BY report_code
)
SELECT
    r.report_start_at,
    toDate(r.report_start_at) AS report_date,
    r.report_code,
    r.title AS report_title,
    r.guild_name AS report_guild_name,
    if(lower(coalesce(r.guild_name, '')) LIKE '%nightmares asylum%', 1, 0) AS is_nightmares_asylum_report,
    r.zone_name AS raid_or_dungeon,
    round((toUnixTimestamp64Milli(r.report_end_at) - toUnixTimestamp64Milli(r.report_start_at)) / 3600000.0, 2) AS report_hours,
    fs.total_pulls,
    fs.boss_pulls,
    fs.boss_kills,
    fs.boss_wipes,
    fs.distinct_bosses,
    fs.mythic_plus_runs,
    fs.combat_minutes,
    fs.avg_boss_pull_sec,
    fs.best_wipe_boss_pct,
    round(100.0 * fs.boss_kills / nullIf(fs.boss_pulls, 0), 1) AS boss_kill_rate_pct,
    rs.players_total,
    rs.players_guild,
    il.avg_max_item_level,
    now() AS _gold_loaded_at
FROM {{ ref('wcl_reports') }} AS r
INNER JOIN fight_stats AS fs ON r.report_code = fs.report_code
LEFT JOIN roster_stats AS rs ON r.report_code = rs.report_code
LEFT JOIN ilvl_stats AS il ON r.report_code = il.report_code

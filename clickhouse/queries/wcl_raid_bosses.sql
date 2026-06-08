-- Raid — boss, kills, wipes
-- Prérequis : 00_setup_views.sql

SELECT
    f.fight_name, f.encounter_id, count() AS attempts, countIf(f.kill) AS kills,
    round(countIf(f.kill) / count() * 100, 1) AS kill_pct,
    round(avg(f.duration_ms) / 1000, 0) AS avg_duration_sec
FROM v_wcl_fights AS f
WHERE f.is_boss = true
GROUP BY f.fight_name, f.encounter_id
ORDER BY attempts DESC;

SELECT
    toDateTime64(r.start_time_ms / 1000, 3, 'UTC') AS report_date,
    r.title, f.fight_name, if(f.kill, 'kill', 'wipe') AS outcome,
    round(f.duration_ms / 1000, 0) AS duration_sec, round(f.boss_percentage, 1) AS boss_pct
FROM v_wcl_fights AS f
INNER JOIN v_wcl_guild_reports AS r ON f.report_code = r.report_code
WHERE f.is_boss = true
ORDER BY r.start_time_ms DESC, f.fight_id LIMIT 50;

SELECT
    toDateTime64(r.start_time_ms / 1000, 3, 'UTC') AS report_date,
    s.player_name, s.class_name, round(s.rate_per_sec, 0) AS dps, if(f.kill, 'kill', 'wipe') AS outcome
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN v_wcl_guild_reports AS r ON s.report_code = r.report_code
WHERE s.metric = 'dps' AND f.is_boss = true AND f.fight_name = 'Boss Name Here'
ORDER BY dps DESC LIMIT 20;

SELECT
    toDateTime64(r.start_time_ms / 1000, 3, 'UTC') AS report_date,
    r.zone_name, r.title, uniqExact(s.player_name) AS players
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_guild_reports AS r ON s.report_code = r.report_code
WHERE s.metric = 'summary'
GROUP BY report_date, r.zone_name, r.title, r.report_code
ORDER BY report_date DESC;

SELECT r.zone_name, f.fight_name, s.player_name, s.total_amount AS interrupts
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN v_wcl_guild_reports AS r ON s.report_code = r.report_code
WHERE s.metric = 'interrupts' AND f.is_boss = true
ORDER BY interrupts DESC LIMIT 30;

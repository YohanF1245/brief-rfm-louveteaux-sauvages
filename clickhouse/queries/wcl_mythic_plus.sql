-- Mythic+
-- Prérequis : 00_setup_views.sql

SELECT
    toDateTime64(r.start_time_ms / 1000, 3, 'UTC') AS report_date,
    r.title, r.zone_name, f.fight_name, f.keystone_level,
    round(f.duration_ms / 1000, 0) AS fight_sec, if(f.kill, 'done', 'wipe') AS outcome
FROM v_wcl_fights AS f
INNER JOIN v_wcl_guild_reports AS r ON f.report_code = r.report_code
WHERE f.keystone_level > 0
ORDER BY report_date DESC, f.keystone_level DESC;

SELECT f.keystone_level, count() AS fights, countIf(f.kill) AS completed
FROM v_wcl_fights AS f
WHERE f.keystone_level > 0
GROUP BY f.keystone_level ORDER BY f.keystone_level DESC;

SELECT
    s.player_name, s.class_name, f.keystone_level,
    round(avg(s.rate_per_sec), 0) AS avg_dps, count() AS fights
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
WHERE s.metric = 'dps' AND f.keystone_level >= 10
GROUP BY s.player_name, s.class_name, f.keystone_level
HAVING fights >= 2
ORDER BY f.keystone_level DESC, avg_dps DESC LIMIT 40;

SELECT
    toDateTime64(r.start_time_ms / 1000, 3, 'UTC') AS report_date,
    f.fight_name, s.player_name, round(s.rate_per_sec, 0) AS dps
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN v_wcl_guild_reports AS r ON s.report_code = r.report_code
WHERE s.metric = 'dps' AND f.keystone_level = 12
ORDER BY dps DESC LIMIT 25;

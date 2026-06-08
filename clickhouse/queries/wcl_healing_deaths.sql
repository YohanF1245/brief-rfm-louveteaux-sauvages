-- Healing & deaths
-- Prérequis : 00_setup_views.sql

SELECT
    toDateTime64(r.start_time_ms / 1000, 3, 'UTC') AS report_date,
    f.fight_name, s.player_name, round(s.rate_per_sec, 0) AS hps
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN v_wcl_guild_reports AS r ON s.report_code = r.report_code
WHERE s.metric = 'hps' AND f.is_boss = true
ORDER BY hps DESC LIMIT 40;

SELECT f.fight_name, s.player_name, s.total_amount AS dispels
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
WHERE s.metric = 'dispels' AND f.is_boss = true AND s.total_amount > 0
ORDER BY dispels DESC LIMIT 30;

SELECT s.player_name, s.class_name, sum(s.total_amount) AS total_deaths
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
WHERE s.metric = 'deaths' AND f.is_boss = true AND s.total_amount > 0
GROUP BY s.player_name, s.class_name ORDER BY total_deaths DESC LIMIT 25;

SELECT r.title, f.fight_name, sum(s.total_amount) AS raid_deaths
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN v_wcl_guild_reports AS r ON s.report_code = r.report_code
WHERE s.metric = 'deaths' AND f.is_boss = true
GROUP BY r.title, f.fight_name, f.report_code, f.fight_id
ORDER BY raid_deaths DESC LIMIT 20;

SELECT f.fight_name, s.player_name, round(s.rate_per_sec, 0) AS dtps
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
WHERE s.metric = 'dtps' AND f.is_boss = true
ORDER BY dtps DESC LIMIT 30;

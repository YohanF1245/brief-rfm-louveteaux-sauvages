-- Roster & activité
-- Prérequis : 00_setup_views.sql

SELECT
    s.player_name, s.class_name, uniqExact(s.report_code) AS reports,
    max(toDateTime64(r.start_time_ms / 1000, 3, 'UTC')) AS last_seen
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_guild_reports AS r ON s.report_code = r.report_code
WHERE s.metric = 'summary' AND r.log_source = 'guild'
GROUP BY s.player_name, s.class_name ORDER BY reports DESC;

SELECT s.class_name, s.spec_name, uniqExact(s.player_name) AS players
FROM v_wcl_fight_player_stats AS s
WHERE s.metric = 'dps' AND s.class_name IS NOT NULL
GROUP BY s.class_name, s.spec_name ORDER BY players DESC;

SELECT
    toMonday(toDateTime64(start_time_ms / 1000, 3, 'UTC')) AS week,
    log_source, count() AS reports
FROM v_wcl_guild_reports
GROUP BY week, log_source ORDER BY week DESC;

SELECT zone_name, log_source, count() AS reports
FROM v_wcl_guild_reports
WHERE zone_name IS NOT NULL
GROUP BY zone_name, log_source ORDER BY reports DESC;

SELECT log_source, count() AS reports
FROM v_wcl_guild_reports GROUP BY log_source;

SELECT fight_id, fight_name, is_boss, kill, round(duration_ms / 1000, 1) AS duration_sec
FROM v_wcl_fights
WHERE report_code = '6Btgcf9mQZbFJzap'
ORDER BY fight_id;

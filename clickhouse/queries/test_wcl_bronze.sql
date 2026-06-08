-- Validation bronze — Prérequis : 00_setup_views.sql

SELECT status, count() AS reports FROM v_wcl_ingestion_state GROUP BY status ORDER BY reports DESC;

SELECT report_code, status, title, ingestion_attempts
FROM v_wcl_ingestion_state
WHERE status IN ('pending', 'error')
ORDER BY start_time_ms DESC LIMIT 25;

SELECT log_source, count() AS reports FROM v_wcl_guild_reports GROUP BY log_source;

SELECT report_code, owner_name, title, zone_name
FROM v_wcl_guild_reports ORDER BY start_time_ms DESC LIMIT 25;

SELECT report_code, count() AS fights, countIf(is_boss) AS boss_fights
FROM v_wcl_fights GROUP BY report_code ORDER BY fights DESC LIMIT 20;

SELECT metric, count() AS rows FROM v_wcl_fight_player_stats GROUP BY metric ORDER BY rows DESC;

SELECT r.zone_name, f.fight_name, s.player_name, round(s.rate_per_sec, 0) AS dps
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN v_wcl_guild_reports AS r ON s.report_code = r.report_code
WHERE s.metric = 'dps' AND f.is_boss = true
ORDER BY dps DESC LIMIT 30;

-- Deep dive — remplacer '6Btgcf9mQZbFJzap'
-- Prérequis : 00_setup_views.sql

SELECT report_code, log_source, owner_name, title, zone_name,
    toDateTime64(start_time_ms / 1000, 3, 'UTC') AS started
FROM v_wcl_guild_reports WHERE report_code = '6Btgcf9mQZbFJzap';

SELECT fight_id, fight_name, is_boss, kill, keystone_level,
    round(duration_ms / 1000, 1) AS duration_sec
FROM v_wcl_fights WHERE report_code = '6Btgcf9mQZbFJzap' ORDER BY fight_id;

SELECT f.fight_name, s.player_name, round(s.rate_per_sec, 0) AS dps
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
WHERE s.report_code = '6Btgcf9mQZbFJzap' AND s.metric = 'dps' AND f.is_boss = true
ORDER BY f.fight_id, dps DESC;

SELECT metric, count() AS rows FROM v_wcl_fight_player_stats
WHERE report_code = '6Btgcf9mQZbFJzap' GROUP BY metric ORDER BY metric;

SELECT fight_id, data_type, length(raw_json) AS json_bytes FROM v_wcl_fight_tables_raw
WHERE report_code = '6Btgcf9mQZbFJzap' ORDER BY fight_id, data_type;

SELECT status, ingestion_attempts, last_error FROM v_wcl_ingestion_state
WHERE report_code = '6Btgcf9mQZbFJzap';

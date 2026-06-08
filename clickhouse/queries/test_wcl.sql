-- Smoke test — Prérequis : 00_setup_views.sql

SELECT version() AS ch_version, now() AS server_time;

SELECT table_name, n FROM (
    SELECT 'guild_reports' AS table_name, count() AS n FROM v_wcl_guild_reports
    UNION ALL SELECT 'ingestion_state', count() FROM v_wcl_ingestion_state
    UNION ALL SELECT 'fights', count() FROM v_wcl_fights
    UNION ALL SELECT 'fight_player_stats', count() FROM v_wcl_fight_player_stats
    UNION ALL SELECT 'fight_tables_raw', count() FROM v_wcl_fight_tables_raw
    UNION ALL SELECT 'reports_raw', count() FROM v_wcl_reports_raw
) ORDER BY n DESC;

SELECT log_source, owner_name, title,
    toDateTime64(start_time_ms / 1000, 3, 'UTC') AS report_date
FROM v_wcl_guild_reports ORDER BY start_time_ms DESC LIMIT 15;

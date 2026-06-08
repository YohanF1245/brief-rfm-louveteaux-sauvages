-- Top DPS boss
-- Prérequis : 00_setup_views.sql

SELECT
    toDateTime64(r.start_time_ms / 1000, 3, 'UTC') AS report_date,
    r.log_source, r.zone_name, f.fight_name,
    if(f.kill, 'kill', 'wipe') AS outcome,
    s.player_name, s.class_name, s.spec_name,
    JSONExtractInt(s.extra, 'itemLevel') AS item_level,
    round(s.rate_per_sec, 0) AS dps
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN v_wcl_guild_reports AS r ON s.report_code = r.report_code
WHERE s.metric = 'dps' AND f.is_boss = true
-- AND lower(s.player_name) LIKE '%kimahri%'
ORDER BY dps DESC LIMIT 50;

-- Kimahri — owner_user_id WCL = 105636
-- Prérequis : 00_setup_views.sql (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD)

SELECT report_code, title, zone_name,
    toDateTime64(start_time_ms / 1000, 3, 'UTC') AS report_date
FROM v_wcl_guild_reports
WHERE owner_user_id = 105636 OR lower(owner_name) LIKE '%kimahri%'
ORDER BY start_time_ms DESC;

SELECT
    toDateTime64(r.start_time_ms / 1000, 3, 'UTC') AS report_date,
    r.zone_name, f.fight_name, if(f.kill, 'kill', 'wipe') AS outcome,
    round(f.duration_ms / 1000, 0) AS fight_sec, s.spec_name,
    round(s.rate_per_sec, 0) AS dps, JSONExtractInt(s.extra, 'itemLevel') AS ilvl
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN v_wcl_guild_reports AS r ON s.report_code = r.report_code
WHERE s.metric = 'dps' AND f.is_boss = true AND lower(s.player_name) LIKE '%kimahri%'
ORDER BY report_date DESC, dps DESC;

SELECT
    toDateTime64(r.start_time_ms / 1000, 3, 'UTC') AS report_date,
    f.fight_name, round(s.rate_per_sec, 0) AS hps
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN v_wcl_guild_reports AS r ON s.report_code = r.report_code
WHERE s.metric = 'hps' AND lower(s.player_name) LIKE '%kimahri%'
ORDER BY report_date DESC LIMIT 50;

SELECT r.title, f.fight_name, s.total_amount AS deaths, if(f.kill, 'kill', 'wipe') AS outcome
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN v_wcl_guild_reports AS r ON s.report_code = r.report_code
WHERE s.metric = 'deaths' AND lower(s.player_name) LIKE '%kimahri%' AND s.total_amount > 0
ORDER BY deaths DESC;

SELECT
    f.fight_name, max(round(s.rate_per_sec, 0)) AS best_dps,
    argMax(r.zone_name, s.rate_per_sec) AS zone_name,
    argMax(toDateTime64(r.start_time_ms / 1000, 3, 'UTC'), s.rate_per_sec) AS best_run_date
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN v_wcl_guild_reports AS r ON s.report_code = r.report_code
WHERE s.metric = 'dps' AND f.is_boss = true AND lower(s.player_name) LIKE '%kimahri%'
GROUP BY f.fight_name ORDER BY best_dps DESC;

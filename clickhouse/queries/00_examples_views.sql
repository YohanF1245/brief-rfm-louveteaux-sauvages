-- Exemples — après 00_setup_views.sql

SELECT status, count() AS n FROM v_wcl_ingestion_state GROUP BY status;

SELECT f.fight_name, s.player_name, round(s.rate_per_sec, 0) AS dps
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
WHERE s.metric = 'dps' AND f.is_boss = true
ORDER BY dps DESC LIMIT 20;

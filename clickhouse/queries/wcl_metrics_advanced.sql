-- Métriques avancées
-- Prérequis : 00_setup_views.sql

SELECT f.fight_name, s.player_name, JSONExtractString(s.extra, 'name') AS ability, s.total_amount AS casts
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
WHERE s.metric = 'casts' AND f.is_boss = true AND s.total_amount > 0
ORDER BY casts DESC LIMIT 40;

SELECT f.fight_name, s.player_name, round(s.rate_per_sec, 0) AS threat_per_sec
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
WHERE s.metric = 'threat' AND f.is_boss = true
ORDER BY threat_per_sec DESC LIMIT 25;

SELECT f.fight_name, s.player_name, round(s.rate_per_sec, 2) AS survivability_rate
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
WHERE s.metric = 'survivability' AND f.is_boss = true
ORDER BY survivability_rate DESC LIMIT 25;

SELECT s.player_name, JSONExtractString(s.extra, 'name') AS buff_name,
    round(s.total_amount / 1000, 0) AS uptime_sec
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
WHERE s.metric = 'buffs' AND f.is_boss = true AND s.total_amount > 0
ORDER BY uptime_sec DESC LIMIT 30;

SELECT metric, count() AS rows FROM v_wcl_fight_player_stats GROUP BY metric ORDER BY rows DESC;

SELECT s.class_name, round(avg(s.rate_per_sec), 0) AS avg_dps, count() AS samples
FROM v_wcl_fight_player_stats AS s
INNER JOIN v_wcl_fights AS f ON s.report_code = f.report_code AND s.fight_id = f.fight_id
WHERE s.metric = 'dps' AND f.is_boss = true AND s.class_name IS NOT NULL
GROUP BY s.class_name HAVING samples >= 5 ORDER BY avg_dps DESC;

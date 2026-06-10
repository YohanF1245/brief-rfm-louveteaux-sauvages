-- =============================================================================
-- Découverte bronze WCL "données brutes" — schéma : docs/wcl_bronze.md
-- Prérequis : 00_setup_views.sql (vues v_wcl_*)
-- Un SELECT à la fois dans Play.
-- =============================================================================

-- 1) Volumes par table bronze
SELECT 'guild_reports' AS t, count() AS n FROM v_wcl_guild_reports
UNION ALL SELECT 'fights', count() FROM v_wcl_fights
UNION ALL SELECT 'reports_raw', count() FROM v_wcl_reports_raw
UNION ALL SELECT 'master_info', count() FROM v_wcl_master_info
UNION ALL SELECT 'master_actors', count() FROM v_wcl_master_actors
UNION ALL SELECT 'master_abilities', count() FROM v_wcl_master_abilities
UNION ALL SELECT 'player_details', count() FROM v_wcl_player_details
UNION ALL SELECT 'events', count() FROM v_wcl_events
UNION ALL SELECT 'ingestion_state', count() FROM v_wcl_ingestion_state;

-- 2) Events par type (sanity check du dataType: All)
SELECT event_type, count() AS n
FROM v_wcl_events
GROUP BY event_type
ORDER BY n DESC;

-- 3) Events par report (volumétrie / pages)
SELECT report_code, count() AS events, max(page_index) + 1 AS pages
FROM v_wcl_events
GROUP BY report_code
ORDER BY events DESC
LIMIT 20;

-- 4) Acteurs par type (joueurs / pets / NPC)
SELECT actor_type, sub_type, count() AS n
FROM v_wcl_master_actors
GROUP BY actor_type, sub_type
ORDER BY n DESC;

-- 5) Résolution joueur d'un event (pets remontés au propriétaire)
SELECT
    e.event_type,
    coalesce(o.name, a.name) AS player_name,
    coalesce(o.actor_type, a.actor_type) AS resolved_type,
    count() AS n
FROM v_wcl_events AS e
INNER JOIN v_wcl_master_actors AS a
    ON e.report_code = a.report_code AND e.source_id = a.actor_id
LEFT JOIN v_wcl_master_actors AS o
    ON a.report_code = o.report_code AND a.pet_owner_id = o.actor_id
WHERE e.event_type = 'damage'
GROUP BY e.event_type, player_name, resolved_type
ORDER BY n DESC
LIMIT 30;

-- 6) Clés JSON présentes dans event_json, par type d'event
SELECT
    event_type,
    arrayJoin(JSONExtractKeys(event_json)) AS json_key,
    count() AS n
FROM v_wcl_events
GROUP BY event_type, json_key
ORDER BY event_type, n DESC;

-- 7) playerDetails : joueurs, ilvl, GUID (jointure roster)
SELECT
    pd.player_name,
    pd.role,
    pd.class_name,
    pd.max_item_level,
    pd.player_guid,
    gr.guild_name
FROM v_wcl_player_details AS pd
LEFT JOIN v_wcl_guild_roster AS gr
    ON pd.player_guid = gr.player_guid
ORDER BY pd.max_item_level DESC
LIMIT 30;

-- 8) Échantillon event brut complet
SELECT event_type, substring(event_json, 1, 800) AS sample
FROM v_wcl_events
WHERE event_type IN ('damage', 'applybuff', 'combatantinfo')
LIMIT 5;

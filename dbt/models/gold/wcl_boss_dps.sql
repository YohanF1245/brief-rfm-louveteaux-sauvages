{{ config(
    materialized='table',
    schema='gold',
    engine='MergeTree()',
    order_by='(report_code, fight_id, player_name)',
    tags=['warcraftlogs', 'gold', 'power_bi']
) }}

/*
  Gold WCL : DPS boss, grain joueur × fight × report.
  Play / Power BI : base ``gold`` → table ``wcl_boss_dps``.
*/
SELECT
    r.report_start_at,
    toDate(r.report_start_at) AS report_date,
    r.guild_name,
    r.zone_name,
    r.title AS report_title,
    f.fight_name,
    f.encounter_id,
    f.difficulty,
    f.raid_size,
    if(f.is_kill = 1, 'kill', 'wipe') AS outcome,
    f.duration_sec,
    f.keystone_level,
    s.player_name,
    s.class_name,
    s.spec_name,
    s.item_level,
    s.total_amount,
    round(s.rate_per_sec, 2) AS dps,
    s.player_id,
    s.report_code,
    s.fight_id,
    now() AS _gold_loaded_at
FROM {{ ref('wcl_player_fight_metrics') }} AS s
INNER JOIN {{ ref('wcl_fights') }} AS f
    ON s.report_code = f.report_code AND s.fight_id = f.fight_id
INNER JOIN {{ ref('wcl_reports') }} AS r
    ON s.report_code = r.report_code
WHERE s.metric = 'dps'
  AND f.is_boss = 1

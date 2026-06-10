{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

/*
  Snapshot joueur AU PULL (event ``combatantinfo``, 1 par joueur et par fight) :
  spec jouée et item level moyen (calculé sur ``gear[].itemLevel``).
  Source de vérité pour « quelle spé sur ce pull » et l'évolution d'ilvl.
*/
WITH raw AS (
    SELECT
        report_code,
        toInt32OrNull(toString(fight_id)) AS fight_id,
        toInt32OrNull(toString(source_id)) AS player_actor_id,
        toInt64(timestamp_ms) AS pull_timestamp_ms,
        JSONExtract(event_json, 'specID', 'Nullable(Int32)') AS spec_id,
        JSONExtract(event_json, 'faction', 'Nullable(Int32)') AS faction,
        -- ilvl > 1 : exclut les slots vides (0) et chemise/tabard (ilvl 1).
        -- assumeNotNull : event_json est Nullable via deltaLake(), et
        -- ClickHouse interdit Nullable(Array(...)) en sortie de JSONExtract.
        arrayFilter(
            x -> x > 1,
            JSONExtract(assumeNotNull(event_json), 'gear', 'Array(Tuple(itemLevel Int64))').itemLevel
        ) AS gear_ilvls,
        length(JSONExtractArrayRaw(assumeNotNull(event_json), 'auras')) AS auras_count,
        toDateTime64(fetched_at, 3, 'UTC') AS bronze_fetched_at
    FROM deltaLake(
        'http://minio:9000/lake/bronze/warcraftlogs/events',
        '{{ env_var("MINIO_ROOT_USER", "minioadmin") }}',
        '{{ env_var("MINIO_ROOT_PASSWORD", "minioadmin") }}'
    )
    WHERE event_type = 'combatantinfo'
)
SELECT
    report_code,
    fight_id,
    player_actor_id,
    pull_timestamp_ms,
    spec_id,
    faction,
    if(length(gear_ilvls) = 0, NULL, round(arrayAvg(gear_ilvls), 1)) AS avg_item_level,
    length(gear_ilvls) AS gear_count,
    auras_count,
    bronze_fetched_at,
    now() AS _silver_loaded_at
FROM raw

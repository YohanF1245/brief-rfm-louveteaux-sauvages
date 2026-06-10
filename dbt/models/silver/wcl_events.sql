{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver'],
    engine='MergeTree()',
    order_by='(report_code, fight_id, event_type, timestamp_ms)'
) }}

/*
  Fait central : events bruts typés (1 ligne = 1 event, hors ``combatantinfo``
  traité par ``wcl_pull_combatants`` / ``wcl_pull_auras``).

  Champs métier extraits du JSON ; le JSON intégral reste dans le bronze
  (``v_wcl_events.event_json``) pour tout besoin futur.

  Référentiel temps : ``timestamp_ms`` relatif au début du report
  (= ``wcl_fights.start_time_ms``).

  TODO perf : passer en incrémental (delete+insert par report_code) quand le
  rebuild complet depuis Delta deviendra trop long.
*/
SELECT
    report_code,
    -- 0 = hors fight (clé de tri MergeTree : non nullable obligatoire)
    coalesce(toInt32OrNull(toString(fight_id)), 0) AS fight_id,
    toInt64(timestamp_ms) AS timestamp_ms,
    event_type,
    toInt32OrNull(toString(source_id)) AS source_id,
    toInt32OrNull(toString(source_instance)) AS source_instance,
    toInt32OrNull(toString(target_id)) AS target_id,
    toInt32OrNull(toString(target_instance)) AS target_instance,
    toInt64OrNull(toString(ability_game_id)) AS ability_game_id,
    JSONExtract(event_json, 'amount', 'Nullable(Int64)') AS amount,
    JSONExtract(event_json, 'unmitigatedAmount', 'Nullable(Int64)') AS unmitigated_amount,
    JSONExtract(event_json, 'mitigated', 'Nullable(Int64)') AS mitigated,
    JSONExtract(event_json, 'absorbed', 'Nullable(Int64)') AS absorbed,
    JSONExtract(event_json, 'overkill', 'Nullable(Int64)') AS overkill,
    JSONExtract(event_json, 'overheal', 'Nullable(Int64)') AS overheal,
    JSONExtract(event_json, 'hitType', 'Nullable(Int32)') AS hit_type,
    JSONExtract(event_json, 'tick', 'Nullable(Bool)') AS is_tick,
    JSONExtract(event_json, 'melee', 'Nullable(Bool)') AS is_melee,
    JSONExtract(event_json, 'stack', 'Nullable(Int32)') AS stack,
    JSONExtract(event_json, 'extraAbilityGameID', 'Nullable(Int64)') AS extra_ability_game_id,
    JSONExtract(event_json, 'killerID', 'Nullable(Int32)') AS killer_id,
    toDateTime64(fetched_at, 3, 'UTC') AS bronze_fetched_at,
    now() AS _silver_loaded_at
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/events',
    '{{ env_var("MINIO_ROOT_USER", "minioadmin") }}',
    '{{ env_var("MINIO_ROOT_PASSWORD", "minioadmin") }}'
)
WHERE event_type != 'combatantinfo'

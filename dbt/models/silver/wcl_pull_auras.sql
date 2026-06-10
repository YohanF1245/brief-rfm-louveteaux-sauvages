{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

/*
  Buffs ACTIFS AU PULL (``combatantinfo.auras``), 1 ligne = 1 aura × joueur × fight.
  C'est l'amorce du calcul d'uptime des consommables : un flacon/food actif
  avant le pull n'émet PAS d'event ``applybuff`` pendant le fight — seul
  ce snapshot le révèle.

  ``aura_name`` vient du JSON quand présent, sinon jointure ``wcl_abilities``
  en aval (l'aura peut être absente de ``masterData.abilities`` si elle n'est
  jamais réappliquée pendant le report).
*/
SELECT
    report_code,
    toInt32OrNull(toString(fight_id)) AS fight_id,
    toInt32OrNull(toString(source_id)) AS player_actor_id,
    JSONExtract(aura, 'source', 'Nullable(Int32)') AS aura_source_id,
    JSONExtract(aura, 'ability', 'Nullable(Int64)') AS ability_game_id,
    JSONExtract(aura, 'stacks', 'Nullable(Int32)') AS stacks,
    nullIf(JSONExtractString(aura, 'name'), '') AS aura_name,
    nullIf(JSONExtractString(aura, 'icon'), '') AS aura_icon,
    toDateTime64(fetched_at, 3, 'UTC') AS bronze_fetched_at,
    now() AS _silver_loaded_at
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/events',
    '{{ env_var("MINIO_ROOT_USER", "minioadmin") }}',
    '{{ env_var("MINIO_ROOT_PASSWORD", "minioadmin") }}'
)
ARRAY JOIN JSONExtractArrayRaw(event_json, 'auras') AS aura
WHERE event_type = 'combatantinfo'

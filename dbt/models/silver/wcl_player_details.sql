{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

/*
  Joueurs d'un report (``playerDetails`` API) + flag membre de guilde.
  ``player_guid`` = GUID WoW persistant → jointure ``wcl_guild_roster``.
  Le détail talents/gear/stats reste en bronze (``combatant_info_json``).
*/
SELECT
    pd.report_code,
    pd.role,
    toInt32OrNull(toString(pd.player_id)) AS player_actor_id,
    toUInt64OrNull(toString(pd.player_guid)) AS player_guid,
    pd.player_name,
    pd.server,
    pd.region,
    pd.class_name,
    pd.icon,
    toInt32OrNull(toString(pd.min_item_level)) AS min_item_level,
    toInt32OrNull(toString(pd.max_item_level)) AS max_item_level,
    toInt32OrNull(toString(pd.potion_use)) AS potion_use,
    toInt32OrNull(toString(pd.healthstone_use)) AS healthstone_use,
    gr.character_name AS roster_character_name,
    gr.guild_name AS player_guild_name,
    gr.guild_rank,
    if(gr.player_guid IS NOT NULL, 1, 0) AS is_guild_member,
    toDateTime64(pd.fetched_at, 3, 'UTC') AS bronze_fetched_at,
    now() AS _silver_loaded_at
FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/player_details',
    '{{ env_var("MINIO_ROOT_USER", "minioadmin") }}',
    '{{ env_var("MINIO_ROOT_PASSWORD", "minioadmin") }}'
) AS pd
LEFT JOIN {{ ref('wcl_guild_roster') }} AS gr
    ON toUInt64OrNull(toString(pd.player_guid)) = gr.player_guid

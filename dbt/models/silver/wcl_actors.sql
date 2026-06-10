{{ config(
    materialized='table',
    schema='silver',
    tags=['warcraftlogs', 'silver']
) }}

/*
  Dimension acteurs : joueurs, pets, NPC d'un report (``masterData.actors``).

  Résolution pet → joueur faite ICI une fois pour toutes :
  - ``resolved_player_name``  : nom du propriétaire si pet, sinon nom de l'acteur
  - ``resolved_actor_type``   : type après résolution (un pet devient ``Player``
                                si son propriétaire est un joueur)
  - ``player_guid``           : GUID WoW persistant du joueur résolu
                                (= ``game_id`` des acteurs Player)
  - ``is_guild_member``       : jointure roster (GUID / id WCL / nom+serveur)
*/
WITH actors AS (
    SELECT
        report_code,
        toInt32(actor_id) AS actor_id,
        toInt64OrNull(toString(game_id)) AS game_id,
        name,
        actor_type,
        sub_type,
        toInt32OrNull(toString(pet_owner_id)) AS pet_owner_id,
        server,
        icon,
        toDateTime64(fetched_at, 3, 'UTC') AS bronze_fetched_at
    FROM deltaLake(
        'http://minio:9000/lake/bronze/warcraftlogs/master_actors',
        '{{ env_var("MINIO_ROOT_USER", "minioadmin") }}',
        '{{ env_var("MINIO_ROOT_PASSWORD", "minioadmin") }}'
    )
),
resolved AS (
    SELECT
        a.report_code,
        a.actor_id,
        a.game_id,
        a.name,
        a.actor_type,
        a.sub_type,
        a.pet_owner_id,
        a.server,
        a.icon,
        coalesce(o.name, a.name) AS resolved_player_name,
        coalesce(o.actor_type, a.actor_type) AS resolved_actor_type,
        coalesce(o.sub_type, a.sub_type) AS resolved_class,
        if(
            coalesce(o.actor_type, a.actor_type) = 'Player',
            toUInt64OrNull(toString(coalesce(o.game_id, a.game_id))),
            NULL
        ) AS player_guid,
        a.bronze_fetched_at
    FROM actors AS a
    LEFT JOIN actors AS o
        ON a.report_code = o.report_code
        AND a.pet_owner_id = o.actor_id
)
SELECT
    r.report_code,
    r.actor_id,
    r.game_id,
    r.name,
    r.actor_type,
    r.sub_type,
    r.pet_owner_id,
    r.server,
    r.icon,
    r.resolved_player_name,
    r.resolved_actor_type,
    r.resolved_class,
    r.player_guid,
    gr.character_name AS roster_character_name,
    gr.guild_name AS player_guild_name,
    gr.guild_rank,
    if(gr.character_name IS NOT NULL AND gr.character_name != '', 1, 0) AS is_guild_member,
    r.bronze_fetched_at,
    now() AS _silver_loaded_at
FROM resolved AS r
LEFT ANY JOIN {{ ref('wcl_guild_roster') }} AS gr
    ON {{ wcl_roster_match_actors('r', 'gr') }}

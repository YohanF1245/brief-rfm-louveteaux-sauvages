{#
  Macros partagées Warcraft Logs (gold).

  wcl_consumable_type(name_expr)
    Classe un nom de buff/sort en type de consommable.
    Les logs sont dans la langue du client (master_info.lang) → patterns FR + EN.
    Ajuster ici si un consommable n'est pas détecté (vérifier avec
    silver.wcl_abilities ou silver.wcl_pull_auras).

  wcl_difficulty_label(difficulty_expr, keystone_expr)
    Libellé lisible de la difficulté WoW.

  wcl_roster_match_player_details(pd_alias, gr_alias)
    Jointure roster pour bronze/silver player_details (GUID, id WCL, nom+serveur).

  wcl_roster_match_actors(a_alias, gr_alias)
    Jointure roster pour silver wcl_actors (game_id, actor_id, nom+serveur).
#}

{% macro wcl_roster_match_player_details(pd_alias, gr_alias) -%}
(
    toUInt64OrNull(toString({{ pd_alias }}.player_guid)) = {{ gr_alias }}.player_guid
    OR toUInt64OrNull(toString({{ pd_alias }}.player_guid)) = {{ gr_alias }}.canonical_id
    OR toInt64OrNull(toString({{ pd_alias }}.player_id)) = {{ gr_alias }}.wcl_character_id
    OR (
        lower(trim({{ pd_alias }}.player_name)) = lower(trim({{ gr_alias }}.character_name))
        AND lower(trim({{ pd_alias }}.server)) = lower(trim({{ gr_alias }}.server_slug))
        AND {{ pd_alias }}.player_name != ''
    )
)
{%- endmacro %}

{% macro wcl_roster_match_actors(a_alias, gr_alias) -%}
(
    {{ a_alias }}.player_guid = {{ gr_alias }}.player_guid
    OR {{ a_alias }}.player_guid = {{ gr_alias }}.canonical_id
    OR (
        {{ a_alias }}.resolved_actor_type = 'Player'
        AND {{ a_alias }}.actor_id = {{ gr_alias }}.wcl_character_id
    )
    OR (
        lower(trim({{ a_alias }}.resolved_player_name)) = lower(trim({{ gr_alias }}.character_name))
        AND lower(trim({{ a_alias }}.server)) = lower(trim({{ gr_alias }}.server_slug))
        AND {{ a_alias }}.resolved_player_name != ''
    )
)
{%- endmacro %}

{% macro wcl_consumable_type(name_expr) -%}
{%- set expr = "coalesce(" ~ name_expr ~ ", '')" -%}
multiIf(
    match({{ expr }}, '(?i)(flask|flacon)'), 'flask',
    match({{ expr }}, '(?i)(well fed|bien nourri|food|feast|festin)'), 'food',
    match({{ expr }}, '(?i)(healthstone|pierre de soins)'), 'healthstone',
    match({{ expr }}, '(?i)potion'), 'potion',
    match({{ expr }}, '(?i)(augment rune|rune d.augmentation|crystallized augment)'), 'augment_rune',
    match({{ expr }}, '(?i)(weapon oil|huile|whetstone|sharpening stone|weightstone|pierre . aiguiser)'), 'weapon_buff',
    ''
)
{%- endmacro %}

{% macro wcl_difficulty_label(difficulty_expr, keystone_expr) -%}
multiIf(
    coalesce({{ keystone_expr }}, 0) > 0, 'Mythic+',
    {{ difficulty_expr }} = 1, 'LFR',
    {{ difficulty_expr }} = 3, 'Normal',
    {{ difficulty_expr }} = 4, 'Heroic',
    {{ difficulty_expr }} = 5, 'Mythic',
    {{ difficulty_expr }} IS NOT NULL, concat('diff_', toString({{ difficulty_expr }})),
    'Unknown'
)
{%- endmacro %}

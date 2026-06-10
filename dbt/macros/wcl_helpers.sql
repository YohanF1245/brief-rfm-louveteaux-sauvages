{#
  Macros partagées Warcraft Logs (gold).

  wcl_consumable_type(name_expr)
    Classe un nom de buff/sort en type de consommable.
    Les logs sont dans la langue du client (master_info.lang) → patterns FR + EN.
    Ajuster ici si un consommable n'est pas détecté (vérifier avec
    silver.wcl_abilities ou silver.wcl_pull_auras).

  wcl_difficulty_label(difficulty_expr, keystone_expr)
    Libellé lisible de la difficulté WoW.
#}

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

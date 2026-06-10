# Warcraft Logs — couches Silver & Gold (ClickHouse + dbt)

Suite de [`wcl_bronze.md`](wcl_bronze.md) : transformation du bronze brut
(events Delta) en tables analytiques ClickHouse. Tout est recalculé depuis
`events` × `master_actors` × `master_abilities` — aucune table agrégée WCL.

## Orchestration : 4 DAGs chaînés par assets (Airflow 3)

Aucun cron dbt : chaque couche se déclenche quand l'amont a réussi.

```
warcraftlogs_guild_nightmares (ingest API → bronze)  ──► asset wcl_bronze ─┐
warcraftlogs_guild_roster     (roster API → bronze)  ──► asset wcl_bronze_roster ─┤ (OU)
                                                                           ▼
warcraftlogs_silver (dbt silver) ───────────────────────► asset wcl_silver
                                                                           ▼
warcraftlogs_gold (dbt gold + tests) ───────────────────► asset wcl_gold
```

- Assets définis dans `dags/warcraftlogs_assets.py`.
- Le DAG silver écoute `(wcl_bronze | wcl_bronze_roster)` : une mise à jour
  du roster seul suffit à rafraîchir `is_guild_member` jusqu'au gold.
- Trigger manuel possible sur chaque DAG (rebuild sans toucher à l'amont).
- Un run d'ingest sans nouveau report publie quand même l'asset : le run
  dbt aval est idempotent (tables recréées à l'identique).

## Couche Silver (`silver.wcl_*`)

Tables typées, 1 transformation = 1 responsabilité. Sélection dbt :
`WCL_DBT_SILVER` dans `dags/lakehouse_common.py`.

| Table | Grain | Rôle |
|---|---|---|
| `wcl_reports` | report | Métadonnées report (guilde, zone, dates absolues) |
| `wcl_fights` | fight | Pulls : durée, boss/trash, kill, difficulté, `start_time_ms`/`end_time_ms` **relatifs** au report (même référentiel que `events.timestamp_ms`) |
| `wcl_actors` | acteur × report | **Dimension clé** : pets résolus → `resolved_player_name`, `resolved_actor_type`, `player_guid`, `is_guild_member` (roster multi-clés) |
| `wcl_abilities` | sort × report | Noms de sorts (langue du log → regex FR+EN) |
| `wcl_events` | event | **Fait central** MergeTree trié `(report_code, fight_id, event_type, timestamp_ms)`. Champs extraits du JSON : `amount`, `absorbed`, `overkill`, `overheal`, `hit_type`, `is_tick`, `stack`, `extra_ability_game_id`, `killer_id`… Le JSON intégral reste en bronze. Exclut `combatantinfo` (voir ci-dessous) |
| `wcl_pull_combatants` | joueur × fight | Snapshot au pull : `spec_id`, `avg_item_level` (moyenne `gear[].itemLevel`) |
| `wcl_pull_auras` | aura × joueur × fight | Buffs **déjà actifs au pull** — indispensable à l'uptime des consommables |
| `wcl_player_details` | joueur × rôle × report | Specs, ilvl min/max, potions/healthstones, `is_guild_member` (roster multi-clés) |
| `wcl_player_guild_flags` | joueur × report | **Flag guilde unifié** : max(`player_details`, `actors`) — source du gold |
| `wcl_guild_roster` | membre | Roster guilde (sync quotidien) |
| `wcl_ingestion_state` | report | Suivi d'ingestion |

Résolution joueur (le bug « player unknown » est traité ICI, une fois) :

```sql
-- pets remontés au propriétaire dans wcl_actors :
coalesce(owner.name, actor.name)        AS resolved_player_name
coalesce(owner.actor_type, actor.actor_type) = 'Player'  -- filtre joueurs
```

Jointure roster (`wcl_roster_match_*` dans `dbt/macros/wcl_helpers.sql`) — **OR**
explicites (ClickHouse n'accepte pas `IN (col1, col2)` en JOIN) :

1. `player_guid` log = `player_guid` ou `canonical_id` roster
2. `player_id` / `actor_id` = `wcl_character_id` roster (si `canonicalID` absent API)
3. fallback `player_name` + `server` (même royaume)

Le gold calcule le flag via la macro `wcl_guild_flags_cte` (union
`player_details` + `actors`) — pas besoin que `silver.wcl_player_guild_flags`
soit matérialisée avant le DAG gold. La table homonyme reste dispo pour Play.

## Couche Gold (`gold.wcl_*_viz`)

Tables larges prêtes pour Streamlit / Power BI. Sélection dbt : `WCL_DBT_GOLD`.
Toutes portent : date/heure absolue du fight, boss, difficulté lisible,
kill/wipe, `is_guild_member`, `is_nightmares_asylum_report`.

### `wcl_fight_player_perf_viz` — joueur × fight (KPI central)
- **DPS / HPS / DTPS** (somme / durée du fight), crits, hits subis, absorbs
- morts, **interrupts**, **dispels**, potions & healthstones utilisées
- spé jouée et **ilvl moyen au pull** (combatantinfo) → évolution dans le temps
- Couvre : *évolution du DPS*, *DTPS par joueur*, *filtre membres guilde*,
  comparaison kill vs wipe, perf par difficulté, progression d'ilvl.

### `wcl_damage_taken_viz` — joueur × source × sort × fight
- *DTPS par joueur / source* : qui prend quoi, de quel NPC, sur quel sort
- `unmitigated_damage`, `max_hit`, `killing_hits` (coups fatals)
- Usage : dégâts évitables, tanks vs raid damage, sorts qui wipent.

### `wcl_consumables_viz` — joueur × consommable × fight
- Buffs (`flask`, `food`, `augment_rune`, `weapon_buff`) :
  - `present_at_pull` (0/1) — préparation au pull
  - `uptime_sec` / `uptime_pct` — **uptime exact par intervalles**
- Casts (`potion`, `healthstone`) : `casts` par fight
- Usage : *quels consommables sur un fight*, *uptime sur une soirée*
  (moyenne pondérée par durée des fights d'un report), % de pulls préparés.

Algorithme d'uptime (validé sur ClickHouse 25.8) :
1. état initial = aura présente au pull (`wcl_pull_auras`) — un flacon posé
   avant le pull n'émet aucun `applybuff` pendant le fight ;
2. transitions `applybuff`/`refreshbuff` (actif) et `removebuff` (inactif) ;
3. repli `arrayFold` sur les transitions triées + sentinelle à la fin du
   fight → somme des intervalles actifs.

Classification des consommables : macro `wcl_consumable_type`
(`dbt/macros/wcl_helpers.sql`), regex FR + EN sur le nom du sort
(`flacon|flask`, `bien nourri|well fed`, `potion`, …). À ajuster là-bas si
un consommable n'est pas détecté.

### `wcl_deaths_viz` — 1 ligne par mort
- `death_rank` (ordre dans le fight), `death_at_sec`, `death_at_pct_of_fight`
- sort fatal + tueur quand présents dans le log
- Usage : qui meurt en premier, morts évitables, analyse de wipe.

### `wcl_raid_nights_viz` — 1 ligne par soirée (report)
- pulls, boss kills/wipes, taux de kill, boss distincts, runs M+
- minutes de combat, durée moyenne de pull, meilleur wipe (`best_wipe_boss_pct`)
- effectif total / membres guilde, ilvl moyen
- Usage : rythme de progression, efficacité des soirées, assiduité.

### `wcl_attendance_viz` — membre roster × report
- `attended` 0/1 — présence de chaque membre du roster à chaque soirée
- Usage : taux de présence par joueur, effectif moyen, membres inactifs.

## Tests dbt

`dbt/models/gold/schema.yml` : `not_null` sur les clés de chaque viz,
`unique` sur `wcl_raid_nights_viz.report_code`, `accepted_values` sur
`consumable_type`. Lancés par la task `dbt_test_gold` du DAG gold.

## Limites & TODO

- `silver.wcl_events` est **reconstruite intégralement** à chaque run
  (scan du Delta bronze). À passer en incrémental (delete+insert par
  `report_code`) quand le volume rendra le rebuild trop long.
- Healing : les absorptions (`absorbed` events) sont créditées au lanceur
  du bouclier — approximation alignée WCL.
- DPS = total fight (pas de « temps actif » par joueur).
- Les anciennes pages Streamlit 042/043 pointent vers les anciennes tables
  gold (supprimées) → à migrer vers `wcl_fight_player_perf_viz` /
  `wcl_consumables_viz`.

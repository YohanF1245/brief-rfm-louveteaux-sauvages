# Bronze Warcraft Logs — données brutes

Documentation de référence du bronze WCL : schéma, provenance API, volumétrie,
idempotence. **Tout est brut** : aucune table agrégée WCL n'est ingérée, toutes
les métriques (DPS, soins, buffs, consommables, morts…) sont recalculables
depuis `events` × `master_actors` × `master_abilities`.

> Pourquoi ce choix : les tables agrégées `report.table` de l'API renvoient des
> formats instables selon le `dataType` (buffs sans résolution joueur →
> 308 820 lignes `unknown` lors de l'ancienne ingestion). Le log brut + le
> référentiel d'acteurs est exactement ce que le site WCL utilise en interne.

## Vue d'ensemble

```
API WCL v2 (GraphQL)                    s3://lake/bronze/warcraftlogs/
─────────────────────                   ──────────────────────────────
reports (catalogue)        ──────────►  guild_reports, ingestion_state
report { fights }          ──────────►  reports_raw, fights
report { masterData }      ──────────►  master_info, master_actors, master_abilities
report { playerDetails }   ──────────►  player_details
report { events All }      ──────────►  events          (paginé 10 000/page)
guild { members }          ──────────►  guild_roster    (DAG roster séparé)
```

- **DAG ingestion** : `warcraftlogs_guild_nightmares`
  (`sync_report_catalog` → `ingest_reports_incremental`)
- **DAG roster** : `warcraftlogs_guild_roster`
- **DAGs dbt** : `warcraftlogs_silver` / `warcraftlogs_gold` (déclenchés par assets, cf. `wcl_silver_gold.md`)
- **Code** : `dags/warcraftlogs_common.py` (API), `dags/warcraftlogs_lake.py`
  (Delta), `dags/warcraftlogs_ingest.py` (orchestration par report)

## Coût API par report

| Appel | Nombre | Note |
|-------|--------|------|
| `report { fights }` | 1 | métadonnées |
| `masterData` | 1 | acteurs + abilities |
| `playerDetails` | 1 | toute la plage du report |
| `events` | ⌈total_events / 10 000⌉ | le gros du coût |

Le quota (3 600 pts/h) est géré par `quota_allows_next_report()` (arrêt propre
entre reports) et le throttle adaptatif. Le coût médian mesuré par report est
stocké dans `ingestion_state.last_ingest_points` et recalibré automatiquement.

Si le quota tombe **au milieu** d'un report : le report passe en `error` et
sera ré-ingéré entièrement au run suivant (idempotent).

---

## Tables bronze

Toutes les tables sont au format **Delta Lake** sur MinIO et portent une
colonne `fetched_at` (UTC, ajoutée à l'écriture). L'idempotence est au grain
**report** : ré-ingérer un report supprime puis réécrit toutes ses lignes
(`delete WHERE report_code = … ` + `append`).

### 1. `events` — le log brut (cœur du bronze)

1 ligne = 1 event du log de combat. Source : `report.events(dataType: All)`,
paginé sur **toute la plage du report** (boss + trash + inter-pulls inclus).

| Colonne | Type | Description |
|---------|------|-------------|
| `report_code` | string | Code du report WCL |
| `fight_id` | Int64 | Fight auquel l'event appartient (`event.fight`, joint `fights.fight_id`) |
| `page_index` | Int64 | N° de page API (ordre d'ingestion, debug) |
| `event_index` | Int64 | Position de l'event dans sa page |
| `timestamp_ms` | Int64 | Temps **relatif au début du report** (comme `fights.start_time_ms`) |
| `event_type` | string | Type d'event (voir liste ci-dessous) |
| `source_id` | Int64 | Acteur source → `master_actors.actor_id` |
| `source_instance` | Int64 | Instance de la source (NPC dupliqués), nullable |
| `target_id` | Int64 | Acteur cible → `master_actors.actor_id` (-1 = aucun) |
| `target_instance` | Int64 | Instance de la cible, nullable |
| `ability_game_id` | Int64 | Sort → `master_abilities.ability_game_id` |
| `event_json` | string | **Event complet en JSON — aucune perte** |
| `fetched_at` | timestamp | Date d'ingestion |

#### Types d'events observés (`event_type`)

Constaté en réel sur un report M+ (`dataType: All` inclut bien tout) :

`damage`, `heal`, `absorbed`, `healabsorbed`, `applybuff`, `removebuff`,
`refreshbuff`, `applybuffstack`, `removebuffstack`, `applydebuff`,
`removedebuff`, `refreshdebuff`, `applydebuffstack`, `cast`, `begincast`,
`summon`, `resourcechange`, `combatantinfo`, `dungeonstart`, …
(plus selon contenu : `death`, `interrupt`, `dispel`, `encounterstart`, etc.)

#### Clés JSON par type (dans `event_json`)

Champs universels (déjà extraits en colonnes) : `timestamp`, `type`, `fight`,
`sourceID`, `targetID`, `abilityGameID`.

Champs fréquents restant dans le JSON :

| Clé | Présente sur | Description |
|-----|--------------|-------------|
| `amount` | damage, heal, resourcechange | Quantité effective (dégâts, soin…) |
| `unmitigatedAmount` | damage | Dégâts avant mitigation |
| `mitigated`, `absorbed`, `overheal`, `overkill` | damage / heal | Détail mitigation |
| `hitType` | damage, heal | 1=hit, 2=crit, … |
| `tick` | damage, heal | DoT/HoT tick |
| `isAoE` | damage | Dégât de zone |
| `stack` | *stack events* | Nombre de stacks |
| `extraAbilityGameID` | interrupt, dispel… | Sort secondaire concerné |
| `healerID`, `attackerID` | healabsorbed | Acteurs additionnels |
| `resourceChange`, `resourceChangeType`, `waste` | resourcechange | Ressources classe |
| `melee` | damage | Coup blanc |

Avec `includeResources: true` (défaut), chaque event "combat" porte aussi
l'état de l'acteur ressource : `hitPoints`, `maxHitPoints`, `classResources`
(mana/énergie/…), `attackPower`, `spellPower`, `armor`, `absorb`, `x`, `y`,
`facing`, `mapID`, `itemLevel`, `versatility`, `avoidance`
(→ heatmaps de position, courbes de PV, etc.).

L'event `combatantinfo` (1 par joueur **à chaque pull**) contient en plus :
`gear[]` (id, itemLevel, enchants, gems, bonusIDs, setID), `auras[]`
(buffs actifs au pull — **consommables food/flask inclus**), `talentTree[]`,
`talents[]`, `specID`, `faction`, `expansion`, et toutes les stats
(`strength`, `agility`, `stamina`, `intellect`, `mastery`, `leech`, …).

### 2. `master_actors` — référentiel acteurs (jointure obligatoire)

1 ligne = 1 acteur (joueur, pet, NPC) du report. Source : `masterData.actors`.

| Colonne | Type | Description |
|---------|------|-------------|
| `report_code` | string | Code report |
| `actor_id` | Int64 | **ID local au report** — référencé par `events.source_id` / `target_id` |
| `game_id` | Int64 | GUID jeu (persistant pour un joueur, id NPC pour les mobs) |
| `name` | string | Nom de l'acteur |
| `actor_type` | string | `Player`, `Pet`, `NPC` |
| `sub_type` | string | Joueur → classe (`Druid`, `Mage`…) ; NPC → `Boss` / `NPC` |
| `pet_owner_id` | Int64 | **Pets : `actor_id` du propriétaire** (attribution pet → joueur), nullable |
| `server` | string | Serveur (joueurs), nullable |
| `icon` | string | Icône site WCL (joueur → `Classe-Spé`) |

Résolution d'un event :

```sql
-- nom du joueur source (pets remontés au propriétaire)
SELECT coalesce(owner.name, a.name) AS player_name
FROM events e
JOIN master_actors a
  ON e.report_code = a.report_code AND e.source_id = a.actor_id
LEFT JOIN master_actors owner
  ON a.report_code = owner.report_code AND a.pet_owner_id = owner.actor_id
```

### 3. `master_abilities` — référentiel sorts

1 ligne = 1 ability rencontrée dans le report. Source : `masterData.abilities`.

| Colonne | Type | Description |
|---------|------|-------------|
| `report_code` | string | Code report |
| `ability_game_id` | Int64 | ID jeu du sort — joint `events.ability_game_id` |
| `name` | string | Nom du sort (langue du log, cf. `master_info.lang`) |
| `ability_type` | string | École de dégâts / type (code numérique WCL) |
| `icon` | string | Icône du sort |

### 4. `master_info` — versions et langue du report

1 ligne = 1 report. Source : `masterData` (scalaires).

| Colonne | Type | Description |
|---------|------|-------------|
| `report_code` | string | Code report |
| `log_version` | Int64 | Version du parseur client WCL |
| `game_version` | Int64 | 1 = Retail, autre = Classic… |
| `lang` | string | Langue source du log (`fr`, `en`…) — affecte `name` des abilities |
| `actors_count` / `abilities_count` | Int64 | Contrôle volumétrie |

### 5. `player_details` — specs, ilvl, talents, gear par joueur

1 ligne = 1 joueur × rôle, sur toute la plage du report.
Source : `report.playerDetails(includeCombatantInfo: true)`.

| Colonne | Type | Description |
|---------|------|-------------|
| `report_code` | string | Code report |
| `role` | string | `tanks` / `healers` / `dps` |
| `player_id` | Int64 | = `master_actors.actor_id` du joueur |
| `player_guid` | Int64 | **GUID WoW persistant** — joint `guild_roster.player_guid` |
| `player_name` | string | Nom du personnage |
| `server`, `region` | string | Serveur / région |
| `class_name` | string | Classe (`Druid`…) |
| `icon` | string | `Classe-Spé` |
| `min_item_level` / `max_item_level` | Int64 | ilvl min/max observé |
| `potion_use` / `healthstone_use` | Int64 | Compteurs WCL |
| `specs_json` | string | `[{spec, count}]` — spés jouées |
| `combatant_info_json` | string | Stats complètes, `talentTree`, gear (brut) |
| `player_json` | string | Bloc joueur intégral (aucune perte) |

Non bloquant : si `playerDetails` échoue (report vide/archivé), l'ingestion
continue (events + masterData restent la source de vérité).

### 6. `fights` — métadonnées combats

1 ligne = 1 fight (boss **et** trash). Source : `report.fights`.

| Colonne | Type | Description |
|---------|------|-------------|
| `report_code` | string | Code report |
| `fight_id` | Int64 | ID du fight — joint `events.fight_id` |
| `encounter_id` | Int64 | ID rencontre boss (0 = trash) |
| `fight_name` | string | Nom (boss ou zone) |
| `start_time_ms` / `end_time_ms` | Int64 | Bornes relatives au report |
| `duration_ms` | Int64 | Durée |
| `kill` | bool | Kill (null/false = wipe), nullable |
| `difficulty` | Int64 | 1=LFR, 3=NM, 4=HM, 5=MM, 10=M+ (WoW) |
| `size` | Int64 | Taille du groupe |
| `boss_percentage` | float | % PV boss restant (wipe) |
| `keystone_level` / `keystone_time_ms` | Int64 | M+ : niveau de clé / timer |
| `is_boss` | bool | `encounter_id != 0` |

### 7. `guild_reports` — catalogue des reports

1 ligne = 1 report connu (logs guilde + logs perso publics des membres
`wcl_user_ids`). Colonnes : `report_code`, `guild_id`, `guild_name`,
`server_region`, `server_slug`, `title`, `zone_name`, `owner_name`,
`owner_user_id`, `log_source` (`guild`/`personal`), `visibility`,
`start_time_ms`, `end_time_ms` (epoch ms absolus), `fetched_at`.

### 8. `reports_raw` — réponse API brute

1 ligne = 1 report : `report_code`, `raw_json` (réponse `report { fights }`
intégrale), `fetched_at`. Filet de sécurité / audit.

### 9. `ingestion_state` — suivi d'ingestion

1 ligne = 1 report du catalogue : `report_code`, `status`
(`pending` / `ok` / `error`), `last_error`, `ingestion_attempts`,
`last_ingest_points` (coût API mesuré), `synced_at`, `catalog_json`, …
Reset pour ré-ingest : `scripts/reset_wcl_ingestion.py`.

### 10. `guild_roster` — roster guilde (DAG séparé)

1 ligne = 1 membre : `player_guid` (= `canonicalID`), `character_name`,
`class_id`/`class_name`, `guild_rank`, `character_level`, serveur/région.
Jointure silver (multi-clés) : GUID log = `canonical_id` / `player_guid` roster,
ou `player_id` = `wcl_character_id`, ou nom+serveur — cf. `docs/wcl_silver_gold.md`.

---

## Garanties & limites

| Garantie | Détail |
|----------|--------|
| **Aucune perte** | `event_json` / `player_json` / `raw_json` conservent 100 % de la réponse API |
| **Idempotence** | ré-ingestion d'un report = delete + append (pas de doublons) |
| **RAM constante** | events flush par page de 10 000 (VPS 8 Go OK) |
| **Anti-boucle** | curseur `nextPageTimestamp` vérifié strictement croissant |
| **Pets** | `master_actors.pet_owner_id` → attribution au joueur |
| **Trash & inter-pulls** | plage complète du report ingérée (pas seulement les boss) |

| Limite | Détail |
|--------|--------|
| Langue | `abilities.name` est dans la langue du log (`master_info.lang`) |
| Events « non figés » | WCL documente `events` comme *not frozen* (rejouable : re-pending le report) |
| Quota | un gros report = 30-100 pages ; le batch s'arrête proprement entre reports |
| Bordure de page | en cas de doublon à la frontière `nextPageTimestamp`, dédupliquer en silver sur `(report_code, timestamp_ms, event_type, source_id, target_id, ability_game_id, event_index)` |

## Variables Airflow (ingestion)

| Variable | Défaut | Rôle |
|----------|--------|------|
| `wcl_guild_url` | URL Nightmares Asylum | Guilde cible |
| `wcl_user_ids` | — | Logs perso publics des membres |
| `wcl_ingest_batch_size` | 25 | Reports max par run |
| `wcl_events_page_size` | 10000 | Taille page events (100-10000) |
| `wcl_events_include_resources` | true | PV/ressources/position dans chaque event |
| `wcl_adaptive_rate_limit` | true | Throttle adaptatif au quota |
| `wcl_quota_reserve_fraction` | 0.05 | Réserve de points conservée |
| `wcl_dag_schedule` | `*/30 * * * *` | Cron du DAG ingestion |

## Recalculer les métriques (exemples ClickHouse)

```sql
-- DPS par joueur sur les boss (équivalent table DamageDone)
SELECT coalesce(o.name, a.name) AS player,
       sum(JSONExtractInt(e.event_json, 'amount')) / (max(f.duration_ms) / 1000.0) AS dps
FROM v_wcl_events e
JOIN v_wcl_master_actors a ON e.report_code = a.report_code AND e.source_id = a.actor_id
LEFT JOIN v_wcl_master_actors o ON a.report_code = o.report_code AND a.pet_owner_id = o.actor_id
JOIN v_wcl_fights f ON e.report_code = f.report_code AND e.fight_id = f.fight_id
WHERE e.event_type = 'damage' AND f.is_boss = 1
  AND coalesce(o.actor_type, a.actor_type) = 'Player'
GROUP BY player ORDER BY dps DESC;

-- Buffs consommables actifs au pull (food/flask) — via combatantinfo.auras
SELECT a.name AS player, ab.name AS buff
FROM v_wcl_events e
JOIN v_wcl_master_actors a ON e.report_code = a.report_code AND e.source_id = a.actor_id
ARRAY JOIN JSONExtractArrayRaw(e.event_json, 'auras') AS aura
JOIN v_wcl_master_abilities ab
  ON e.report_code = ab.report_code
 AND ab.ability_game_id = JSONExtractInt(aura, 'ability')
WHERE e.event_type = 'combatantinfo';
```

Setup des vues `v_wcl_*` : `clickhouse/queries/00_setup_views.sql`.

## Aval (silver / gold)

Couches silver et gold construites sur ce bronze : voir
[`wcl_silver_gold.md`](wcl_silver_gold.md) (modèles dbt, KPI, DAGs
`warcraftlogs_silver` / `warcraftlogs_gold` déclenchés par assets).

Reste à faire : repointer les pages Streamlit 042/043 vers
`gold.wcl_fight_player_perf_viz` / `gold.wcl_consumables_viz`.

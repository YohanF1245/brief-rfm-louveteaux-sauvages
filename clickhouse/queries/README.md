# Requêtes ClickHouse — Warcraft Logs

## Workflow

1. Login `/play` → `default` + `CLICKHOUSE_PASSWORD`
2. **Une fois** : `00_setup_views.sql` — remplace `TON_MINIO_ROOT_USER` / `TON_MINIO_ROOT_PASSWORD`
3. Toutes les autres requêtes utilisent les vues `v_wcl_*` (plus de creds)

Schéma bronze complet : `docs/wcl_bronze.md`.

## Vues (bronze données brutes)

| Vue | Table bronze | Contenu |
|-----|--------------|---------|
| `v_wcl_ingestion_state` | ingestion_state | Suivi ingestion par report |
| `v_wcl_guild_reports` | guild_reports | Catalogue reports |
| `v_wcl_fights` | fights | 1 ligne / fight (boss + trash) |
| `v_wcl_reports_raw` | reports_raw | JSON report brut |
| `v_wcl_master_info` | master_info | Versions log/jeu, langue |
| `v_wcl_master_actors` | master_actors | id → joueur / pet / NPC |
| `v_wcl_master_abilities` | master_abilities | id → sort |
| `v_wcl_player_details` | player_details | Specs, ilvl, talents, gear |
| `v_wcl_events` | events | **Log brut, 1 ligne / event** |
| `v_wcl_guild_roster` | guild_roster | Roster guilde (player_guid) |

## Fichiers

| Fichier | Sujet |
|---------|--------|
| `00_setup_views.sql` | **Setup creds MinIO → vues** |
| `wcl_bronze_discovery.sql` | Volumétrie + exploration du bronze brut |
| `test_wcl.sql` | Smoke test |

### Legacy (ancien bronze agrégé — à réécrire sur `v_wcl_events`)

`00_examples_views.sql`, `test_wcl_bronze.sql`, `test_wcl_silver.sql`,
`top_dps_wcl.sql`, `wcl_kimahri.sql`, `wcl_raid_bosses.sql`,
`wcl_mythic_plus.sql`, `wcl_healing_deaths.sql`, `wcl_roster_timeline.sql`,
`wcl_report_deep_dive.sql`, `wcl_metrics_advanced.sql`, `wcl_silver_dps.sql` :
ces requêtes référencent `v_wcl_fight_player_stats` / `silver.wcl_player_fight_metrics`
(tables agrégées supprimées). Exemples de réécriture events : `docs/wcl_bronze.md` §
« Recalculer les métriques ».

Un `SELECT` à la fois dans Play.

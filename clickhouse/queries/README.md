# Requêtes ClickHouse — Warcraft Logs

## Workflow

1. Login `/play` → `default` + `CLICKHOUSE_PASSWORD`
2. **Une fois** : `00_setup_views.sql` — remplace `TON_MINIO_ROOT_USER` / `TON_MINIO_ROOT_PASSWORD`
3. Toutes les autres requêtes utilisent les vues `v_wcl_*` (plus de creds)

## Vues

| Vue | Table bronze |
|-----|----------------|
| `v_wcl_ingestion_state` | ingestion_state |
| `v_wcl_guild_reports` | guild_reports |
| `v_wcl_fights` | fights |
| `v_wcl_fight_player_stats` | fight_player_stats |
| `v_wcl_fight_tables_raw` | fight_tables_raw |
| `v_wcl_reports_raw` | reports_raw |

## Fichiers

| Fichier | Sujet |
|---------|--------|
| `00_setup_views.sql` | **Setup creds MinIO → vues** |
| `00_examples_views.sql` | Exemples |
| `test_wcl.sql` | Smoke test |
| `test_wcl_bronze.sql` | Validation bronze |
| `top_dps_wcl.sql` | Top DPS |
| `wcl_kimahri.sql` | Kimahri |
| `wcl_raid_bosses.sql` | Raid |
| `wcl_mythic_plus.sql` | M+ |
| `wcl_healing_deaths.sql` | Soins / morts |
| `wcl_roster_timeline.sql` | Roster |
| `wcl_report_deep_dive.sql` | Un report |
| `wcl_metrics_advanced.sql` | Casts, threat, … |

Un `SELECT` à la fois dans Play.

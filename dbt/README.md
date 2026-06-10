# dbt — lakehouse RFM

Transformations SQL versionnées (couche **gold** ClickHouse + option Postgres).

## Warcraft Logs

Le bronze WCL est passé aux **données brutes** (events + masterData), voir
`docs/wcl_bronze.md`. Couches silver/gold reconstruites dessus, documentées
dans `docs/wcl_silver_gold.md` :

- **silver** : `wcl_reports`, `wcl_fights`, `wcl_actors` (pets résolus),
  `wcl_abilities`, `wcl_events` (fait central MergeTree), `wcl_pull_combatants`,
  `wcl_pull_auras`, `wcl_player_details`, `wcl_guild_roster`, `wcl_ingestion_state`
- **gold** (KPI) : `wcl_fight_player_perf_viz`, `wcl_damage_taken_viz`,
  `wcl_consumables_viz`, `wcl_deaths_viz`, `wcl_raid_nights_viz`,
  `wcl_attendance_viz`
- macros : `wcl_consumable_type` / `wcl_difficulty_label` (`macros/wcl_helpers.sql`)

Les modèles basés sur l'ancien bronze agrégé (`wcl_player_fight_metrics`,
`wcl_fight_player_guids`, `wcl_boss_dps`, `wcl_player_dps_viz`,
`wcl_raid_consumables_viz`) restent désactivés (`enabled=false`), conservés
comme référence de logique métier.

## Cibles

| Target | Moteur | Usage |
|--------|--------|-------|
| `dev` (défaut) | ClickHouse `gold` | Analytics lakehouse |
| `postgres` | PostgreSQL `rfm` | Modèles alignés app Streamlit |

## Commandes (depuis la racine du projet)

```bash
docker compose run --rm dbt dbt deps
docker compose run --rm dbt dbt run
docker compose run --rm dbt dbt test
docker compose run --rm dbt dbt docs generate
```

Documentation servie en prod : `https://ymfo1nom.com/dbt/` (service `dbt-docs`).

## Power BI (couche gold)

Après le DAG Airflow `lakehouse_stack_test` :

| Paramètre | Valeur (local dev) |
|-----------|-------------------|
| Connecteur | **ClickHouse** (natif Power BI) |
| Serveur | `localhost` |
| Port | `8123` (HTTP) |
| Base | `gold` |
| Table | `stack_test_daily_kpis` |

Requête de contrôle :

```sql
SELECT * FROM gold.stack_test_daily_kpis ORDER BY event_date, region;
```

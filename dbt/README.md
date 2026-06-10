# dbt — lakehouse RFM

Transformations SQL versionnées (couche **gold** ClickHouse + option Postgres).

## Warcraft Logs

Le bronze WCL est passé aux **données brutes** (events + masterData), voir
`docs/wcl_bronze.md`. Modèles actifs : `wcl_reports`, `wcl_fights`,
`wcl_ingestion_state`, `wcl_guild_roster`. Les modèles basés sur l'ancien
bronze agrégé (`wcl_player_fight_metrics`, `wcl_fight_player_guids`,
`wcl_boss_dps`, `wcl_player_dps_viz`, `wcl_raid_consumables_viz`) sont
désactivés (`enabled=false`) et servent de référence pour la reconstruction
silver/gold depuis `events` × `master_actors`.

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

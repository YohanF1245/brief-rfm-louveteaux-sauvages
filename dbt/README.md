# dbt — lakehouse RFM

Transformations SQL versionnées (couche **gold** ClickHouse + option Postgres).

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

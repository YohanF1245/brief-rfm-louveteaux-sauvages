{{ config(
    materialized='table',
    schema='silver',
    tags=['stack_test', 'silver']
) }}

/*
  Silver : lecture de la bronze Delta sur MinIO via le moteur ClickHouse.
  Credentials MinIO injectés via variables d'environnement (worker Airflow / dbt).
*/
SELECT
    toDate(event_date) AS event_date,
    region,
    product,
    toInt32(quantity) AS quantity,
    toFloat64(unit_price) AS unit_price,
    toFloat64(quantity) * toFloat64(unit_price) AS revenue,
    now() AS _loaded_at
FROM deltaLake(
    'http://minio:9000/lake/bronze/stack_test/ventes',
    '{{ env_var("MINIO_ROOT_USER", "minioadmin") }}',
    '{{ env_var("MINIO_ROOT_PASSWORD", "minioadmin") }}'
)

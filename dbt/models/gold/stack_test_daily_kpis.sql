{{ config(
    materialized='table',
    schema='gold',
    tags=['stack_test', 'gold', 'power_bi']
) }}

/*
  Gold Power BI : table plate, grain jour × région.
  Connexion PBI : ClickHouse → base ``gold`` → table ``stack_test_daily_kpis``.
*/
SELECT
    event_date,
    region,
    count() AS nb_lignes,
    sum(quantity) AS total_quantity,
    round(sum(revenue), 2) AS total_revenue,
    round(avg(unit_price), 2) AS avg_unit_price
FROM {{ ref('stack_test_ventes') }}
GROUP BY
    event_date,
    region
ORDER BY
    event_date,
    region

select station_nom, date_mesure, temp_max
from climat_data cd
order by temp_max desc limit 100;

SELECT
    station_id,
    station_nom,
    CAST(strftime('%m', date_mesure) AS INTEGER) AS mois,
    CAST(strftime('%d', date_mesure) AS INTEGER) AS jour,
    MAX(CASE WHEN CAST(strftime('%Y', date_mesure) AS INTEGER) = 2025 THEN temp_max END) AS temp_max_2025,
    MAX(CASE WHEN CAST(strftime('%Y', date_mesure) AS INTEGER) = 2024 THEN temp_max END) AS temp_max_2024,
    MAX(CASE WHEN CAST(strftime('%Y', date_mesure) AS INTEGER) = 2023 THEN temp_max END) AS temp_max_2023,
    MAX(CASE WHEN CAST(strftime('%Y', date_mesure) AS INTEGER) = 2022 THEN temp_max END) AS temp_max_2022
FROM climat_data
WHERE CAST(strftime('%m', date_mesure) AS INTEGER) BETWEEN 5 AND 8
GROUP BY station_id, station_nom, mois, jour
ORDER BY station_id, mois, jour;

SELECT
    station_nom,
    CAST(strftime('%m', date_mesure) AS INTEGER) AS mois,
    CAST(strftime('%d', date_mesure) AS INTEGER) AS jour,
    GROUP_CONCAT(DISTINCT strftime('%Y', date_mesure)) AS annees_presentes
FROM climat_data
WHERE CAST(strftime('%m', date_mesure) AS INTEGER) BETWEEN 5 AND 8
GROUP BY station_id, station_nom, mois, jour
LIMIT 20;

WITH hist AS (
    SELECT
        station_id,
        MAX(temp_max) AS temp_max_all_time
    FROM climat_data
    WHERE CAST(strftime('%Y', date_mesure) AS INTEGER) < 2026
      AND temp_max IS NOT NULL
    GROUP BY station_id
),
y2026 AS (
    SELECT
        station_id,
        station_nom,
        date_mesure,
        temp_max AS temp_max_2026
    FROM climat_data
    WHERE strftime('%Y', date_mesure) = '2026'
      AND CAST(strftime('%m', date_mesure) AS INTEGER) BETWEEN 5 AND 8
)
SELECT
    y.station_nom,
    y.date_mesure,
    y.temp_max_2026,
    h.temp_max_all_time,
    y.temp_max_2026 - h.temp_max_all_time AS ecart_vs_record,
    CASE
        WHEN y.temp_max_2026 > h.temp_max_all_time THEN 1
        ELSE 0
    END AS bat_record_absolu
FROM y2026 y
JOIN hist h ON h.station_id = y.station_id
ORDER BY ecart_vs_record DESC;

WITH y2026 AS (
    SELECT MAX(temp_max) AS max_temp
    FROM climat_data
    WHERE strftime('%Y', date_mesure) = '2026'
      AND CAST(strftime('%m', date_mesure) AS INTEGER) BETWEEN 5 AND 8
      AND temp_max IS NOT NULL
),
hist AS (
    SELECT MAX(temp_max) AS max_temp
    FROM climat_data
    WHERE CAST(strftime('%Y', date_mesure) AS INTEGER) < 2026
      AND CAST(strftime('%m', date_mesure) AS INTEGER) BETWEEN 5 AND 8
      AND temp_max IS NOT NULL
)
SELECT
    y.max_temp AS max_2026,
    h.max_temp AS max_all_time,
    y.max_temp - h.max_temp AS ecart_vs_record,
    CASE WHEN y.max_temp > h.max_temp THEN 1 ELSE 0 END AS bat_record_absolu
FROM y2026 y, hist h;

WITH hist AS (
    SELECT
        CAST(strftime('%m', date_mesure) AS INTEGER) AS mois,
        CAST(strftime('%d', date_mesure) AS INTEGER) AS jour,
        MAX(temp_max) AS max_all_time
    FROM climat_data
    WHERE CAST(strftime('%Y', date_mesure) AS INTEGER) < 2026
      AND CAST(strftime('%m', date_mesure) AS INTEGER) BETWEEN 5 AND 8
      AND temp_max IS NOT NULL
    GROUP BY mois, jour
),
y2026 AS (
    SELECT
        CAST(strftime('%m', date_mesure) AS INTEGER) AS mois,
        CAST(strftime('%d', date_mesure) AS INTEGER) AS jour,
        MAX(temp_max) AS max_2026
    FROM climat_data
    WHERE strftime('%Y', date_mesure) = '2026'
      AND CAST(strftime('%m', date_mesure) AS INTEGER) BETWEEN 5 AND 8
      AND temp_max IS NOT NULL
      
      WITH hist AS (
    SELECT
        CAST(strftime('%m', date_mesure) AS INTEGER) AS mois,
        CAST(strftime('%d', date_mesure) AS INTEGER) AS jour,
        MAX(temp_max) AS max_all_time
    FROM climat_data
    WHERE CAST(strftime('%Y', date_mesure) AS INTEGER) < 2026
      AND CAST(strftime('%m', date_mesure) AS INTEGER) BETWEEN 5 AND 8
      AND temp_max IS NOT NULL
    GROUP BY mois, jour
),
hist_record AS (
  SELECT
    h.mois,
    h.jour,
    h.max_all_time,
    (
      SELECT CAST(strftime('%Y', d.date_mesure) AS INTEGER)
      FROM climat_data d
      WHERE CAST(strftime('%m', d.date_mesure) AS INTEGER) = h.mois
        AND CAST(strftime('%d', d.date_mesure) AS INTEGER) = h.jour
        AND CAST(strftime('%Y', d.date_mesure) AS INTEGER) < 2026
        AND CAST(strftime('%m', d.date_mesure) AS INTEGER) BETWEEN 5 AND 8
        AND d.temp_max = h.max_all_time
      ORDER BY CAST(strftime('%Y', d.date_mesure) AS INTEGER) DESC
      LIMIT 1
    ) AS annee_record
  FROM hist h
),
y2026 AS (
    SELECT
        CAST(strftime('%m', date_mesure) AS INTEGER) AS mois,
        CAST(strftime('%d', date_mesure) AS INTEGER) AS jour,
        MAX(temp_max) AS max_2026
    FROM climat_data
    WHERE strftime('%Y', date_mesure) = '2026'
      AND CAST(strftime('%m', date_mesure) AS INTEGER) BETWEEN 5 AND 8
      AND temp_max IS NOT NULL
    GROUP BY mois, jour
)
SELECT
    y.mois,
    y.jour,
    printf('2026-%02d-%02d', y.mois, y.jour) AS date_2026,
    y.max_2026,
    r.max_all_time,
    r.annee_record,
    y.max_2026 - r.max_all_time AS ecart_vs_record,
    CASE WHEN y.max_2026 > r.max_all_time THEN 1 ELSE 0 END AS bat_record_jour
FROM y2026 y
LEFT JOIN hist_record r ON r.mois = y.mois AND r.jour = y.jour
ORDER BY y.mois, y.jour;
    GROUP BY mois, jour
)
SELECT
    y.mois,
    y.jour,
    y.max_2026,
    h.max_all_time,
    y.max_2026 - h.max_all_time AS ecart_vs_record,
    CASE WHEN y.max_2026 > h.max_all_time THEN 1 ELSE 0 END AS bat_record_jour
FROM y2026 y
LEFT JOIN hist h ON h.mois = y.mois AND h.jour = y.jour
ORDER BY y.mois, y.jour;
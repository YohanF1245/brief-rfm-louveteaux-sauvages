-- =============================================================================
-- Setup UNE FOIS dans Play — remplace USER / PASS (MINIO_ROOT_* du .env)
-- Ensuite : SELECT ... FROM v_wcl_* (plus de creds MinIO dans les requêtes)
-- =============================================================================

CREATE OR REPLACE VIEW v_wcl_ingestion_state AS
SELECT * FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/ingestion_state',
    'TON_MINIO_ROOT_USER',
    'TON_MINIO_ROOT_PASSWORD'
);

CREATE OR REPLACE VIEW v_wcl_guild_reports AS
SELECT * FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/guild_reports',
    'TON_MINIO_ROOT_USER',
    'TON_MINIO_ROOT_PASSWORD'
);

CREATE OR REPLACE VIEW v_wcl_fights AS
SELECT * FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/fights',
    'TON_MINIO_ROOT_USER',
    'TON_MINIO_ROOT_PASSWORD'
);

CREATE OR REPLACE VIEW v_wcl_fight_player_stats AS
SELECT * FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/fight_player_stats',
    'TON_MINIO_ROOT_USER',
    'TON_MINIO_ROOT_PASSWORD'
);

CREATE OR REPLACE VIEW v_wcl_fight_tables_raw AS
SELECT * FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/fight_tables_raw',
    'TON_MINIO_ROOT_USER',
    'TON_MINIO_ROOT_PASSWORD'
);

CREATE OR REPLACE VIEW v_wcl_reports_raw AS
SELECT * FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/reports_raw',
    'TON_MINIO_ROOT_USER',
    'TON_MINIO_ROOT_PASSWORD'
);

-- Vérif
SELECT 'v_wcl_ingestion_state' AS view_name, count() AS n FROM v_wcl_ingestion_state;

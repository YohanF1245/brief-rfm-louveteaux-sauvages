-- =============================================================================
-- Setup UNE FOIS dans Play — remplace USER / PASS (MINIO_ROOT_* du .env)
-- Ensuite : SELECT ... FROM v_wcl_* (plus de creds MinIO dans les requêtes)
-- Schéma bronze documenté dans docs/wcl_bronze.md
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

CREATE OR REPLACE VIEW v_wcl_reports_raw AS
SELECT * FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/reports_raw',
    'TON_MINIO_ROOT_USER',
    'TON_MINIO_ROOT_PASSWORD'
);

CREATE OR REPLACE VIEW v_wcl_master_info AS
SELECT * FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/master_info',
    'TON_MINIO_ROOT_USER',
    'TON_MINIO_ROOT_PASSWORD'
);

CREATE OR REPLACE VIEW v_wcl_master_actors AS
SELECT * FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/master_actors',
    'TON_MINIO_ROOT_USER',
    'TON_MINIO_ROOT_PASSWORD'
);

CREATE OR REPLACE VIEW v_wcl_master_abilities AS
SELECT * FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/master_abilities',
    'TON_MINIO_ROOT_USER',
    'TON_MINIO_ROOT_PASSWORD'
);

CREATE OR REPLACE VIEW v_wcl_player_details AS
SELECT * FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/player_details',
    'TON_MINIO_ROOT_USER',
    'TON_MINIO_ROOT_PASSWORD'
);

CREATE OR REPLACE VIEW v_wcl_events AS
SELECT * FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/events',
    'TON_MINIO_ROOT_USER',
    'TON_MINIO_ROOT_PASSWORD'
);

CREATE OR REPLACE VIEW v_wcl_guild_roster AS
SELECT * FROM deltaLake(
    'http://minio:9000/lake/bronze/warcraftlogs/guild_roster',
    'TON_MINIO_ROOT_USER',
    'TON_MINIO_ROOT_PASSWORD'
);

-- Vérif
SELECT 'v_wcl_ingestion_state' AS view_name, count() AS n FROM v_wcl_ingestion_state;

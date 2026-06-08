"""Schéma PostgreSQL Warcraft Logs + helpers d'upsert (DATA-DB)."""

from __future__ import annotations

from typing import Any

SCHEMA = "public"
TABLE_REPORTS = "wcl_guild_reports"
TABLE_FIGHTS = "wcl_fights"
TABLE_PLAYER_STATS = "wcl_fight_player_stats"

FULL_REPORTS = f"{SCHEMA}.{TABLE_REPORTS}"
FULL_FIGHTS = f"{SCHEMA}.{TABLE_FIGHTS}"
FULL_PLAYER_STATS = f"{SCHEMA}.{TABLE_PLAYER_STATS}"

DDL_STATEMENTS = [
    f"""
    CREATE TABLE IF NOT EXISTS {FULL_REPORTS} (
        report_code TEXT PRIMARY KEY,
        guild_id BIGINT,
        guild_name TEXT,
        server_region TEXT,
        server_slug TEXT,
        title TEXT,
        zone_name TEXT,
        owner_name TEXT,
        owner_user_id BIGINT,
        log_source TEXT,
        visibility TEXT,
        start_time_ms BIGINT,
        end_time_ms BIGINT,
        fights_fetched_at TIMESTAMPTZ,
        bronze_synced_at TIMESTAMPTZ,
        last_error TEXT,
        ingestion_attempts INTEGER NOT NULL DEFAULT 0,
        fetched_at TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {FULL_FIGHTS} (
        report_code TEXT NOT NULL REFERENCES {FULL_REPORTS} (report_code) ON DELETE CASCADE,
        fight_id INTEGER NOT NULL,
        encounter_id INTEGER NOT NULL,
        fight_name TEXT,
        start_time_ms BIGINT,
        end_time_ms BIGINT,
        duration_ms BIGINT,
        kill BOOLEAN,
        difficulty INTEGER,
        size INTEGER,
        boss_percentage DOUBLE PRECISION,
        keystone_level INTEGER,
        keystone_time_ms BIGINT,
        is_boss BOOLEAN NOT NULL DEFAULT FALSE,
        stats_fetched_at TIMESTAMPTZ,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (report_code, fight_id)
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {FULL_PLAYER_STATS} (
        report_code TEXT NOT NULL,
        fight_id INTEGER NOT NULL,
        player_name TEXT NOT NULL,
        metric TEXT NOT NULL,
        player_id INTEGER,
        class_name TEXT,
        spec_name TEXT,
        total_amount BIGINT,
        active_time_ms BIGINT,
        rate_per_sec DOUBLE PRECISION,
        extra JSONB,
        fetched_at TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (report_code, fight_id, player_name, metric),
        FOREIGN KEY (report_code, fight_id)
            REFERENCES {FULL_FIGHTS} (report_code, fight_id) ON DELETE CASCADE
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_wcl_fights_boss ON {FULL_FIGHTS} (report_code, is_boss)",
    f"CREATE INDEX IF NOT EXISTS idx_wcl_fights_stats_pending ON {FULL_FIGHTS} (stats_fetched_at)",
    f"CREATE INDEX IF NOT EXISTS idx_wcl_player_stats_metric ON {FULL_PLAYER_STATS} (metric)",
    f"CREATE INDEX IF NOT EXISTS idx_wcl_reports_bronze_pending ON {FULL_REPORTS} (bronze_synced_at)",
]

REPORT_UPSERT_SQL = f"""
INSERT INTO {FULL_REPORTS} (
    report_code, guild_id, guild_name, server_region, server_slug,
    title, zone_name, owner_name, owner_user_id, log_source, visibility,
    start_time_ms, end_time_ms, fetched_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (report_code) DO UPDATE SET
    guild_id = EXCLUDED.guild_id,
    guild_name = EXCLUDED.guild_name,
    title = EXCLUDED.title,
    zone_name = EXCLUDED.zone_name,
    owner_name = EXCLUDED.owner_name,
    owner_user_id = EXCLUDED.owner_user_id,
    log_source = EXCLUDED.log_source,
    visibility = EXCLUDED.visibility,
    start_time_ms = EXCLUDED.start_time_ms,
    end_time_ms = EXCLUDED.end_time_ms,
    fetched_at = NOW()
"""

FIGHT_UPSERT_SQL = f"""
INSERT INTO {FULL_FIGHTS} (
    report_code, fight_id, encounter_id, fight_name,
    start_time_ms, end_time_ms, duration_ms, kill, difficulty, size,
    boss_percentage, keystone_level, keystone_time_ms, is_boss, fetched_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (report_code, fight_id) DO UPDATE SET
    encounter_id = EXCLUDED.encounter_id,
    fight_name = EXCLUDED.fight_name,
    start_time_ms = EXCLUDED.start_time_ms,
    end_time_ms = EXCLUDED.end_time_ms,
    duration_ms = EXCLUDED.duration_ms,
    kill = EXCLUDED.kill,
    difficulty = EXCLUDED.difficulty,
    size = EXCLUDED.size,
    boss_percentage = EXCLUDED.boss_percentage,
    keystone_level = EXCLUDED.keystone_level,
    keystone_time_ms = EXCLUDED.keystone_time_ms,
    is_boss = EXCLUDED.is_boss,
    fetched_at = NOW()
"""

PLAYER_STAT_UPSERT_SQL = f"""
INSERT INTO {FULL_PLAYER_STATS} (
    report_code, fight_id, player_name, metric, player_id, class_name,
    spec_name, total_amount, active_time_ms, rate_per_sec, extra, fetched_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
ON CONFLICT (report_code, fight_id, player_name, metric) DO UPDATE SET
    player_id = EXCLUDED.player_id,
    class_name = EXCLUDED.class_name,
    spec_name = EXCLUDED.spec_name,
    total_amount = EXCLUDED.total_amount,
    active_time_ms = EXCLUDED.active_time_ms,
    rate_per_sec = EXCLUDED.rate_per_sec,
    extra = EXCLUDED.extra,
    fetched_at = NOW()
"""


def ensure_tables(conn: Any) -> None:
    with conn.cursor() as cur:
        for ddl in DDL_STATEMENTS:
            cur.execute(ddl)
        for ddl in (
            f"ALTER TABLE {FULL_REPORTS} ADD COLUMN IF NOT EXISTS fights_fetched_at TIMESTAMPTZ",
            f"ALTER TABLE {FULL_REPORTS} ADD COLUMN IF NOT EXISTS owner_user_id BIGINT",
            f"ALTER TABLE {FULL_REPORTS} ADD COLUMN IF NOT EXISTS log_source TEXT",
            f"ALTER TABLE {FULL_REPORTS} ADD COLUMN IF NOT EXISTS visibility TEXT",
            f"ALTER TABLE {FULL_FIGHTS} ADD COLUMN IF NOT EXISTS keystone_level INTEGER",
            f"ALTER TABLE {FULL_FIGHTS} ADD COLUMN IF NOT EXISTS keystone_time_ms BIGINT",
            f"ALTER TABLE {FULL_REPORTS} ADD COLUMN IF NOT EXISTS bronze_synced_at TIMESTAMPTZ",
            f"ALTER TABLE {FULL_REPORTS} ADD COLUMN IF NOT EXISTS last_error TEXT",
            f"ALTER TABLE {FULL_REPORTS} ADD COLUMN IF NOT EXISTS ingestion_attempts INTEGER NOT NULL DEFAULT 0",
        ):
            cur.execute(ddl)


def report_tuple_from_api(
    report: dict[str, Any],
    guild: dict[str, Any],
    keys: dict[str, Any],
) -> tuple[Any, ...]:
    zone = (report.get("zone") or {}) if isinstance(report.get("zone"), dict) else {}
    owner = (report.get("owner") or {}) if isinstance(report.get("owner"), dict) else {}
    report_guild = (report.get("guild") or {}) if isinstance(report.get("guild"), dict) else {}
    log_source = report.get("_log_source") or "guild"
    return (
        report.get("code"),
        report_guild.get("id") or guild.get("id"),
        report_guild.get("name") or guild.get("name"),
        keys["server_region"],
        keys["server_slug"],
        report.get("title"),
        zone.get("name"),
        owner.get("name"),
        report.get("_owner_user_id") or owner.get("id"),
        log_source,
        report.get("visibility"),
        report.get("startTime"),
        report.get("endTime"),
    )


def upsert_report_catalog_row(
    conn: Any,
    report: dict[str, Any],
    guild: dict[str, Any],
    keys: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(REPORT_UPSERT_SQL, report_tuple_from_api(report, guild, keys))


def list_reports_pending_bronze(conn: Any, limit: int | None = None) -> list[str]:
    sql = f"""
        SELECT report_code
        FROM {FULL_REPORTS}
        WHERE bronze_synced_at IS NULL
        ORDER BY start_time_ms DESC NULLS LAST
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    with conn.cursor() as cur:
        cur.execute(sql)
        return [row[0] for row in cur.fetchall()]


def mark_report_bronze_synced(conn: Any, report_code: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {FULL_REPORTS}
            SET bronze_synced_at = NOW(),
                fights_fetched_at = COALESCE(fights_fetched_at, NOW()),
                last_error = NULL
            WHERE report_code = %s
            """,
            (report_code,),
        )
        cur.execute(
            f"""
            UPDATE {FULL_FIGHTS}
            SET stats_fetched_at = NOW()
            WHERE report_code = %s AND stats_fetched_at IS NULL
            """,
            (report_code,),
        )


def mark_report_bronze_error(conn: Any, report_code: str, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {FULL_REPORTS}
            SET last_error = %s,
                ingestion_attempts = ingestion_attempts + 1
            WHERE report_code = %s
            """,
            (error, report_code),
        )


def mark_report_fights_fetched(conn: Any, report_code: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE {FULL_REPORTS} SET fights_fetched_at = NOW() WHERE report_code = %s",
            (report_code,),
        )


def mark_fight_stats_fetched(conn: Any, report_code: str, fight_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {FULL_FIGHTS}
            SET stats_fetched_at = NOW()
            WHERE report_code = %s AND fight_id = %s
            """,
            (report_code, fight_id),
        )


def list_reports_pending_fights(conn: Any, limit: int | None = None) -> list[str]:
    sql = f"""
        SELECT report_code
        FROM {FULL_REPORTS}
        WHERE fights_fetched_at IS NULL
        ORDER BY start_time_ms DESC NULLS LAST
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    with conn.cursor() as cur:
        cur.execute(sql)
        return [row[0] for row in cur.fetchall()]


def list_fights_pending_stats(conn: Any, limit: int | None = None) -> list[tuple[str, int]]:
    sql = f"""
        SELECT report_code, fight_id
        FROM {FULL_FIGHTS}
        WHERE is_boss = TRUE AND stats_fetched_at IS NULL
        ORDER BY report_code, fight_id
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    with conn.cursor() as cur:
        cur.execute(sql)
        return [(row[0], row[1]) for row in cur.fetchall()]


def get_fight_bounds(conn: Any, report_code: str, fight_id: int) -> tuple[int, int] | None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT start_time_ms, end_time_ms
            FROM {FULL_FIGHTS}
            WHERE report_code = %s AND fight_id = %s
            """,
            (report_code, fight_id),
        )
        row = cur.fetchone()
    if not row or row[0] is None or row[1] is None:
        return None
    return int(row[0]), int(row[1])

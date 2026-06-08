"""Client Warcraft Logs API v2 (OAuth + GraphQL) — guilde Nightmares Asylum / générique."""

from __future__ import annotations

import base64
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any
from urllib.parse import unquote, urlparse

TOKEN_URL = "https://www.warcraftlogs.com/oauth/token"
API_URL = "https://www.warcraftlogs.com/api/v2/client"

DEFAULT_GUILD_URL = (
    "https://www.warcraftlogs.com/guild/eu/dalaran/nightmares%20asylum"
)

# Connexion Airflow : login = client_id, password = client_secret (type HTTP).
WCL_CONN_ID = "WCL_API"

REPORT_PAGE_SIZE = 100

USER_REPORTS_QUERY = """
query UserReports($userId: Int!, $limit: Int!, $page: Int!) {
  reportData {
    reports(userID: $userId, limit: $limit, page: $page) {
      total
      has_more_pages
      current_page
      last_page
      data {
        code
        title
        startTime
        endTime
        visibility
        zone { id name }
        owner { id name }
        guild { id name server { slug region { slug } } }
      }
    }
  }
}
"""

GUILD_REPORTS_QUERY = """
query GuildReports(
  $name: String!
  $serverSlug: String!
  $serverRegion: String!
  $limit: Int!
  $page: Int!
) {
  guildData {
    guild(name: $name, serverSlug: $serverSlug, serverRegion: $serverRegion) {
      id
      name
      faction { name }
      server { name slug }
    }
  }
  reportData {
    reports(
      guildName: $name
      guildServerSlug: $serverSlug
      guildServerRegion: $serverRegion
      limit: $limit
      page: $page
    ) {
      total
      has_more_pages
      current_page
      last_page
      data {
        code
        title
        startTime
        endTime
        zone { id name }
        owner { name }
      }
    }
  }
}
"""

REPORT_FIGHTS_QUERY = """
query ReportFights($code: String!) {
  reportData {
    report(code: $code) {
      code
      title
      startTime
      endTime
      fights {
        id
        encounterID
        name
        startTime
        endTime
        kill
        size
        difficulty
        bossPercentage
        keystoneLevel
        keystoneTime
      }
    }
  }
}
"""

FIGHT_TABLES_QUERY = """
query FightTables(
  $code: String!
  $startTime: Float!
  $endTime: Float!
) {
  reportData {
    report(code: $code) {
      damage: table(
        startTime: $startTime
        endTime: $endTime
        dataType: DamageDone
      )
      healing: table(
        startTime: $startTime
        endTime: $endTime
        dataType: Healing
      )
      damageTaken: table(
        startTime: $startTime
        endTime: $endTime
        dataType: DamageTaken
      )
      deaths: table(
        startTime: $startTime
        endTime: $endTime
        dataType: Deaths
      )
    }
  }
}
"""

TABLE_METRICS = {
    "damage": "dps",
    "healing": "hps",
    "damageTaken": "dtps",
    "deaths": "deaths",
}

_token_cache: dict[str, Any] = {}


def parse_guild_url(url: str) -> dict[str, str]:
    """Parse une URL type ``/guild/eu/dalaran/nightmares%20asylum``."""
    parts = urlparse(url).path.strip("/").split("/")
    if len(parts) < 4 or parts[0].lower() != "guild":
        raise ValueError(f"URL guilde Warcraft Logs invalide : {url}")
    return {
        "server_region": parts[1].upper(),
        "server_slug": parts[2],
        "guild_name": unquote("/".join(parts[3:])),
    }


def _airflow_variable(name: str, default: str = "") -> str:
    try:
        from airflow.sdk import Variable

        return str(Variable.get(name, default=default)).strip()
    except Exception:
        return default


def _parse_user_ids_csv(raw: str) -> list[int]:
    if not raw.strip():
        return []
    ids: list[int] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            ids.append(int(chunk))
        except ValueError as exc:
            raise ValueError(
                f"Liste d'IDs WCL invalide ({chunk!r}) : entiers séparés par des virgules."
            ) from exc
    return ids


def api_sleep_seconds() -> float:
    raw = _airflow_variable("wcl_api_sleep_seconds") or os.environ.get("WCL_API_SLEEP_SECONDS", "0.5")
    try:
        return max(0.0, float(raw.strip()))
    except ValueError:
        return 0.5


def max_report_pages() -> int | None:
    raw = _airflow_variable("wcl_max_report_pages") or os.environ.get("WCL_MAX_REPORT_PAGES", "")
    if not raw.strip():
        return None
    try:
        return max(1, int(raw.strip()))
    except ValueError:
        return None


def guild_url_from_env() -> str:
    """URL guilde : Variable Airflow ``wcl_guild_url`` ou fallback env/local."""
    raw = _airflow_variable("wcl_guild_url") or os.environ.get("WCL_GUILD_URL", DEFAULT_GUILD_URL)
    return raw.strip() or DEFAULT_GUILD_URL


def user_ids_from_env() -> list[int]:
    """IDs membres (logs perso publics) : Variable ``wcl_user_ids`` ou env."""
    raw = _airflow_variable("wcl_user_ids") or os.environ.get("WCL_USER_IDS", "")
    return _parse_user_ids_csv(raw)


def _wcl_credentials() -> tuple[str, str]:
    """OAuth client_credentials : connexion Airflow ``WCL_API`` (login + password)."""
    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(WCL_CONN_ID)
        client_id = (conn.login or "").strip()
        client_secret = (conn.password or "").strip()
        if client_id and client_secret:
            return client_id, client_secret
    except Exception:
        pass

    client_id = os.environ.get("WCL_CLIENT_ID", "").strip()
    client_secret = os.environ.get("WCL_CLIENT_SECRET", "").strip()
    if client_id and client_secret:
        return client_id, client_secret

    raise RuntimeError(
        f"Connexion Airflow '{WCL_CONN_ID}' requise (login=client_id, password=client_secret). "
        "Créer un client sur https://www.warcraftlogs.com/api/docs. "
        "Fallback local : WCL_CLIENT_ID / WCL_CLIENT_SECRET dans l'environnement."
    )


def fetch_access_token() -> str:
    """OAuth2 client_credentials avec cache mémoire."""
    cached = _token_cache.get("token")
    if cached and cached["expires_at"] > time.time():
        return cached["value"]

    client_id, client_secret = _wcl_credentials()
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    body = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode()
    req = urllib.request.Request(TOKEN_URL, data=body, method="POST")
    req.add_header("Authorization", f"Basic {basic}")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode())
    token = payload.get("access_token")
    if not token:
        raise RuntimeError(f"Token OAuth WCL absent : {payload}")
    expires_in = int(payload.get("expires_in", 3600))
    _token_cache["token"] = {
        "value": token,
        "expires_at": time.time() + max(60, expires_in - 60),
    }
    return token


def graphql_request(query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    token = fetch_access_token()
    payload = json.dumps({"query": query, "variables": variables or {}}).encode()
    req = urllib.request.Request(API_URL, data=payload, method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            body = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"Warcraft Logs HTTP {exc.code} : {detail}") from exc
    if body.get("errors"):
        raise RuntimeError(f"Warcraft Logs GraphQL : {body['errors']}")
    return body.get("data") or {}


def _throttle() -> None:
    delay = api_sleep_seconds()
    if delay:
        time.sleep(delay)


def _normalize_table_payload(table_data: Any) -> dict[str, Any]:
    if table_data is None:
        return {}
    if isinstance(table_data, str):
        try:
            table_data = json.loads(table_data)
        except json.JSONDecodeError:
            return {}
    return table_data if isinstance(table_data, dict) else {}


def _table_entries_block(table_data: Any) -> dict[str, Any]:
    """WCL renvoie ``{ data: { entries, totalTime } }`` ou parfois à plat."""
    payload = _normalize_table_payload(table_data)
    nested = payload.get("data")
    if isinstance(nested, dict):
        return nested
    return payload


def parse_table_entries(table_data: Any, metric: str) -> list[dict[str, Any]]:
    """Extrait les stats joueur depuis la réponse ``table`` WCL."""
    block = _table_entries_block(table_data)
    entries = block.get("entries") or []
    total_time = int(block.get("totalTime") or 0)
    rows: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        total = int(entry.get("total") or 0)
        active = int(entry.get("activeTime") or total_time or 0)
        rate = (total / active * 1000.0) if active > 0 else 0.0
        rows.append(
            {
                "player_name": entry.get("name") or "unknown",
                "player_id": entry.get("id"),
                "class_name": entry.get("type"),
                "spec_name": entry.get("spec"),
                "metric": metric,
                "total_amount": total,
                "active_time_ms": active,
                "rate_per_sec": rate,
                "extra": {
                    "guild": entry.get("guild"),
                    "item_level": entry.get("itemLevel"),
                    "talents": entry.get("talents"),
                },
            }
        )
    return rows


def fetch_guild_reports_page(
    guild_url: str,
    *,
    page: int = 1,
    limit: int = REPORT_PAGE_SIZE,
) -> dict[str, Any]:
    """Une page de reports publics de guilde."""
    keys = parse_guild_url(guild_url)
    variables = {
        "name": keys["guild_name"],
        "serverSlug": keys["server_slug"],
        "serverRegion": keys["server_region"],
        "limit": min(max(limit, 1), REPORT_PAGE_SIZE),
        "page": max(page, 1),
    }
    data = graphql_request(GUILD_REPORTS_QUERY, variables)
    guild = (data.get("guildData") or {}).get("guild")
    reports_block = (data.get("reportData") or {}).get("reports") or {}
    return {
        "guild": guild,
        "reports": reports_block.get("data") or [],
        "reports_total": reports_block.get("total"),
        "has_more_pages": bool(reports_block.get("has_more_pages")),
        "current_page": reports_block.get("current_page"),
        "last_page": reports_block.get("last_page"),
        "query": keys,
    }


def fetch_guild_reports(
    guild_url: str = DEFAULT_GUILD_URL,
    limit: int = 25,
) -> dict[str, Any]:
    """Récupère métadonnées guilde + première page de reports (compat script test)."""
    payload = fetch_guild_reports_page(guild_url, page=1, limit=limit)
    return payload


def fetch_user_reports_page(
    user_id: int,
    *,
    page: int = 1,
    limit: int = REPORT_PAGE_SIZE,
) -> dict[str, Any]:
    """Une page de reports publics des logs personnels d'un utilisateur WCL."""
    variables = {
        "userId": int(user_id),
        "limit": min(max(limit, 1), REPORT_PAGE_SIZE),
        "page": max(page, 1),
    }
    data = graphql_request(USER_REPORTS_QUERY, variables)
    reports_block = (data.get("reportData") or {}).get("reports") or {}
    return {
        "user_id": user_id,
        "reports": reports_block.get("data") or [],
        "reports_total": reports_block.get("total"),
        "has_more_pages": bool(reports_block.get("has_more_pages")),
        "current_page": reports_block.get("current_page"),
        "last_page": reports_block.get("last_page"),
    }


def fetch_all_user_reports(
    user_id: int,
    *,
    page_size: int = REPORT_PAGE_SIZE,
    max_pages: int | None = None,
) -> dict[str, Any]:
    """Tous les reports publics des logs personnels d'un membre."""
    cap = max_pages if max_pages is not None else max_report_pages()
    all_reports: list[dict[str, Any]] = []
    page = 1
    total = 0

    while True:
        payload = fetch_user_reports_page(user_id, page=page, limit=page_size)
        batch = payload.get("reports") or []
        all_reports.extend(batch)
        total = int(payload.get("reports_total") or len(all_reports))
        has_more = bool(payload.get("has_more_pages"))
        print(f"WCL user {user_id} page {page} : {len(batch)} reports (total API {total})")
        if not has_more:
            break
        if cap and page >= cap:
            print(f"Limite WCL_MAX_REPORT_PAGES={cap} atteinte pour user {user_id}.")
            break
        page += 1
        _throttle()

    return {
        "user_id": user_id,
        "reports": all_reports,
        "reports_total": total,
    }


def fetch_all_guild_and_member_reports(
    guild_url: str | None = None,
    *,
    user_ids: list[int] | None = None,
    page_size: int = REPORT_PAGE_SIZE,
    max_pages: int | None = None,
) -> dict[str, Any]:
    """Union dédupliquée : logs guilde + logs personnels publics des membres."""
    guild_payload = fetch_all_guild_reports(guild_url, page_size=page_size, max_pages=max_pages)
    merged: dict[str, dict[str, Any]] = {}

    guild = guild_payload.get("guild") or {}
    keys = guild_payload.get("query") or {}
    for report in guild_payload.get("reports") or []:
        code = report.get("code")
        if code:
            merged[code] = {**report, "_log_source": "guild", "_owner_user_id": None}

    member_ids = user_ids if user_ids is not None else user_ids_from_env()
    for user_id in member_ids:
        user_payload = fetch_all_user_reports(user_id, page_size=page_size, max_pages=max_pages)
        for report in user_payload.get("reports") or []:
            code = report.get("code")
            if not code or code in merged:
                continue
            owner = (report.get("owner") or {}) if isinstance(report.get("owner"), dict) else {}
            merged[code] = {
                **report,
                "_log_source": "personal",
                "_owner_user_id": owner.get("id") or user_id,
            }

    reports = list(merged.values())
    guild_n = sum(1 for r in reports if r.get("_log_source") == "guild")
    personal_n = sum(1 for r in reports if r.get("_log_source") == "personal")
    return {
        "guild": guild,
        "reports": reports,
        "reports_total": len(reports),
        "query": keys,
        "member_user_ids": member_ids,
        "guild_reports_count": guild_n,
        "personal_reports_count": personal_n,
    }


def fetch_all_guild_reports(
    guild_url: str | None = None,
    *,
    page_size: int = REPORT_PAGE_SIZE,
    max_pages: int | None = None,
) -> dict[str, Any]:
    """Récupère **tous** les reports publics (pagination API)."""
    url = guild_url or guild_url_from_env()
    cap = max_pages if max_pages is not None else max_report_pages()
    all_reports: list[dict[str, Any]] = []
    guild: dict[str, Any] | None = None
    keys: dict[str, str] | None = None
    page = 1
    total = 0

    while True:
        payload = fetch_guild_reports_page(url, page=page, limit=page_size)
        if guild is None:
            guild = payload.get("guild")
            keys = payload["query"]
        batch = payload.get("reports") or []
        all_reports.extend(batch)
        total = int(payload.get("reports_total") or len(all_reports))
        has_more = bool(payload.get("has_more_pages"))
        print(f"WCL reports page {page} : {len(batch)} lignes (total API {total})")
        if not has_more:
            break
        if cap and page >= cap:
            print(f"Limite WCL_MAX_REPORT_PAGES={cap} atteinte.")
            break
        page += 1
        _throttle()

    return {
        "guild": guild,
        "reports": all_reports,
        "reports_total": total,
        "query": keys or parse_guild_url(url),
    }


def fetch_report_fights(report_code: str) -> dict[str, Any]:
    """Fights d'un report (boss + trash)."""
    _throttle()
    data = graphql_request(REPORT_FIGHTS_QUERY, {"code": report_code})
    report = (data.get("reportData") or {}).get("report")
    if not report:
        raise RuntimeError(f"Report WCL introuvable ou privé : {report_code}")
    return report


def fetch_fight_tables(
    report_code: str,
    start_time_ms: int,
    end_time_ms: int,
) -> dict[str, list[dict[str, Any]]]:
    """Stats agrégées DPS/HPS/DTPS/morts pour une plage de combat."""
    if start_time_ms >= end_time_ms:
        return {}
    _throttle()
    data = graphql_request(
        FIGHT_TABLES_QUERY,
        {
            "code": report_code,
            "startTime": float(start_time_ms),
            "endTime": float(end_time_ms),
        },
    )
    report = (data.get("reportData") or {}).get("report") or {}
    parsed: dict[str, list[dict[str, Any]]] = {}
    for field, metric in TABLE_METRICS.items():
        parsed[metric] = parse_table_entries(report.get(field), metric)
    return parsed


def fight_rows_from_report(report_code: str, report: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalise les fights API en lignes prêtes pour le bronze Delta."""
    rows: list[dict[str, Any]] = []
    for fight in report.get("fights") or []:
        if not isinstance(fight, dict):
            continue
        start = fight.get("startTime")
        end = fight.get("endTime")
        duration = None
        if start is not None and end is not None:
            duration = int(end) - int(start)
        encounter_id = int(fight.get("encounterID") or 0)
        rows.append(
            {
                "report_code": report_code,
                "fight_id": int(fight.get("id")),
                "encounter_id": encounter_id,
                "fight_name": fight.get("name"),
                "start_time_ms": start,
                "end_time_ms": end,
                "duration_ms": duration,
                "kill": fight.get("kill"),
                "difficulty": fight.get("difficulty"),
                "size": fight.get("size"),
                "boss_percentage": fight.get("bossPercentage"),
                "keystone_level": fight.get("keystoneLevel"),
                "keystone_time_ms": fight.get("keystoneTime"),
                "is_boss": encounter_id != 0,
            }
        )
    return rows

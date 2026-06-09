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
ROSTER_PAGE_SIZE = 100

WOW_CLASS_NAMES: dict[int, str] = {
    1: "Warrior",
    2: "Paladin",
    3: "Hunter",
    4: "Rogue",
    5: "Priest",
    6: "Death Knight",
    7: "Shaman",
    8: "Mage",
    9: "Warlock",
    10: "Monk",
    11: "Druid",
    12: "Demon Hunter",
    13: "Evoker",
}

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

GUILD_MEMBERS_QUERY = """
query GuildMembers(
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
      server { name slug region { slug } }
      members(limit: $limit, page: $page) {
        total
        has_more_pages
        current_page
        last_page
        data {
          id
          canonicalID
          name
          classID
          level
          guildRank
          server { slug region { slug } }
        }
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
      owner {
        id
        name
      }
      zone {
        id
        name
      }
      guild {
        id
        name
      }
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

# Tous les TableDataType WCL v2 (https://www.warcraftlogs.com/v2-api-docs/warcraft/tabledatatype.doc.html)
WCL_TABLE_DATA_TYPES: tuple[str, ...] = (
    "Summary",
    "Buffs",
    "Casts",
    "DamageDone",
    "DamageTaken",
    "Deaths",
    "Debuffs",
    "Dispels",
    "Healing",
    "Interrupts",
    "Resources",
    "Summons",
    "Survivability",
    "Threat",
)

# Alias rétrocompat (requêtes ClickHouse existantes)
TABLE_METRIC_ALIASES: dict[str, str] = {
    "DamageDone": "dps",
    "Healing": "hps",
    "DamageTaken": "dtps",
    "Deaths": "deaths",
    "Summary": "summary",
    "Buffs": "buffs",
    "Casts": "casts",
    "Debuffs": "debuffs",
    "Dispels": "dispels",
    "Interrupts": "interrupts",
    "Resources": "resources",
    "Summons": "summons",
    "Survivability": "survivability",
    "Threat": "threat",
}

# viewBy Source pour buffs/casts par joueur ; Default pour DPS/soins (entries classiques).
TABLE_VIEW_BY: dict[str, str] = {
    "Buffs": "Source",
    "Debuffs": "Source",
    "Casts": "Source",
    "Threat": "Source",
    "Resources": "Source",
}

FIGHT_TABLE_QUERY = """
query FightTable(
  $code: String!
  $startTime: Float!
  $endTime: Float!
  $dataType: TableDataType!
  $viewBy: ViewType!
) {
  reportData {
    report(code: $code) {
      table(
        startTime: $startTime
        endTime: $endTime
        dataType: $dataType
        viewBy: $viewBy
      )
    }
  }
}
"""

RATE_LIMIT_QUERY = """
query WclRateLimit {
  rateLimitData {
    limitPerHour
    pointsSpentThisHour
    pointsResetIn
  }
}
"""

_token_cache: dict[str, Any] = {}
_rate_limit_cache: dict[str, Any] = {}
_throttle_counter = 0


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
    raw = _airflow_variable("wcl_api_sleep_seconds") or os.environ.get("WCL_API_SLEEP_SECONDS", "0.2")
    try:
        return max(0.0, float(raw.strip()))
    except ValueError:
        return 0.2


def adaptive_rate_limit_enabled() -> bool:
    raw = _airflow_variable("wcl_adaptive_rate_limit") or os.environ.get(
        "WCL_ADAPTIVE_RATE_LIMIT", "true"
    )
    return raw.strip().lower() not in {"0", "false", "no", "off"}


def rate_limit_check_interval() -> int:
    raw = _airflow_variable("wcl_rate_limit_check_interval") or os.environ.get(
        "WCL_RATE_LIMIT_CHECK_INTERVAL", "10"
    )
    try:
        return max(1, int(raw.strip()))
    except ValueError:
        return 10


def points_per_report_estimate() -> int:
    """Fallback si aucun coût mesuré en bronze (voir ``median_ingest_points``)."""
    raw = _airflow_variable("wcl_points_per_report_estimate") or os.environ.get(
        "WCL_POINTS_PER_REPORT_ESTIMATE", "1000"
    )
    try:
        return max(200, int(raw.strip()))
    except ValueError:
        return 1000


def quota_reserve_fraction() -> float:
    raw = _airflow_variable("wcl_quota_reserve_fraction") or os.environ.get(
        "WCL_QUOTA_RESERVE_FRACTION", "0.05"
    )
    try:
        return max(0.0, min(0.5, float(raw.strip())))
    except ValueError:
        return 0.05


def points_per_request_estimate() -> int:
    raw = _airflow_variable("wcl_points_per_request_estimate") or os.environ.get(
        "WCL_POINTS_PER_REQUEST_ESTIMATE", "2"
    )
    try:
        return max(1, int(raw.strip()))
    except ValueError:
        return 2


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


def refresh_rate_limit_data(*, force: bool = False) -> dict[str, int] | None:
    """Lit ``rateLimitData`` WCL (quota horaire en points)."""
    now = time.time()
    cached = _rate_limit_cache.get("data")
    if (
        not force
        and isinstance(cached, dict)
        and now - float(_rate_limit_cache.get("fetched_at", 0)) < 30.0
    ):
        return cached
    try:
        data = graphql_request(RATE_LIMIT_QUERY)
        block = data.get("rateLimitData") or {}
        parsed = {
            "limitPerHour": int(block.get("limitPerHour") or 0),
            "pointsSpentThisHour": int(block.get("pointsSpentThisHour") or 0),
            "pointsResetIn": max(0, int(block.get("pointsResetIn") or 0)),
        }
        _rate_limit_cache["data"] = parsed
        _rate_limit_cache["fetched_at"] = now
        return parsed
    except Exception as exc:
        print(f"WCL rateLimitData indisponible : {exc}")
        return cached if isinstance(cached, dict) else None


def rate_limit_snapshot() -> dict[str, int] | None:
    data = _rate_limit_cache.get("data")
    return data if isinstance(data, dict) else None


def estimated_report_cost() -> int:
    """Coût estimé : médiane mesurée × 1,15 ou fallback variable."""
    try:
        from warcraftlogs_lake import median_ingest_points

        measured = median_ingest_points()
    except Exception:
        measured = None
    if measured and measured > 0:
        return max(200, int(measured * 1.15))
    return points_per_report_estimate()


def quota_allows_next_report() -> tuple[bool, str]:
    """Vérifie s'il reste assez de points pour tenter un report (arrêt propre sinon)."""
    if not adaptive_rate_limit_enabled():
        return True, "throttle adaptatif désactivé"
    snapshot = rate_limit_snapshot() or refresh_rate_limit_data(force=True)
    if not snapshot:
        return True, "quota API indisponible"
    limit_h = snapshot["limitPerHour"]
    spent = snapshot["pointsSpentThisHour"]
    reset_in = snapshot["pointsResetIn"]
    if limit_h <= 0:
        return True, "limite horaire inconnue"
    remaining = max(0, limit_h - spent)
    reserve = max(50, int(limit_h * quota_reserve_fraction()))
    cost = estimated_report_cost()
    if remaining < cost + reserve:
        return (
            False,
            f"quota {spent}/{limit_h} pts, reste {remaining}, "
            f"besoin ~{cost}+{reserve} (reset {reset_in}s)",
        )
    return (
        True,
        f"quota {spent}/{limit_h} pts, estimé ~{cost}/report (reset {reset_in}s)",
    )


def measure_report_points_delta(spent_before: int | None) -> int | None:
    """Points consommés depuis ``spent_before`` (``rateLimitData`` API)."""
    if spent_before is None:
        return None
    snapshot = refresh_rate_limit_data(force=True)
    if not snapshot:
        return None
    return max(0, snapshot["pointsSpentThisHour"] - spent_before)


def _adaptive_sleep_seconds() -> float | None:
    if not adaptive_rate_limit_enabled():
        return None
    snapshot = rate_limit_snapshot()
    if not snapshot:
        return None
    limit_h = snapshot["limitPerHour"]
    spent = snapshot["pointsSpentThisHour"]
    reset_in = max(1, snapshot["pointsResetIn"])
    if limit_h <= 0:
        return None
    remaining = max(0, limit_h - spent)
    if remaining <= 0:
        return min(60.0, float(reset_in) / 5.0)
    pts_budget = remaining * 0.85
    pts_per_sec = pts_budget / float(reset_in)
    pts_per_req = float(points_per_request_estimate())
    req_per_sec = pts_per_sec / pts_per_req
    if req_per_sec <= 0.05:
        return min(60.0, float(reset_in) / 5.0)
    sleep = 1.0 / req_per_sec
    return max(0.12, min(sleep, 3.0))


def _compute_sleep_seconds() -> float:
    static = api_sleep_seconds()
    adaptive = _adaptive_sleep_seconds()
    if adaptive is None:
        return static
    if adaptive > static:
        return adaptive
    return min(static, adaptive) if static > 0 else adaptive


def _throttle() -> None:
    global _throttle_counter
    _throttle_counter += 1
    if adaptive_rate_limit_enabled() and _throttle_counter % rate_limit_check_interval() == 0:
        refresh_rate_limit_data()
    delay = _compute_sleep_seconds()
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


def metric_slug_for_data_type(data_type: str) -> str:
    return TABLE_METRIC_ALIASES.get(data_type, data_type.lower())


def view_by_for_data_type(data_type: str) -> str:
    return TABLE_VIEW_BY.get(data_type, "Default")


def _first_spec_name(player: dict[str, Any]) -> Any:
    specs = player.get("specs") or []
    if specs and isinstance(specs[0], dict):
        return specs[0].get("spec")
    return player.get("spec")


def _parse_composition_rows(
    block: dict[str, Any],
    total_time: int,
) -> list[dict[str, Any]]:
    """Summary WCL : roster dans ``data.composition`` (pas ``entries``)."""
    rows: list[dict[str, Any]] = []
    item_level = block.get("itemLevel")
    for player in block.get("composition") or []:
        if not isinstance(player, dict):
            continue
        extra = dict(player)
        if item_level is not None:
            extra.setdefault("itemLevel", item_level)
        rows.append(
            _stat_row_from_entry(
                extra,
                "summary",
                player_name=str(player.get("name") or "unknown"),
                player_id=player.get("id"),
                class_name=player.get("type"),
                spec_name=_first_spec_name(player),
                total_time=total_time,
            )
        )
    return rows


def _parse_auras_rows(
    block: dict[str, Any],
    metric: str,
    total_time: int,
    *,
    player_name: str = "unknown",
    player_id: Any = None,
    class_name: Any = None,
    spec_name: Any = None,
) -> list[dict[str, Any]]:
    """Buffs/Debuffs WCL : ``data.auras`` (uptime par aura, parfois sans joueur)."""
    rows: list[dict[str, Any]] = []
    for aura in block.get("auras") or []:
        if not isinstance(aura, dict):
            continue
        rows.append(
            _stat_row_from_entry(
                aura,
                metric,
                player_name=player_name,
                player_id=player_id,
                class_name=class_name,
                spec_name=spec_name,
                total_time=total_time,
            )
        )
    return rows


def _entry_label(entry: dict[str, Any]) -> str:
    for key in ("name", "abilityName", "targetName"):
        value = entry.get(key)
        if value:
            return str(value)
    if entry.get("id") is not None:
        return str(entry["id"])
    return "unknown"


def _stat_row_from_entry(
    entry: dict[str, Any],
    metric: str,
    *,
    player_name: str,
    player_id: Any,
    class_name: Any,
    spec_name: Any,
    total_time: int,
) -> dict[str, Any]:
    total = int(entry.get("total") or entry.get("amount") or entry.get("count") or 0)
    active = int(
        entry.get("activeTime")
        or entry.get("totalUptime")
        or entry.get("uptime")
        or total
        or total_time
        or 0
    )
    rate = (total / active * 1000.0) if active > 0 else 0.0
    extra = dict(entry)
    extra.setdefault("playerName", player_name)
    if player_id is not None:
        extra.setdefault("playerId", player_id)
    return {
        "player_name": player_name,
        "player_id": player_id,
        "class_name": class_name,
        "spec_name": spec_name,
        "metric": metric,
        "total_amount": total,
        "active_time_ms": active,
        "rate_per_sec": rate,
        "extra": extra,
    }


def parse_table_entries(table_data: Any, metric: str) -> list[dict[str, Any]]:
    """Extrait les lignes depuis la réponse ``table`` WCL (formats entries, composition, auras)."""
    block = _table_entries_block(table_data)
    entries = block.get("entries") or []
    total_time = int(block.get("totalTime") or 0)
    rows: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        subentries = entry.get("subentries") or entry.get("subEntries")
        if subentries:
            player_name = str(entry.get("name") or entry.get("playerName") or "unknown")
            player_id = entry.get("id")
            class_name = entry.get("type")
            spec_name = entry.get("spec")
            for sub in subentries:
                if not isinstance(sub, dict):
                    continue
                rows.append(
                    _stat_row_from_entry(
                        sub,
                        metric,
                        player_name=player_name,
                        player_id=player_id,
                        class_name=class_name,
                        spec_name=spec_name,
                        total_time=total_time,
                    )
                )
            continue
        rows.append(
            _stat_row_from_entry(
                entry,
                metric,
                player_name=_entry_label(entry),
                player_id=entry.get("id"),
                class_name=entry.get("type"),
                spec_name=entry.get("spec"),
                total_time=total_time,
            )
        )

    if metric == "summary":
        rows.extend(_parse_composition_rows(block, total_time))

    if metric in ("buffs", "debuffs"):
        if rows:
            # entries + subentries (viewBy Source) : compléter si auras au niveau joueur
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                subentries = entry.get("subentries") or entry.get("subEntries")
                if subentries:
                    continue
                nested_auras = entry.get("auras")
                if nested_auras:
                    rows.extend(
                        _parse_auras_rows(
                            {"auras": nested_auras},
                            metric,
                            total_time,
                            player_name=str(entry.get("name") or "unknown"),
                            player_id=entry.get("id"),
                            class_name=entry.get("type"),
                            spec_name=entry.get("spec"),
                        )
                    )
        else:
            rows.extend(_parse_auras_rows(block, metric, total_time))

    return rows


def _wow_class_name(class_id: Any) -> str | None:
    try:
        return WOW_CLASS_NAMES.get(int(class_id))
    except (TypeError, ValueError):
        return None


def guild_roster_rows_from_api(
    guild: dict[str, Any],
    keys: dict[str, Any],
    members: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Normalise le roster API WCL pour bronze Delta."""
    guild_id = guild.get("id")
    guild_name = guild.get("name") or keys.get("guild_name")
    rows: list[dict[str, Any]] = []
    for member in members:
        if not isinstance(member, dict):
            continue
        name = str(member.get("name") or "").strip()
        if not name:
            continue
        canonical_id = member.get("canonicalID")
        wcl_character_id = member.get("id")
        player_guid = canonical_id or wcl_character_id
        server = (member.get("server") or {}) if isinstance(member.get("server"), dict) else {}
        region = (server.get("region") or {}) if isinstance(server.get("region"), dict) else {}
        class_id = member.get("classID")
        rows.append(
            {
                "guild_id": guild_id,
                "guild_name": guild_name,
                "server_region": keys.get("server_region") or region.get("slug"),
                "server_slug": keys.get("server_slug") or server.get("slug"),
                "wcl_character_id": wcl_character_id,
                "canonical_id": canonical_id,
                "player_guid": player_guid,
                "character_name": name,
                "class_id": class_id,
                "class_name": _wow_class_name(class_id),
                "character_level": member.get("level"),
                "guild_rank": member.get("guildRank"),
            }
        )
    return rows


def fetch_guild_members_page(
    guild_url: str,
    *,
    page: int = 1,
    limit: int = ROSTER_PAGE_SIZE,
) -> dict[str, Any]:
    """Une page du roster guilde (membres WCL vérifiés)."""
    keys = parse_guild_url(guild_url)
    variables = {
        "name": keys["guild_name"],
        "serverSlug": keys["server_slug"],
        "serverRegion": keys["server_region"],
        "limit": min(max(limit, 1), ROSTER_PAGE_SIZE),
        "page": max(page, 1),
    }
    data = graphql_request(GUILD_MEMBERS_QUERY, variables)
    guild = (data.get("guildData") or {}).get("guild") or {}
    members_block = guild.get("members") or {}
    return {
        "guild": guild,
        "members": members_block.get("data") or [],
        "members_total": members_block.get("total"),
        "has_more_pages": bool(members_block.get("has_more_pages")),
        "current_page": members_block.get("current_page"),
        "last_page": members_block.get("last_page"),
        "query": keys,
    }


def fetch_all_guild_members(
    guild_url: str | None = None,
    *,
    page_size: int = ROSTER_PAGE_SIZE,
    max_pages: int | None = None,
) -> dict[str, Any]:
    """Roster complet guilde (pagination API ``guild.members``)."""
    url = guild_url or guild_url_from_env()
    keys = parse_guild_url(url)
    guild: dict[str, Any] | None = None
    members: list[dict[str, Any]] = []
    page = 1
    last_page: int | None = None

    while True:
        payload = fetch_guild_members_page(url, page=page, limit=page_size)
        if guild is None:
            guild = payload.get("guild") or {}
        members.extend(payload.get("members") or [])
        last_page = payload.get("last_page") or last_page
        if not payload.get("has_more_pages"):
            break
        page += 1
        if max_pages is not None and page > max_pages:
            break
        if last_page is not None and page > int(last_page):
            break

    rows = guild_roster_rows_from_api(guild or {}, keys, members)
    return {
        "guild": guild,
        "query": keys,
        "members_total": len(members),
        "rows": rows,
    }


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


def fetch_fight_table_raw(
    report_code: str,
    start_time_ms: int,
    end_time_ms: int,
    data_type: str,
) -> Any:
    """Une table WCL brute pour une plage de combat et un ``TableDataType``."""
    if start_time_ms >= end_time_ms:
        return None
    _throttle()
    data = graphql_request(
        FIGHT_TABLE_QUERY,
        {
            "code": report_code,
            "startTime": float(start_time_ms),
            "endTime": float(end_time_ms),
            "dataType": data_type,
            "viewBy": view_by_for_data_type(data_type),
        },
    )
    report = (data.get("reportData") or {}).get("report") or {}
    return report.get("table")


def fetch_fight_tables(
    report_code: str,
    start_time_ms: int,
    end_time_ms: int,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    """Toutes les tables WCL pour un combat : lignes parsées + JSON brut par dataType."""
    if start_time_ms >= end_time_ms:
        return {}, {}

    parsed: dict[str, list[dict[str, Any]]] = {}
    raw_by_type: dict[str, Any] = {}
    for data_type in WCL_TABLE_DATA_TYPES:
        metric = metric_slug_for_data_type(data_type)
        table_data = fetch_fight_table_raw(report_code, start_time_ms, end_time_ms, data_type)
        raw_by_type[data_type] = table_data
        parsed[metric] = parse_table_entries(table_data, metric)
    return parsed, raw_by_type


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

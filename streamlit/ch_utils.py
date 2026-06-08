"""Requêtes ClickHouse HTTP pour Streamlit (table gold WCL)."""

from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request

import pandas as pd
import streamlit as st


def _cfg(name: str, default: str = "") -> str:
    if name in st.secrets:
        return str(st.secrets[name])
    return os.getenv(name, default)


def _esc(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


@st.cache_data(ttl=120)
def ch_query(sql: str, database: str = "gold") -> pd.DataFrame:
    host = _cfg("CLICKHOUSE_HOST", "clickhouse")
    port = _cfg("CLICKHOUSE_PORT", "8123")
    user = _cfg("CLICKHOUSE_USER", "default")
    password = _cfg("CLICKHOUSE_PASSWORD", "")

    url = f"http://{host}:{port}/?database={database}"
    headers: dict[str, str] = {}
    if user or password:
        token = base64.b64encode(f"{user}:{password}".encode()).decode("ascii")
        headers["Authorization"] = f"Basic {token}"

    body = f"{sql.strip()}\nFORMAT JSON".encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse HTTP {exc.code}: {detail}") from exc

    rows = payload.get("data") or []
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def ch_scalar(sql: str, database: str = "gold") -> int:
    df = ch_query(sql, database=database)
    if df.empty:
        return 0
    return int(df.iloc[0, 0])

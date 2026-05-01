import json
import os
import re
import ast
from pathlib import Path

import altair as alt
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import requests
import seaborn as sns
import streamlit as st


DOC_PATH_CANDIDATES = [
    Path(__file__).resolve().parents[1] / "data" / "llm-viz-base-columns.json",
    Path(__file__).resolve().parents[2] / ".local" / "llm-viz-base-columns.json",
]

GROQ_MODEL_PRICING_PER_1M = {
    "openai/gpt-oss-120b": {"input_usd": 0.15, "output_usd": 0.60},
    "llama-3.3-70b-versatile": {"input_usd": 0.59, "output_usd": 0.79},
}


def _cfg(name: str, default: str = "") -> str:
    if name in st.secrets:
        return str(st.secrets[name])
    return os.getenv(name, default)


def _load_doc() -> dict:
    for path in DOC_PATH_CANDIDATES:
        if path.exists():
            with path.open("r", encoding="utf-8") as file:
                return json.load(file)
    searched = ", ".join(str(path) for path in DOC_PATH_CANDIDATES)
    raise FileNotFoundError(f"Aucun JSON trouve. Chemins testes: {searched}")


def _build_sql_prompt(user_question: str, doc: dict) -> str:
    allowed = [row["row_name"] for row in doc["rows"]]
    columns_doc = "\n".join(
        [
            f"- {row['row_name']} ({row.get('row_type', 'unknown')}): {row.get('row_doc', '')}"
            for row in doc["rows"]
        ]
    )
    minimal_rules = [
        "n'utiliser que la table autorisee",
        "n'utiliser que les colonnes autorisees",
        "retourner uniquement du SQL PostgreSQL valide en SELECT",
    ]
    return f"""Tu es un assistant SQL PostgreSQL.
Ta mission: produire une requete SQL de visualisation depuis la question utilisateur.

Table autorisee:
- {doc["table"]}

Colonnes autorisees:
- {", ".join(allowed)}

Documentation des colonnes:
{columns_doc}

Regles strictes:
- {chr(10).join(f"- {rule}" for rule in minimal_rules)}
- Compat PostgreSQL: pour un ecart en jours, utiliser `DATE_PART('day', ...)` ou une soustraction de dates cast en `date`
- Ne jamais caster directement un `interval` en `int`

Format de sortie OBLIGATOIRE (JSON strict):
{{
  "sql": "SELECT ...",
  "why": "Explication courte"
}}

Question utilisateur:
{user_question}
"""


def _build_sql_repair_prompt(
    previous_sql: str,
    error_message: str,
    doc: dict,
) -> str:
    allowed = [row["row_name"] for row in doc["rows"]]
    return f"""Corrige la requete SQL PostgreSQL suivante qui echoue a l'execution.
Retourne uniquement un JSON strict:
{{
  "sql": "SELECT ...",
  "why": "Explication courte"
}}

Table autorisee: {doc["table"]}
Colonnes autorisees: {", ".join(allowed)}

Regles de correction OBLIGATOIRES:
- retourner du SQL PostgreSQL executable immediatement
- corriger exactement l'erreur remontee, sans en introduire une autre
- si erreur de type "must appear in the GROUP BY clause or be used in an aggregate function":
  - toute colonne non agregee du SELECT doit etre dans GROUP BY
  - sinon il faut l'agreger (MAX/MIN/COUNT/SUM selon le contexte)
  - cas typique avec CROSS JOIN d'une date globale: utiliser `MAX(alias.colonne)` dans l'expression
- ne pas modifier les noms de colonnes autorisees
- ne pas retourner de texte hors JSON

Erreur observee:
{error_message}

SQL a corriger:
{previous_sql}
"""


def _build_viz_prompt(
    user_question: str,
    sql_query: str,
    result_columns: str,
) -> str:
    return f"""Tu es un assistant Python Streamlit specialise data-viz.
Genere uniquement du code de visualisation Streamlit.

Contraintes:
- pas de SQL
- pas de texte hors code
- pas de colonne inventee
- libs: streamlit, pandas, numpy, altair, plotly, seaborn, matplotlib
- choisir les imports en fonction des types de colonnes disponibles, et importer uniquement le strict necessaire
- garder `if df.empty`

SQL valide deja retenu:
{sql_query}

Schema du resultat SQL (colonnes + types), sans data:
{result_columns}

Attendu:
- code Python executable uniquement

Question utilisateur:
{user_question}
"""


def _build_viz_repair_prompt(
    previous_code: str,
    error_message: str,
    result_columns: str,
) -> str:
    return f"""Corrige le code Python suivant pour Streamlit.
Le code actuel echoue a l'execution.

Contraintes globales:
- Retourner uniquement du code Python executable
- N'utiliser que: streamlit, pandas, numpy, altair, plotly, seaborn, matplotlib
- Ne pas inventer de colonnes
- Utiliser uniquement ce schema de resultat: {result_columns}
- Garder une gestion `if df.empty`

Erreur observee:
{error_message}

Code a corriger:
```python
{previous_code}
```
"""


def _extract_json_block(text: str) -> dict:
    raw = text.strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        if len(lines) >= 3:
            raw = "\n".join(lines[1:-1]).strip()
    return json.loads(raw)


def _extract_python_code(text: str) -> str:
    raw = text.strip()
    fenced = re.search(r"```(?:python)?\s*([\s\S]*?)```", raw, flags=re.IGNORECASE)
    if fenced:
        return fenced.group(1).strip()
    return raw


def _assert_safe_viz_code(viz_code: str) -> None:
    allowed_imports = {"streamlit", "pandas", "numpy", "altair", "plotly", "seaborn", "matplotlib"}
    forbidden_calls = {"exec", "eval", "__import__", "open"}

    tree = ast.parse(viz_code)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                module_name = alias.name.split(".")[0]
                if module_name not in allowed_imports:
                    raise ValueError(f"Import non autorise: {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            module_name = (node.module or "").split(".")[0]
            if module_name not in allowed_imports:
                raise ValueError(f"ImportFrom non autorise: {node.module}")
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in forbidden_calls:
                raise ValueError(f"Appel interdit detecte: {node.func.id}")


def _normalize_usage(payload: dict) -> dict[str, int]:
    usage = payload.get("usage") or {}
    prompt_tokens = int(usage.get("prompt_tokens", 0) or 0)
    completion_tokens = int(usage.get("completion_tokens", 0) or 0)
    total_tokens = int(usage.get("total_tokens", prompt_tokens + completion_tokens) or 0)
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
    }


def _estimate_cost_usd(model: str, usage: dict[str, int]) -> float | None:
    pricing = GROQ_MODEL_PRICING_PER_1M.get(model)
    if not pricing:
        return None
    input_cost = (usage.get("prompt_tokens", 0) / 1_000_000) * pricing["input_usd"]
    output_cost = (usage.get("completion_tokens", 0) / 1_000_000) * pricing["output_usd"]
    return input_cost + output_cost


def _call_grok(prompt: str, model: str) -> tuple[str, dict[str, int]]:
    api_key = _cfg("GROQ_API_KEY", "")
    if not api_key:
        raise ValueError("GROQ_API_KEY manquante dans secrets/env.")
    response = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "temperature": 0.1,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=90,
    )
    response.raise_for_status()
    payload = response.json()
    content = payload["choices"][0]["message"]["content"]
    usage = _normalize_usage(payload)
    return content, usage


def _assert_safe_select_sql(sql_query: str, doc: dict) -> None:
    lower = sql_query.strip().lower()
    forbidden = [" delete ", " update ", " insert ", " drop ", " alter ", " truncate "]
    normalized = f" {lower} "
    if not (lower.startswith("select") or lower.startswith("with ")):
        raise ValueError("La requete doit commencer par SELECT ou WITH (CTE).")
    if lower.startswith("with ") and " select " not in normalized:
        raise ValueError("CTE invalide: la requete WITH doit contenir un SELECT final.")
    if any(token in normalized for token in forbidden):
        raise ValueError("Requete interdite: operation non-SELECT detectee.")
    if doc["table"].lower() not in lower:
        raise ValueError(f"La table autorisee `{doc['table']}` est absente de la requete.")


@st.cache_data(ttl=60)
def _run_sql_query(sql_query: str) -> pd.DataFrame:
    connection = psycopg2.connect(
        host=_cfg("APP_DB_HOST", "postgres-db"),
        port=int(_cfg("APP_DB_PORT", "5432")),
        user=_cfg("APP_DB_USER", ""),
        password=_cfg("APP_DB_PASSWORD", ""),
        dbname=_cfg("APP_DB_NAME", "rfm"),
    )
    try:
        return pd.read_sql_query(sql_query, connection)
    finally:
        connection.close()


def _schema_without_data(df: pd.DataFrame) -> str:
    schema = [{"name": col, "type": str(df[col].dtype)} for col in df.columns]
    return json.dumps(schema, ensure_ascii=True)


def _build_jira_payload(
    summary: str,
    ticket_text: str,
) -> dict:
    return {
        "fields": {
            "project": {"key": "DATA"},
            "summary": summary,
            "description": {
                "type": "doc",
                "version": 1,
                "content": [
                    {
                        "type": "paragraph",
                        "content": [{"type": "text", "text": ticket_text}],
                    }
                ],
            },
            "issuetype": {"name": "Task"},
            "assignee": {"displayName": "Anna-Lise Datavich"},
            "reporter": {"name": "data-viz-automation"},
        }
    }


def _build_ticket_text(question: str, sql_query: str) -> tuple[str, str]:
    summary = "demande de viz"
    text = (
        "j'aimerais ajouter sur power bi cette visualition :\n"
        f"{question}\n"
        "la requete utilisee est :\n"
        f"{sql_query}\n"
        "Merci bonne journee."
    )
    return summary, text


@st.dialog("Apercu ticket Jira (factice)")
def _jira_ticket_preview_dialog() -> None:
    if "jira_draft" not in st.session_state:
        st.warning("Aucun draft Jira disponible.")
        return

    draft = st.session_state["jira_draft"]
    edited_summary = st.text_input("Summary", value=draft["summary"])
    edited_text = st.text_area("Description", value=draft["text"], height=220)

    payload = _build_jira_payload(summary=edited_summary, ticket_text=edited_text)
    payload_json = json.dumps(payload, ensure_ascii=False, indent=2)

    st.markdown("#### JSON Jira genere")
    st.code(payload_json, language="json")
    st.button("Confirmer l'envoi du ticket (factice)", use_container_width=True)
    st.caption("Bouton factice: aucun appel Jira n'est execute.")


@st.cache_data(ttl=60)
def _run_preview_query(table_name: str) -> int:
    connection = psycopg2.connect(
        host=_cfg("APP_DB_HOST", "postgres-db"),
        port=int(_cfg("APP_DB_PORT", "5432")),
        user=_cfg("APP_DB_USER", ""),
        password=_cfg("APP_DB_PASSWORD", ""),
        dbname=_cfg("APP_DB_NAME", "rfm"),
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            row = cursor.fetchone()
            return int(row[0]) if row else 0
    finally:
        connection.close()


st.set_page_config(page_title="Steam LLM Viz SQL", layout="wide")
st.title("Steam LLM Viz SQL")
st.caption("Generation de SQL avec garde-fous anti-hallucination")

try:
    doc = _load_doc()
except Exception as error:
    st.error(f"Impossible de charger le fichier JSON de documentation: {error}")
    st.stop()

st.markdown("### Documentation chargee")
st.write(f"Table: `{doc['table']}`")
st.write(f"Colonnes autorisees: `{len(doc['rows'])}`")

try:
    row_count = _run_preview_query(doc["table"])
    st.metric("Lignes disponibles (table source)", f"{row_count}")
except Exception as error:
    st.warning(f"Connexion DB indisponible pour compter les lignes: {error}")

st.markdown("### Colonnes de base autorisees")
st.dataframe(doc["rows"], use_container_width=True, hide_index=True)

st.markdown("### Description rapide du dataset")
st.markdown(
    """
- Source: `public.cleaned_orders`
- Granularite: 1 ligne = 1 ligne de facture (produit x quantite x prix)
- Periode: historique du dataset (pas de donnees temps reel)
- Mesure principale: `total_price` (montant de ligne)
- Axes frequents: `invoice_date`, `country`, `stock_code`, `description`

Exemples de demandes utiles:
- "chiffre d'affaires mensuel par pays"
- "top 10 produits par chiffre d'affaires"
- "evolution mensuelle du nombre de factures"
"""
)

st.markdown("### Question utilisateur -> Prompt SQL")
with st.form("viz_request_form", clear_on_submit=False):
    question = st.text_area(
        "Question dataviz",
        placeholder="Exemple: Donne le chiffre d'affaires total par pays sur les 12 derniers mois.",
    )
    submit_viz = st.form_submit_button(
        "Generer automatiquement la viz",
        use_container_width=True,
        type="primary",
    )

if submit_viz:
    if not question.strip():
        st.warning("Saisis une question avant de lancer le workflow.")
    else:
        debug: dict[str, str] = {}
        try:
            sql_prompt = _build_sql_prompt(question.strip(), doc)
            debug["prompt_sql_enrichi"] = sql_prompt

            model_sql = _cfg("GROQ_MODEL_SQL", "openai/gpt-oss-120b")
            llm_usage_rows: list[dict[str, object]] = []

            sql_response_raw, sql_usage = _call_grok(sql_prompt, model_sql)
            llm_usage_rows.append(
                {
                    "step": "sql_generation",
                    "model": model_sql,
                    **sql_usage,
                    "estimated_cost_usd": _estimate_cost_usd(model_sql, sql_usage),
                }
            )
            debug["reponse_1_grok"] = sql_response_raw

            sql_payload = _extract_json_block(sql_response_raw)
            sql_query = str(sql_payload.get("sql", "")).strip()
            why_sql = str(sql_payload.get("why", "")).strip()
            if not sql_query:
                raise ValueError("La reponse SQL de Grok ne contient pas de champ `sql`.")

            _assert_safe_select_sql(sql_query, doc)
            st.markdown("### SQL genere")
            st.code(sql_query, language="sql")
            if why_sql:
                st.caption(why_sql)

            result_df = pd.DataFrame()
            sql_executed = False
            last_sql_error = ""
            for attempt in range(1, 4):
                try:
                    result_df = _run_sql_query(sql_query)
                    sql_executed = True
                    break
                except Exception as sql_error:
                    last_sql_error = str(sql_error)
                    debug[f"sql_error_attempt_{attempt}"] = last_sql_error
                    if attempt == 3:
                        break
                    repair_prompt = _build_sql_repair_prompt(
                        previous_sql=sql_query,
                        error_message=last_sql_error,
                        doc=doc,
                    )
                    debug["prompt_sql_repair"] = repair_prompt
                    sql_repair_raw, sql_repair_usage = _call_grok(repair_prompt, model_sql)
                    llm_usage_rows.append(
                        {
                            "step": f"sql_repair_attempt_{attempt}",
                            "model": model_sql,
                            **sql_repair_usage,
                            "estimated_cost_usd": _estimate_cost_usd(model_sql, sql_repair_usage),
                        }
                    )
                    debug["reponse_sql_repair"] = sql_repair_raw
                    sql_repair_payload = _extract_json_block(sql_repair_raw)
                    sql_query = str(sql_repair_payload.get("sql", "")).strip()
                    why_sql = str(sql_repair_payload.get("why", why_sql)).strip()
                    if not sql_query:
                        raise ValueError("La reparation SQL n'a pas retourne de champ `sql`.")
                    _assert_safe_select_sql(sql_query, doc)

            if not sql_executed:
                raise ValueError(f"Echec execution SQL apres correction auto: {last_sql_error}")

            st.markdown("### Resultat SQL")
            st.dataframe(result_df.head(200), use_container_width=True, hide_index=True)
            if result_df.empty:
                st.warning("La requete SQL ne retourne aucune ligne.")

            viz_prompt = _build_viz_prompt(
                user_question=question.strip(),
                sql_query=sql_query,
                result_columns=_schema_without_data(result_df),
            )
            debug["prompt_viz_enrichi"] = viz_prompt

            model_code = _cfg("GROQ_MODEL_CODE", "llama-3.3-70b-versatile")
            viz_response_raw, viz_usage = _call_grok(viz_prompt, model_code)
            llm_usage_rows.append(
                {
                    "step": "viz_generation",
                    "model": model_code,
                    **viz_usage,
                    "estimated_cost_usd": _estimate_cost_usd(model_code, viz_usage),
                }
            )
            debug["reponse_2_grok"] = viz_response_raw
            viz_code = _extract_python_code(viz_response_raw)
            result_schema = _schema_without_data(result_df)

            st.markdown("### Visualisation")
            exec_globals = {
                "st": st,
                "pd": pd,
                "np": np,
                "alt": alt,
                "px": px,
                "go": go,
                "sns": sns,
                "plt": plt,
            }
            viz_executed = False
            last_viz_error = ""

            for attempt in range(1, 3):
                try:
                    _assert_safe_viz_code(viz_code)
                    compile(viz_code, "<viz_code>", "exec")
                    local_vars = {"df": result_df.copy()}
                    exec(viz_code, exec_globals, local_vars)
                    viz_executed = True
                    break
                except Exception as viz_error:
                    last_viz_error = str(viz_error)
                    debug[f"viz_error_attempt_{attempt}"] = last_viz_error
                    if attempt == 2:
                        break
                    repair_prompt = _build_viz_repair_prompt(
                        previous_code=viz_code,
                        error_message=last_viz_error,
                        result_columns=result_schema,
                    )
                    debug["prompt_viz_repair"] = repair_prompt
                    repair_response_raw, viz_repair_usage = _call_grok(repair_prompt, model_code)
                    llm_usage_rows.append(
                        {
                            "step": f"viz_repair_attempt_{attempt}",
                            "model": model_code,
                            **viz_repair_usage,
                            "estimated_cost_usd": _estimate_cost_usd(model_code, viz_repair_usage),
                        }
                    )
                    debug["reponse_viz_repair"] = repair_response_raw
                    viz_code = _extract_python_code(repair_response_raw)

            debug["code_viz"] = viz_code
            if not viz_executed:
                raise ValueError(f"Echec execution code viz apres correction auto: {last_viz_error}")

            ticket_summary, ticket_text = _build_ticket_text(
                question=question.strip(),
                sql_query=sql_query,
            )
            jira_payload = _build_jira_payload(
                summary=ticket_summary,
                ticket_text=ticket_text,
            )
            jira_payload_json = json.dumps(jira_payload, ensure_ascii=False, indent=2)

            st.session_state["last_viz_artifacts"] = {
                "question": question.strip(),
                "sql_query": sql_query,
                "sql_why": why_sql,
                "result_df": result_df,
            }
            st.session_state["jira_draft"] = {
                "summary": ticket_summary,
                "text": ticket_text,
                "payload_json": jira_payload_json,
            }

            usage_df = pd.DataFrame(llm_usage_rows)
            if not usage_df.empty:
                usage_df["estimated_cost_usd"] = usage_df["estimated_cost_usd"].fillna(0.0)
                total_prompt = int(usage_df["prompt_tokens"].sum())
                total_completion = int(usage_df["completion_tokens"].sum())
                total_tokens = int(usage_df["total_tokens"].sum())
                total_cost = float(usage_df["estimated_cost_usd"].sum())

                st.markdown("### Usage et cout LLM (estimation)")
                st.dataframe(
                    usage_df,
                    use_container_width=True,
                    hide_index=True,
                )
                st.caption(
                    "Total tokens "
                    f"(prompt/completion/total): {total_prompt}/{total_completion}/{total_tokens} "
                    f"| Cout estime: ${total_cost:.6f}"
                )
                debug["llm_usage_json"] = usage_df.to_json(orient="records", force_ascii=True)

            debug["jira_payload_json"] = jira_payload_json
        except Exception as error:
            st.error(f"Echec workflow auto: {error}")
        finally:
            st.markdown("---")
            st.markdown("### Debug workflow")
            st.markdown("#### Prompt enrichi SQL")
            st.code(debug.get("prompt_sql_enrichi", ""), language="text")
            st.markdown("#### Reponse 1 Grok")
            st.code(debug.get("reponse_1_grok", ""), language="text")
            st.markdown("#### Prompt repair SQL (si utilise)")
            st.code(debug.get("prompt_sql_repair", ""), language="text")
            st.markdown("#### Reponse repair SQL (si utilise)")
            st.code(debug.get("reponse_sql_repair", ""), language="text")
            st.markdown("#### Prompt enrichi VIZ")
            st.code(debug.get("prompt_viz_enrichi", ""), language="text")
            st.markdown("#### Reponse 2 Grok")
            st.code(debug.get("reponse_2_grok", ""), language="text")
            st.markdown("#### Prompt repair VIZ (si utilise)")
            st.code(debug.get("prompt_viz_repair", ""), language="text")
            st.markdown("#### Reponse repair VIZ (si utilise)")
            st.code(debug.get("reponse_viz_repair", ""), language="text")
            st.markdown("#### Code VIZ execute")
            st.code(debug.get("code_viz", ""), language="python")
            st.markdown("#### JSON type Jira genere")
            st.code(debug.get("jira_payload_json", ""), language="json")
            st.markdown("#### Usage LLM (JSON)")
            st.code(debug.get("llm_usage_json", ""), language="json")

st.info(
    "Workflow Grok: (1) prompt SQL a partir de la doc, (2) execution SQL, "
    "(3) prompt VIZ a partir du schema de resultat sans envoyer de data brute."
)

if "jira_draft" in st.session_state:
    if st.button("Generer un ticket Jira", use_container_width=True):
        _jira_ticket_preview_dialog()


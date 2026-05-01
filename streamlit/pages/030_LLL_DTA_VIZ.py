import json
import os
import re
import ast
from pathlib import Path

import altair as alt
import pandas as pd
import psycopg2
import requests
import streamlit as st


DOC_PATH_CANDIDATES = [
    Path(__file__).resolve().parents[1] / "data" / "llm-viz-base-columns.json",
    Path(__file__).resolve().parents[2] / ".local" / "llm-viz-base-columns.json",
]


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

Format de sortie OBLIGATOIRE (JSON strict):
{{
  "sql": "SELECT ...",
  "why": "Explication courte"
}}

Question utilisateur:
{user_question}
"""


def _build_viz_prompt(
    user_question: str,
    sql_query: str,
    result_columns: str,
) -> str:
    return f"""Tu es un assistant Python Streamlit specialise data-viz.
Ta mission: generer uniquement du code de visualisation Streamlit/Altair a partir
de la question utilisateur, du SQL deja produit, et du schema de resultat.

Contraintes:
- Ne pas regenerer de SQL
- Ne pas inventer de colonnes
- Ne pas demander ni utiliser de donnees brutes
- Utiliser uniquement les colonnes du schema fourni
- Retourner uniquement du code Python executable
- Libraries autorisees uniquement: streamlit, pandas, altair
- Si tu ajoutes des imports, ils doivent etre strictement: `import streamlit as st`, `import pandas as pd`, `import altair as alt`
- Ne retourne aucun texte hors code Python

SQL valide deja retenu:
{sql_query}

Schema du resultat SQL (colonnes + types), sans data:
{result_columns}

Contexte d'execution:
- Un DataFrame pandas `df` existe deja (resultat de la requete SQL)
- Stack dispo: streamlit as st, pandas as pd, altair as alt

Attendu:
1) code Streamlit/Altair
2) titre du graphique clair
3) gestion minimale des cas vides (`if df.empty`)

Question utilisateur:
{user_question}
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
    allowed_imports = {"streamlit", "pandas", "altair"}
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


def _call_grok(prompt: str, model: str) -> str:
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
    return payload["choices"][0]["message"]["content"]


def _assert_safe_select_sql(sql_query: str, doc: dict) -> None:
    lower = sql_query.strip().lower()
    forbidden = [" delete ", " update ", " insert ", " drop ", " alter ", " truncate "]
    normalized = f" {lower} "
    if not lower.startswith("select"):
        raise ValueError("La requete doit commencer par SELECT.")
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
            sql_response_raw = _call_grok(sql_prompt, model_sql)
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

            result_df = _run_sql_query(sql_query)
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
            viz_response_raw = _call_grok(viz_prompt, model_code)
            debug["reponse_2_grok"] = viz_response_raw
            viz_code = _extract_python_code(viz_response_raw)
            _assert_safe_viz_code(viz_code)
            debug["code_viz"] = viz_code

            st.markdown("### Visualisation")
            local_vars = {"df": result_df.copy()}
            exec_globals = {"st": st, "pd": pd, "alt": alt}
            exec(viz_code, exec_globals, local_vars)

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
            st.markdown("#### Prompt enrichi VIZ")
            st.code(debug.get("prompt_viz_enrichi", ""), language="text")
            st.markdown("#### Reponse 2 Grok")
            st.code(debug.get("reponse_2_grok", ""), language="text")
            st.markdown("#### Code VIZ execute")
            st.code(debug.get("code_viz", ""), language="python")
            st.markdown("#### JSON type Jira genere")
            st.code(debug.get("jira_payload_json", ""), language="json")

st.info(
    "Workflow Grok: (1) prompt SQL a partir de la doc, (2) execution SQL, "
    "(3) prompt VIZ a partir du schema de resultat sans envoyer de data brute."
)

if "jira_draft" in st.session_state:
    if st.button("Generer un ticket Jira", use_container_width=True):
        _jira_ticket_preview_dialog()


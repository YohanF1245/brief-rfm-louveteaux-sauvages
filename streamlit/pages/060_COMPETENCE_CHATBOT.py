from pathlib import Path
import json
import re
import streamlit as st
from groq import Groq

APP_TITLE = "Assistant référentiel Data Engineer"
DEFAULT_MODEL = "llama-3.1-8b-instant"
CONFIG_FILE = Path("config.json")
KB_FILE = Path("data/knowledge_base.json")


def load_config():
    if CONFIG_FILE.exists():
        try:
            return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_config(config):
    CONFIG_FILE.write_text(json.dumps(config, indent=2), encoding="utf-8")


@st.cache_data(show_spinner=False)
def load_kb():
    return json.loads(KB_FILE.read_text(encoding="utf-8"))


def norm(text):
    return re.sub(r"\s+", " ", text.upper()).strip()


def extract_range_or_list(question, prefix, min_n, max_n):
    q = norm(question)
    m = re.search(rf"{prefix}\s*(\d{{1,2}})\s*(?:-|A|À|TO|JUSQU.?A|JUSQU.?À)\s*{prefix}?\s*(\d{{1,2}})", q)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if min_n <= a <= max_n and min_n <= b <= max_n:
            lo, hi = sorted((a, b))
            return list(range(lo, hi + 1))
    nums = [int(x) for x in re.findall(rf"{prefix}\s*(\d{{1,2}})", q)]
    return sorted({x for x in nums if min_n <= x <= max_n})


def extract_competencies(question):
    return [f"C{x}" for x in extract_range_or_list(question, "C", 1, 21)]


def extract_evaluations(question):
    return [f"E{x}" for x in extract_range_or_list(question, "E", 1, 7)]


def extract_blocks(question):
    q = norm(question)
    nums = []
    for p in [r"BLOC\s*(?:DE\s*COMP[ÉE]TENCES?)?\s*(\d)", r"BLOCK\s*(\d)"]:
        nums += [int(x) for x in re.findall(p, q)]
    return [f"Bloc {x}" for x in sorted({x for x in nums if 1 <= x <= 4})]


def fallback_context(question, kb):
    q = norm(question)
    ctx = {}

    keyword_map = {
        "SQL": ["C9"],
        "API": ["C8", "C12"],
        "REST": ["C12"],
        "SCRAP": ["C8"],
        "EXTRACTION": ["C8", "C9"],
        "AGRÉGATION": ["C10"],
        "AGREGATION": ["C10"],
        "NETTOY": ["C10"],
        "BASE DE DONN": ["C11"],
        "RGPD": ["C3", "C11", "C16", "C20", "C21"],
        "ENTREP": ["C13", "C14", "C15", "C16", "C17"],
        "WAREHOUSE": ["C13", "C14", "C15", "C16", "C17"],
        "ETL": ["C15"],
        "DIMENSION": ["C13", "C17"],
        "DATA LAKE": ["C18", "C19", "C20", "C21"],
        "CATALOG": ["C20"],
        "GOUVERNANCE": ["C21"],
        "LIVRABLE": [],
        "SOUTENANCE": []
    }

    selected = []
    for k, comps in keyword_map.items():
        if k in q:
            selected.extend(comps)

    selected = selected[:6]
    if not selected:
        selected = ["C8", "C9", "C10", "C11", "C12"] if "BLOC 2" in q else []

    for c in selected:
        ctx[c] = kb["competencies"][c]

    return ctx


def build_context(question, kb):
    comps = extract_competencies(question)
    evals = extract_evaluations(question)
    blocks = extract_blocks(question)

    context = {
        "competencies": {},
        "evaluations": {},
        "blocks": {}
    }

    for c in comps:
        if c in kb["competencies"]:
            context["competencies"][c] = kb["competencies"][c]

    for e in evals:
        if e in kb["evaluations"]:
            context["evaluations"][e] = kb["evaluations"][e]
            for c in re.findall(r"C\d{1,2}", kb["evaluations"][e]):
                if c in kb["competencies"]:
                    context["competencies"][c] = kb["competencies"][c]

    for b in blocks:
        if b in kb["blocks"]:
            context["blocks"][b] = kb["blocks"][b]
            ranges = {"Bloc 1": range(1, 8), "Bloc 2": range(8, 13), "Bloc 3": range(13, 18), "Bloc 4": range(18, 22)}
            for n in ranges[b]:
                context["competencies"][f"C{n}"] = kb["competencies"][f"C{n}"]

    if not context["competencies"] and not context["evaluations"] and not context["blocks"]:
        context["competencies"] = fallback_context(question, kb)

    return context


def deterministic_answer(question, context):
    q = norm(question)
    wants_simple = any(x in q for x in ["EXPLIQUE", "EXPLAIN", "RÉSUME", "RESUME", "WHAT IS", "C'EST QUOI"])
    wants_compare = any(x in q for x in ["DIFFÉRENCE", "DIFFERENCE", "COMPARE", "COMPARER"])

    if wants_simple and context["competencies"] and len(context["competencies"]) <= 6 and not wants_compare:
        lines = []
        for code, item in context["competencies"].items():
            lines.append(f"**{code} — {item['title']}**")
            lines.append(f"{item['summary']}")
            if item.get("expected"):
                lines.append(f"Attendu : {item['expected']}")
            lines.append("")
        return "\n".join(lines).strip()

    return None


def ask_groq(question, context, api_key, model):
    compact_context = json.dumps(context, ensure_ascii=False)
    system = """
Tu es un assistant pédagogique pour des étudiants Data Engineer Simplon.
Réponds en français, de manière directe et structurée.
Utilise uniquement le contexte JSON fourni.
N'invente rien. Ne dis pas "en tant qu'IA". N'utilise pas d'emoji.
Si plusieurs compétences sont demandées, réponds compétence par compétence.
Ne rédige pas un rapport complet à la place de l'étudiant.
Réponse courte : maximum 250 mots.
"""
    user = f"Contexte JSON:\n{compact_context}\n\nQuestion:\n{question}"
    client = Groq(api_key=api_key)
    r = client.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": system.strip()}, {"role": "user", "content": user}],
        temperature=0.2,
        max_tokens=450
    )
    return r.choices[0].message.content


def test_groq(api_key, model):
    try:
        client = Groq(api_key=api_key)
        client.chat.completions.create(model=model, messages=[{"role": "user", "content": "OK"}], max_tokens=3)
        return True, "Connexion validée."
    except Exception as e:
        return False, f"Connexion impossible : {e}"


st.set_page_config(page_title=APP_TITLE, layout="wide")
kb = load_kb()
config = load_config()

if "messages" not in st.session_state:
    st.session_state.messages = []
if "api_key" not in st.session_state:
    st.session_state.api_key = config.get("groq_api_key", "")
if "model" not in st.session_state:
    st.session_state.model = config.get("model", DEFAULT_MODEL)
if "pending" not in st.session_state:
    st.session_state.pending = None

st.title(APP_TITLE)
st.caption("Version légère : index structuré, faible consommation de tokens.")

left, right = st.columns([1, 2])

with left:
    st.subheader("Configuration Groq")
    api_key = st.text_input("Groq API Key", value=st.session_state.api_key, type="password", placeholder="gsk_...")
    model = st.text_input("Modèle", value=st.session_state.model)

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Enregistrer", use_container_width=True):
            st.session_state.api_key = api_key.strip()
            st.session_state.model = model.strip() or DEFAULT_MODEL
            save_config({"groq_api_key": st.session_state.api_key, "model": st.session_state.model})
            st.success("Enregistré.")
    with c2:
        if st.button("Tester", use_container_width=True):
            if not api_key.strip():
                st.error("Colle une clé Groq.")
            else:
                ok, msg = test_groq(api_key.strip(), model.strip() or DEFAULT_MODEL)
                st.success(msg) if ok else st.error(msg)

    st.divider()
    st.subheader("Questions rapides")
    examples = [
        "Explique C8.",
        "Explique C8 à C12.",
        "Compare C13, C14 et C15.",
        "Quelles compétences sont évaluées dans E4 ?",
        "Quels livrables pour le bloc 2 ?",
        "Quelles compétences concernent le data lake ?"
    ]
    for ex in examples:
        if st.button(ex, use_container_width=True):
            st.session_state.pending = ex

    st.divider()
    if st.button("Réinitialiser", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

with right:
    if not st.session_state.api_key and not api_key.strip():
        st.info("Colle une clé Groq à gauche, puis clique sur Enregistrer.")
    else:
        for m in st.session_state.messages:
            with st.chat_message(m["role"]):
                st.write(m["content"])
                if m.get("context"):
                    with st.expander("Contexte utilisé"):
                        st.json(m["context"])

        question = st.chat_input("Écrire une question")
        if st.session_state.pending:
            question = st.session_state.pending
            st.session_state.pending = None

        if question:
            st.session_state.messages.append({"role": "user", "content": question})
            with st.chat_message("user"):
                st.write(question)

            context = build_context(question, kb)
            answer = deterministic_answer(question, context)

            if answer is None:
                try:
                    answer = ask_groq(question, context, api_key.strip() or st.session_state.api_key, model.strip() or st.session_state.model)
                except Exception as e:
                    answer = f"Erreur lors de l'appel à Groq : {e}"

            with st.chat_message("assistant"):
                st.write(answer)
                with st.expander("Contexte utilisé"):
                    st.json(context)

            st.session_state.messages.append({"role": "assistant", "content": answer, "context": context})

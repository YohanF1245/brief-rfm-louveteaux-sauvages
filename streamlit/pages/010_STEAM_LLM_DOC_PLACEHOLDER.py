import streamlit as st


st.set_page_config(page_title="Steam LLM Doc", layout="wide")

st.title("Steam LLM - Documentation (Placeholder)")
st.caption("Page reservee pour ecrire la documentation du workflow Steam + LLM.")

st.markdown(
    """
## A completer

- Objectif du pipeline
- Description du DAG `steam_reviews_sentiment_dag`
- Description du DAG `llm_analysis_dag`
- Choix des modeles LLM et methode de comparaison
- Limites connues et pistes d'amelioration
- Procedure d'execution (ordre des DAGs, verification, debug)
"""
)

st.info("Tu peux modifier cette page librement pour documenter ton travail.")

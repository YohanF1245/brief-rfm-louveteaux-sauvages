import re
from pathlib import Path

import streamlit as st

# st.title, st.header, st.subheader, st.caption, st.markdown, st.write
# st.text_input, st.text_area, st.number_input, st.selectbox, st.multiselect, st.radio, st.checkbox, st.button
# st.form, st.form_submit_button
# st.file_uploader, st.download_button
# st.dataframe, st.table, st.metric, st.json
# st.line_chart, st.bar_chart, st.area_chart, st.map
# st.columns, st.tabs, st.expander, st.container, st.sidebar
# st.image, st.audio, st.video
# st.success, st.warning, st.error, st.info, st.toast
# st.spinner, st.progress, st.status
# st.session_state, st.rerun, st.stop
# st.cache_data, st.cache_resource
# st.set_page_config, st.page_link, st.switch_page


st.set_page_config(page_title="Steam LLM Doc", layout="wide")

TITLE_COLOR = "#1F4E79"
HEADER_COLOR = "#2E7D32"

st.markdown(
    f"""
    <style>
      h1.st-doc-title {{
        color: {TITLE_COLOR};
        margin-bottom: 0.2rem;
      }}
      h2.st-doc-header {{
        color: {HEADER_COLOR};
        margin-top: 1rem;
      }}
      .st-doc-toc a {{
        text-decoration: none;
      }}
      .st-doc-toc a:hover {{
        text-decoration: underline;
      }}
      .st-back-top {{
        position: fixed;
        right: 1rem;
        bottom: 1rem;
        background: #1F4E79;
        color: #fff !important;
        padding: 0.55rem 0.8rem;
        border-radius: 999px;
        font-size: 0.9rem;
        text-decoration: none;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
        z-index: 1000;
      }}
      .st-back-top:hover {{
        background: #163a5a;
      }}
    </style>
    """,
    unsafe_allow_html=True,
)


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", text.strip().lower()).strip("-")
    return slug or "section"


def title_anchor(text: str, anchor_id: str | None = None) -> None:
    anchor = anchor_id or _slugify(text)
    st.markdown(
        f'<h1 id="{anchor}" class="st-doc-title">{text}</h1>',
        unsafe_allow_html=True,
    )


def header_anchor(text: str, anchor_id: str | None = None) -> None:
    anchor = anchor_id or _slugify(text)
    st.markdown(
        f'<h2 id="{anchor}" class="st-doc-header">{text}</h2>',
        unsafe_allow_html=True,
    )

title_anchor("Steam LLM - Documentation", anchor_id="steam-llm-documentation")
st.markdown(
    """
<div class="st-doc-toc">

- [Objectif du pipeline](#objectif-du-pipepline)
- [Prompt utilisé pour l'analyse des reviews](#prompt-utilise)
- [Modèle entité - relationnel pour le stockage des données](#schema-entite-relationnel)
- [Description du DAG `steam_reviews_sentiment_dag`](#description-du-dag-steam-reviews)
- [Description du DAG `llm_analysis_dag`](#description-du-dag-llm-analysis)
- [Choix des modeles LLM et methode de comparaison](#choix-modeles-et-comparaison)
- [Limites connues et pistes d'amelioration](#limites-et-ameliorations)

</div>
""",
    unsafe_allow_html=True,
)

header_anchor("Objectif du pipepline", anchor_id="objectif-du-pipepline")
st.markdown("""
L'idée de ce pipeline est de traiter et d'analyser le verbatim des reviews steam afin d'en extraire des insights utiles via des modèles llm.

Pour les besoins du POC, le jeu retenu est « Cyberpunk 2077 ». Ce choix s’explique par les nombreuses controverses qu’il a suscitées ainsi que par l’ampleur des débats qu’il a générés en ligne. Les avis à son sujet étant particulièrement partagés, il divise fortement l’opinion, ce qui en fait un candidat pertinent pour ce type d’étude.

Le pipeline se divise en deux étapes :
- Ingestin des reviews via l'url "https://store.steampowered.com/appreviews/{app_id}"
- Analyse des reviews concurentielles via plusieurs modeles llm

Pour la visualisation des resulats trois pages ont été créées :
- reviews steam : "https://ymfo1nom.com/STEAM_REVIEWS"
- analyse llm : "https://ymfo1nom.com/STEAM_LLM_REVIEW"
- stats llm : "https://ymfo1nom.com/STEAM_LLM_KEYWORDS"
""")
header_anchor("Prompt  Utilisé", anchor_id="prompt-utilise")

with st.expander("Prompt utilisé pour l'analyse des reviews"):
    st.markdown(
"""

    You are an expert system for analyzing video game reviews.

    Your task is to extract structured insights from a single user review. Reviews may contain slang, sarcasm, exaggeration, or low-effort text. You must interpret the REAL meaning, not just the literal wording.

    ---

    OUTPUT (JSON only):
    ```json
    {
        "sentiment": "positive | negative | mixed",
        "confidence": 0-1,

        "criticite": 0-100,
        "serieux": 0-100,

        "keywords": [
              {
             "category": "gameplay | graphics | performance | story | sound | content | bugs | optimization | multiplayer | ui | pricing | replayability | immersion | other",
              "keyword": "string",
              "polarity": "positive | negative"
             }
        ],

        "analysis_flags": {
            "sarcasm_detected": true/false,
            "noise_level": 0-100,
            "is_constructive": true/false
            }
    }
    ```

    ---

    RULES:

    1. SENTIMENT:

    * Determine the REAL sentiment, not just literal wording.
    * Detect sarcasm and irony (e.g. "great game, crashes every 5 min" = negative).
    * Use "mixed" if both strong positive and negative points exist.

    2. CRITICITE (0-100):

    * Measures emotional intensity.
    * 0 = neutral/objective
    * 100 = extreme (anger, hype, outrage)

    3. SERIEUX (0-100):

    * Measures how trustworthy and useful the review is.
    * High = detailed, specific, constructive
    * Low = short, insults, memes, spam

    4. KEYWORDS:

    * Extract meaningful aspects of the game.
    * Normalize slang (e.g. "runs like shit" -> performance, negative)
    * Avoid duplicates and keep keywords concise.

    5. FLAGS:

    * sarcasm_detected: true if irony likely
    * noise_level: high if slang, insults, low signal
    * is_constructive: true if useful feedback

    6. KEYWORD NORMALIZATION LEVEL:
    - Use mid-level abstraction.
    - Avoid overly generic terms (e.g. "world building", "game quality")
    - Avoid overly specific phrases copied verbatim.
    - Prefer standardized but meaningful expressions (e.g. "immersive world", "strong character writing")

    ---

    Be strict, consistent, and avoid hallucinations.
    Return only valid JSON.

    
    ---

    INPUT:

    * review_text: {review_text}
    """
    )


header_anchor("Schema Entité - Relationnel", anchor_id="schema-entite-relationnel")
_img_path = Path(__file__).resolve().parents[1] / "data" / "llm-analysis-steam-images" / "mcd.jpg"
if _img_path.exists():
    st.image(str(_img_path))
else:
    st.warning(f"Image introuvable: {_img_path}")

header_anchor("Description du DAG steam_reviews_sentiment_dag", anchor_id="description-du-dag-steam-reviews")

st.markdown("""
Ce DAG a pour objectif d’ingérer des avis issus de Steam, puis de les stocker en base de données.

Dans ce cadre, il récupère un échantillon équilibré de **50 avis positifs** et **50 avis négatifs**, afin de constituer un jeu de données exploitable pour les traitements d’analyse ultérieurs.

Pour chaque avis, les informations suivantes sont enregistrées :
- l’identifiant de la review  
- le contenu textuel  
- la polarité (*positif* ou *négatif*)  
- la date de création  
""")

header_anchor("Description du DAG llm_analysis_dag", anchor_id="description-du-dag-llm-analysis")

st.markdown("""
Ce DAG a pour rôle d’analyser les avis préalablement stockés en base de données.

Pour chaque review, une requête est envoyée à l’API de Groq, en utilisant un prompt spécifique adapté à chaque modèle de langage (LLM). L’objectif est d’obtenir une réponse structurée au format JSON, contenant différents insights issus de l’analyse.

En cas d'erreur, l'erreur est journalisée dans la table `review_llm_analysis_errors` pour faciliter le debuggage.

**Modèles LLM utilisés :**
- llama-3.1-8b-instant  
- llama-3.3-70b-versatile  
- openai/gpt-oss-120b  
- openai/gpt-oss-20b  
- meta-llama/llama-4-scout-17b-16e-instruct  
- qwen/qwen3-32b  

Les résultats de l’analyse sont ensuite stockés dans plusieurs tables :
- `review_llm_analysis` : synthèse globale du sentiment de la review  
- `review_llm_analysis_keywords` : extraction et catégorisation des mots-clés, permettant la construction d’une matrice d’analyse  
- `review_llm_analysis_errors` : journal des erreurs d'appel LLM (review_id, modele, code HTTP, body d'erreur) pour faciliter le debuggage  

""")
header_anchor("Choix des modeles LLM et methode de comparaison", anchor_id="choix-modeles-et-comparaison")
st.markdown("""
### Comprendre les indicateurs

#### Moyenne (mean)
La moyenne permet d’évaluer la **tendance générale** d’un modèle.  
Par exemple, une confiance moyenne élevée indique que le modèle “assume” globalement ses réponses.

#### Écart-type (std)
L’écart-type mesure la **régularité du modèle** :
- faible écart-type → comportement stable  
- fort écart-type → comportement variable selon les avis  

---

###  Interet des indicateurs

La moyenne seule peut être trompeuse.  
Deux modèles peuvent afficher la même moyenne, tout en ayant des comportements très différents.

Associer **moyenne + dispersion** permet d’obtenir une vision plus fiable et complète.

---

### Logique métier

En production, l’objectif est d’avoir un modèle :
- **pertinent**
- **prévisible**

La stabilité est essentielle pour éviter des résultats incohérents d’un avis à l’autre.

---

### Aide à la décision

Ces indicateurs permettent de choisir un modèle selon le besoin :

- **Homogénéité / fiabilité** → privilégier un faible écart-type  
- **Sensibilité / détection de nuances** → une certaine variabilité peut être acceptable  

---

### Lecture rapide des graphiques

- **Moyenne haute + std faible** → modèle **solide et constant**  
- **Moyenne haute + std élevé** → bon en moyenne, mais **instable**  
- **Moyenne basse + std faible** → **constant mais peu performant**  
- **Moyenne basse + std élevé** → modèle **peu fiable**  

### Gestion des erreurs
- un toggle pour ne garder que les reviews analysees sur les 6 modeles
- un tableau des reviews incompletes avec les modeles manquants
- un filtre par code d'erreur (ex: 400, 429)

""")
header_anchor("Limites connues et pistes d'amelioration", anchor_id="limites-et-ameliorations")
st.markdown("""
### Limites de l’analyse

L’analyse des verbatims utilisateurs présente plusieurs défis majeurs.

D’une part, le contexte des plateformes en ligne — combinant une forte activité communautaire, une culture “geek” marquée et un climat parfois polémique — favorise l’émergence de contenus bruités, voire de comportements de type *trolling*.

D’autre part, les avis Steam sont souvent rédigés avec des figures de style comme le sarcasme ou l’ironie, rendant leur interprétation complexe pour les modèles de langage.  
Par exemple : *« Ma femme est partie avec le facteur, du coup je mets 10/10 au jeu »*.

Ces éléments peuvent entraîner des erreurs d’interprétation, notamment sur la polarité réelle des avis, et limitent ainsi la fiabilité de l’analyse purement basée sur le texte.

---

### Pistes d’amélioration

Afin d’améliorer la précision et la robustesse de l’analyse, plusieurs axes peuvent être envisagés.

#### Fiabilisation des appels LLM

Mettre en place une gestion d'erreurs dediee pour renforcer la robustesse du pipeline:
- reprise automatique des appels en cas de `rate_limit_exceeded` (HTTP 429) avec temporisation progressive
- gestion explicite des reponses JSON invalides (`json_validate_failed`) avec retries cibles et journalisation des cas rejetes


#### Enrichissement des données

L’intégration de métadonnées supplémentaires permettrait d’apporter un contexte précieux, notamment :
- configuration matérielle du joueur  
- nombre total de jeux possédés  
- nombre de reviews publiées  
- temps de jeu sur le jeu concerné  
- indication “produit reçu gratuitement”  
- indication “produit remboursé”  

Ces informations permettraient de mieux qualifier la fiabilité et le profil des auteurs.

#### Approche hybride (statistique + LLM)

Le croisement des analyses LLM avec des méthodes de machine learning plus classiques permettrait d’améliorer la robustesse globale :
- détection de patterns comportementaux  
- pondération des avis selon leur crédibilité  
- meilleure gestion des cas ambigus (ironie, sarcasme, trolling)  

Cette approche hybride offrirait une analyse plus précise et mieux adaptée aux spécificités des données utilisateurs.
""")

st.markdown("[↑ Retour en haut](#steam-llm-documentation)")
st.markdown(
    '<a class="st-back-top" href="#steam-llm-documentation">↑ Haut</a>',
    unsafe_allow_html=True,
)
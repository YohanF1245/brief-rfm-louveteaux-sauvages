from __future__ import annotations

import json
import re
import time
from typing import Any

import requests
from airflow.sdk import Variable, dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime


GROQ_CHAT_COMPLETIONS_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODELS_URL = "https://api.groq.com/openai/v1/models"
# IDs alignés sur la doc Groq (chat). Variable Airflow `groq_models` (CSV) pour surcharger.
# Hors scope ici: whisper-*, canopylabs/*, *prompt-guard* (audio / TTS / modération).
MODELS_LIST = [
    "llama-3.1-8b-instant",
    "llama-3.3-70b-versatile",
    "openai/gpt-oss-120b",
    "openai/gpt-oss-20b",
    "meta-llama/llama-4-scout-17b-16e-instruct",
    "qwen/qwen3-32b",
]


class GroqRequestError(Exception):
    def __init__(self, status_code: int | None, error_text: str):
        self.status_code = status_code
        self.error_text = (error_text or "").strip()
        super().__init__(f"Groq request failed status={status_code}")


def _resolve_target_models() -> list[str]:
    raw = Variable.get("groq_models", default="")
    if not raw.strip():
        return MODELS_LIST
    return [m.strip() for m in raw.split(",") if m.strip()]


def _truthy_airflow_var(raw: str | None) -> bool:
    if raw is None:
        return False
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _request_delay_seconds() -> float:
    raw = Variable.get("groq_request_sleep_seconds", default="0.5").strip()
    try:
        value = float(raw.replace(",", "."))
    except ValueError:
        return 0.5
    return max(0.0, min(value, 120.0))


def _fetch_available_groq_models(api_key: str) -> set[str]:
    response = requests.get(
        GROQ_MODELS_URL,
        headers={"Authorization": f"Bearer {api_key}"},
        timeout=30,
    )
    response.raise_for_status()
    body = response.json()
    return {str(item.get("id")) for item in (body.get("data") or []) if item.get("id")}


def _build_prompt(review_text: str) -> str:
    return f"""
You are an expert system for analyzing video game reviews.

Your task is to extract structured insights from a single user review. Reviews may contain slang, sarcasm, exaggeration, or low-effort text. You must interpret the REAL meaning, not just the literal wording.

---

OUTPUT (JSON only):
{{
  "sentiment": "positive | negative | mixed",
  "confidence": 0-1,
  "criticite": 0-100,
  "serieux": 0-100,
  "keywords": [
    {{
      "category": "gameplay | graphics | performance | story | sound | content | bugs | optimization | multiplayer | ui | pricing | replayability | immersion | other",
      "keyword": "string",
      "polarity": "positive | negative"
    }}
  ],
  "analysis_flags": {{
    "sarcasm_detected": true/false,
    "noise_level": 0-100,
    "is_constructive": true/false
  }}
}}

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
* Use mid-level abstraction.
* Avoid overly generic terms (e.g. "world building", "game quality")
* Avoid overly specific phrases copied verbatim.
* Prefer standardized but meaningful expressions (e.g. "immersive world", "strong character writing")

---

Be strict, consistent, and avoid hallucinations.
Return only valid JSON.

INPUT:
review_text: {review_text}
""".strip()


def _extract_json_object(raw_text: str) -> dict[str, Any]:
    text = (raw_text or "").strip()
    if not text:
        raise ValueError("Empty LLM output")

    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    return json.loads(text)


def _call_groq_json(
    api_key: str,
    model_name: str,
    review_text: str,
) -> dict[str, Any]:
    payload = {
        "model": model_name,
        "messages": [{"role": "user", "content": _build_prompt(review_text)}],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }
    response = requests.post(
        GROQ_CHAT_COMPLETIONS_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=120,
    )
    if response.status_code != 200:
        error_text = response.text
        try:
            parsed_error = response.json()
            error_text = json.dumps(parsed_error, ensure_ascii=False)
        except Exception:
            pass
        raise GroqRequestError(status_code=response.status_code, error_text=error_text)

    body = response.json()
    content = (((body.get("choices") or [{}])[0].get("message") or {}).get("content")) or ""
    return _extract_json_object(content)


@dag(
    dag_id="llm_analysis_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["llm", "grok", "analysis", "postgres"],
    doc_md="""
    ## Reprise / tokens (backfill)

    - **Sans option**: chaque run refait tous les appels Groq (upsert en base, pas de doublon de lignes).
    - **Variable `groq_analysis_skip_existing`** (defaut `true`): si une ligne existe deja pour
      `analysis_id` (modele + review), **aucun appel API** — utile apres quota/token epuise pour
      ne traiter que les combinaisons manquantes.
    - Pour **forcer un recalcul complet**: definir `groq_analysis_skip_existing` a `false`.

    Autres variables: `groq_api_key`, `groq_models` (CSV), `grok_api_key` (fallback cle).
    - **`groq_request_sleep_seconds`**: pause en secondes entre deux appels Groq (defaut `0.5`, max `120`).
      Mettre `0` pour desactiver. Les lignes skippees (`groq_analysis_skip_existing`) ne declenchent pas d'appel ni de sleep.

    ## Persistance (v3)

    - **Commit PostgreSQL apres chaque paire (review_id, modele)** reussie : l'appel Groq n'est pas dans une transaction ouverte.
    - En cas de crash du worker, les analyses deja validees restent en base.
    """,
)
def llm_analysis_dag():
    @task()
    def run_analysis_and_load() -> dict[str, int]:
        groq_api_key = Variable.get("groq_api_key", default=None) or Variable.get(
            "grok_api_key",
            default=None,
        )
        if not groq_api_key:
            raise ValueError(
                "Missing Airflow variable for Groq API key. "
                "Set 'groq_api_key' (preferred) or 'grok_api_key'."
            )
        target_models = _resolve_target_models()
        try:
            available_models = _fetch_available_groq_models(groq_api_key)
            active_models = [m for m in target_models if m in available_models]
            skipped_models = [m for m in target_models if m not in available_models]
            if skipped_models:
                print(
                    "[WARN] Skipping unavailable Groq models: "
                    + ", ".join(skipped_models)
                )
        except Exception as error:
            print(
                f"[WARN] Could not fetch Groq models list, fallback to configured list. Error={error}"
            )
            active_models = target_models

        if not active_models:
            raise ValueError(
                "No active Groq model available. Configure Airflow variable 'groq_models' "
                "with valid model IDs for your account."
            )
        skip_existing = _truthy_airflow_var(
            Variable.get("groq_analysis_skip_existing", default="true")
        )
        request_delay_s = _request_delay_seconds()
        hook = PostgresHook(postgres_conn_id="DATA-DB")

        create_analysis_table_sql = """
        CREATE TABLE IF NOT EXISTS public.review_llm_analysis (
            analysis_id TEXT PRIMARY KEY,
            review_id TEXT NOT NULL,
            model_used VARCHAR(120) NOT NULL,
            sentiment VARCHAR(20),
            confidence NUMERIC,
            criticite NUMERIC,
            serieux NUMERIC,
            sarcasm_detected BOOLEAN,
            noise_level NUMERIC,
            is_contructive BOOLEAN,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT fk_review_llm_analysis_review
                FOREIGN KEY (review_id)
                REFERENCES public.steam_reviews_sentiment (review_id)
                ON DELETE CASCADE
        );
        """
        create_keywords_table_sql = """
        CREATE TABLE IF NOT EXISTS public.review_llm_analysis_keywords (
            id BIGSERIAL PRIMARY KEY,
            analysis_id TEXT NOT NULL,
            category VARCHAR(120),
            keyword VARCHAR(255),
            polarity VARCHAR(20),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT fk_review_llm_analysis_keywords_analysis
                FOREIGN KEY (analysis_id)
                REFERENCES public.review_llm_analysis (analysis_id)
                ON DELETE CASCADE
        );
        """
        create_errors_table_sql = """
        CREATE TABLE IF NOT EXISTS public.review_llm_analysis_errors (
            id BIGSERIAL PRIMARY KEY,
            review_id TEXT NOT NULL,
            model_used VARCHAR(120) NOT NULL,
            code_error INTEGER,
            error_text TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        select_reviews_sql = """
        SELECT review_id, review_text
        FROM public.steam_reviews_sentiment
        WHERE review_text IS NOT NULL AND review_text <> ''
        ORDER BY created_at DESC;
        """
        upsert_analysis_sql = """
        INSERT INTO public.review_llm_analysis (
            analysis_id,
            review_id,
            model_used,
            sentiment,
            confidence,
            criticite,
            serieux,
            sarcasm_detected,
            noise_level,
            is_contructive,
            updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (analysis_id) DO UPDATE
        SET
            review_id = EXCLUDED.review_id,
            model_used = EXCLUDED.model_used,
            sentiment = EXCLUDED.sentiment,
            confidence = EXCLUDED.confidence,
            criticite = EXCLUDED.criticite,
            serieux = EXCLUDED.serieux,
            sarcasm_detected = EXCLUDED.sarcasm_detected,
            noise_level = EXCLUDED.noise_level,
            is_contructive = EXCLUDED.is_contructive,
            updated_at = NOW();
        """
        delete_keywords_sql = """
        DELETE FROM public.review_llm_analysis_keywords
        WHERE analysis_id = %s;
        """
        insert_keyword_sql = """
        INSERT INTO public.review_llm_analysis_keywords (
            analysis_id,
            category,
            keyword,
            polarity
        ) VALUES (%s, %s, %s, %s);
        """
        insert_error_sql = """
        INSERT INTO public.review_llm_analysis_errors (
            review_id,
            model_used,
            code_error,
            error_text
        ) VALUES (%s, %s, %s, %s);
        """

        conn_setup = hook.get_conn()
        try:
            with conn_setup.cursor() as cursor:
                cursor.execute(create_analysis_table_sql)
                cursor.execute(create_keywords_table_sql)
                cursor.execute(create_errors_table_sql)
                cursor.execute(select_reviews_sql)
                review_rows = cursor.fetchall()
            conn_setup.commit()
        except Exception:
            conn_setup.rollback()
            raise
        finally:
            conn_setup.close()

        existing_analysis_ids: set[str] = set()
        if skip_existing and review_rows:
            review_ids = [row[0] for row in review_rows]
            conn_ids = hook.get_conn()
            try:
                with conn_ids.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT analysis_id
                        FROM public.review_llm_analysis
                        WHERE review_id = ANY(%s);
                        """,
                        (review_ids,),
                    )
                    existing_analysis_ids = {row[0] for row in cursor.fetchall() if row[0]}
                conn_ids.commit()
            except Exception:
                conn_ids.rollback()
                raise
            finally:
                conn_ids.close()
            print(
                f"[INFO] groq_analysis_skip_existing={skip_existing}: "
                f"{len(existing_analysis_ids)} analyses deja en base (candidates skip)."
            )
        print(f"[INFO] groq_request_sleep_seconds={request_delay_s}s entre chaque appel Groq.")
        print(
            f"[INFO] A traiter: {len(review_rows)} reviews x {len(active_models)} modeles "
            f"(max {len(review_rows) * len(active_models)} appels Groq)."
        )

        analyses_done = 0
        analyses_skipped = 0
        analysis_errors = 0
        keywords_written = 0

        for review_id, review_text in review_rows:
            for model_name in active_models:
                analysis_id = f"{model_name}__{review_id}"
                if skip_existing and analysis_id in existing_analysis_ids:
                    analyses_skipped += 1
                    continue
                print(
                    f"[INFO] Groq START review_id={review_id} model={model_name} "
                    f"(text_len={len(review_text or '')})"
                )
                try:
                    parsed = _call_groq_json(
                        api_key=groq_api_key,
                        model_name=model_name,
                        review_text=review_text,
                    )
                except Exception as error:
                    analysis_errors += 1
                    status_code = None
                    error_text = str(error)
                    if isinstance(error, GroqRequestError):
                        status_code = error.status_code
                        error_text = error.error_text
                    conn_error = hook.get_conn()
                    try:
                        with conn_error.cursor() as cursor:
                            cursor.execute(
                                insert_error_sql,
                                (
                                    review_id,
                                    model_name,
                                    status_code,
                                    error_text,
                                ),
                            )
                        conn_error.commit()
                    except Exception as log_error:
                        conn_error.rollback()
                        print(
                            f"[WARN] Could not log Groq error - review_id={review_id}, "
                            f"model={model_name}, error={log_error}"
                        )
                    finally:
                        conn_error.close()
                    print(
                        f"[WARN] Groq/API failed - review_id={review_id}, "
                        f"model={model_name}, error={error}"
                    )
                    if request_delay_s > 0:
                        time.sleep(request_delay_s)
                    continue

                flags = parsed.get("analysis_flags") or {}
                keywords_batch = parsed.get("keywords") or []
                conn_write = hook.get_conn()
                try:
                    with conn_write.cursor() as cursor:
                        cursor.execute(
                            upsert_analysis_sql,
                            (
                                analysis_id,
                                review_id,
                                model_name,
                                parsed.get("sentiment"),
                                parsed.get("confidence"),
                                parsed.get("criticite"),
                                parsed.get("serieux"),
                                flags.get("sarcasm_detected"),
                                flags.get("noise_level"),
                                flags.get("is_constructive"),
                            ),
                        )
                        cursor.execute(delete_keywords_sql, (analysis_id,))
                        for kw in keywords_batch:
                            cursor.execute(
                                insert_keyword_sql,
                                (
                                    analysis_id,
                                    kw.get("category"),
                                    kw.get("keyword"),
                                    kw.get("polarity"),
                                ),
                            )
                    conn_write.commit()
                except Exception as error:
                    conn_write.rollback()
                    analysis_errors += 1
                    print(
                        f"[WARN] PostgreSQL write failed - review_id={review_id}, "
                        f"model={model_name}, error={error}"
                    )
                    if request_delay_s > 0:
                        time.sleep(request_delay_s)
                    continue
                finally:
                    conn_write.close()

                keywords_written += len(keywords_batch)
                analyses_done += 1
                existing_analysis_ids.add(analysis_id)
                print(
                    f"[INFO] COMMIT OK review_id={review_id} model={model_name} "
                    f"keywords={len(keywords_batch)} sentiment={parsed.get('sentiment')!r}"
                )
                if request_delay_s > 0:
                    time.sleep(request_delay_s)

        return {
            "reviews_loaded": len(review_rows),
            "models_count": len(active_models),
            "analyses_done": analyses_done,
            "analyses_skipped": analyses_skipped,
            "analysis_errors": analysis_errors,
            "keywords_written": keywords_written,
        }

    run_analysis_and_load()


llm_analysis_dag()

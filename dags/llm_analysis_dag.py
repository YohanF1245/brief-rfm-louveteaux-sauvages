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
MODELS_LIST = [
    "llama-3.3-70b-versatile",
    "gpt-oss-120b",
    "qwen-3-32b",
    "llama-4-scout-17b-instruct",
    "llama-3.1-8b-instant",
    "gemma-2-9b-it",
]


def _build_prompt(review_text: str) -> str:
    return f"""
You are an expert system for analyzing video game reviews.

Your task is to extract structured insights from a single user review. Reviews may contain slang, sarcasm, exaggeration, or low-effort text. You must interpret the REAL meaning, not just the literal wording.

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
    response.raise_for_status()

    body = response.json()
    content = (((body.get("choices") or [{}])[0].get("message") or {}).get("content")) or ""
    return _extract_json_object(content)


@dag(
    dag_id="llm_analysis_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["llm", "grok", "analysis", "postgres"],
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
        hook = PostgresHook(postgres_conn_id="DATA-DB")
        conn = hook.get_conn()

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

        with conn:
            with conn.cursor() as cursor:
                cursor.execute(create_analysis_table_sql)
                cursor.execute(create_keywords_table_sql)
                cursor.execute(select_reviews_sql)
                review_rows = cursor.fetchall()

        analyses_done = 0
        analysis_errors = 0
        keywords_written = 0

        with conn:
            with conn.cursor() as cursor:
                for review_id, review_text in review_rows:
                    for model_name in MODELS_LIST:
                        analysis_id = f"{model_name}__{review_id}"
                        try:
                            parsed = _call_groq_json(
                                api_key=groq_api_key,
                                model_name=model_name,
                                review_text=review_text,
                            )
                            flags = parsed.get("analysis_flags") or {}
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
                            for kw in (parsed.get("keywords") or []):
                                cursor.execute(
                                    insert_keyword_sql,
                                    (
                                        analysis_id,
                                        kw.get("category"),
                                        kw.get("keyword"),
                                        kw.get("polarity"),
                                    ),
                                )
                                keywords_written += 1
                            analyses_done += 1
                        except Exception as error:
                            analysis_errors += 1
                            print(
                                f"[WARN] Analysis failed - review_id={review_id}, "
                                f"model={model_name}, error={error}"
                            )
                        time.sleep(0.2)

        return {
            "reviews_loaded": len(review_rows),
            "models_count": len(MODELS_LIST),
            "analyses_done": analyses_done,
            "analysis_errors": analysis_errors,
            "keywords_written": keywords_written,
        }

    run_analysis_and_load()


llm_analysis_dag()

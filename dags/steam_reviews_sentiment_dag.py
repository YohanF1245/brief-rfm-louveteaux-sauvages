from __future__ import annotations

from typing import Any

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import requests


STEAM_REVIEWS_URL = "https://store.steampowered.com/appreviews/{app_id}"
DEFAULT_APP_ID = "1091500"
TARGET_ROWS_PER_SENTIMENT = 50

def _steam_call(
    app_id: str,
    review_type: str,
    cursor: str = "*",
    num_per_page: int = 100,
) -> dict[str, Any]:
    response = requests.get(
        STEAM_REVIEWS_URL.format(app_id=app_id),
        params={
            "json": 1,
            "language": "english",
            "filter": "recent",
            "review_type": review_type,
            "purchase_type": "all",
            "num_per_page": num_per_page,
            "cursor": cursor,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def _shape_row(review: dict[str, Any]) -> dict[str, Any]:
    voted_up = bool(review.get("voted_up"))
    return {
        "review_id": str(review.get("recommendationid")),
        "review_text": (review.get("review") or "").strip(),
        "polarity": "positive" if voted_up else "negative",
        "created_at_ts": int(review.get("timestamp_created") or 0),
    }


def _fetch_sentiment_reviews(app_id: str, review_type: str, target_count: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor = "*"
    max_pages = 20

    for _ in range(max_pages):
        payload = _steam_call(app_id=app_id, review_type=review_type, cursor=cursor)
        reviews = payload.get("reviews") or []
        if not reviews:
            break

        for review in reviews:
            rows.append(_shape_row(review))
            if len(rows) >= target_count:
                return rows[:target_count]

        next_cursor = payload.get("cursor")
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor

    return rows[:target_count]


@dag(
    dag_id="steam_reviews_sentiment_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["steam", "reviews", "postgres", "sentiment"],
)
def steam_reviews_sentiment_dag():
    @task()
    def fetch_positive() -> list[dict[str, Any]]:
        return _fetch_sentiment_reviews(
            app_id=DEFAULT_APP_ID,
            review_type="positive",
            target_count=TARGET_ROWS_PER_SENTIMENT,
        )

    @task()
    def fetch_negative() -> list[dict[str, Any]]:
        return _fetch_sentiment_reviews(
            app_id=DEFAULT_APP_ID,
            review_type="negative",
            target_count=TARGET_ROWS_PER_SENTIMENT,
        )

    @task()
    def load_reviews(
        positive_rows: list[dict[str, Any]],
        negative_rows: list[dict[str, Any]],
    ) -> dict[str, int]:
        all_rows = positive_rows + negative_rows
        if not all_rows:
            return {"total_loaded": 0, "positive": 0, "negative": 0}

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS public.steam_reviews_sentiment (
            review_id TEXT PRIMARY KEY,
            review_text TEXT NOT NULL,
            polarity TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL
        );
        """

        upsert_sql = """
        INSERT INTO public.steam_reviews_sentiment (
            review_id,
            review_text,
            polarity,
            created_at
        ) VALUES (%s, %s, %s, to_timestamp(%s))
        ON CONFLICT (review_id) DO UPDATE
        SET
            review_text = EXCLUDED.review_text,
            polarity = EXCLUDED.polarity,
            created_at = EXCLUDED.created_at;
        """

        hook = PostgresHook(postgres_conn_id="DATA-DB")
        conn = hook.get_conn()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                cursor.executemany(
                    upsert_sql,
                    [
                        (
                            row["review_id"],
                            row["review_text"],
                            row["polarity"],
                            row["created_at_ts"],
                        )
                        for row in all_rows
                    ],
                )

        return {
            "total_loaded": len(all_rows),
            "positive": len(positive_rows),
            "negative": len(negative_rows),
        }

    positive = fetch_positive()
    negative = fetch_negative()
    load_reviews(positive, negative)


steam_reviews_sentiment_dag()

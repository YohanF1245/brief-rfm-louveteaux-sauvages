FROM python:3.11-slim

WORKDIR /usr/app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=/usr/app

RUN pip install --no-cache-dir \
    "dbt-core>=1.9,<2" \
    "dbt-clickhouse>=1.8,<2" \
    "dbt-postgres>=1.9,<2"

COPY . .

CMD ["sleep", "infinity"]

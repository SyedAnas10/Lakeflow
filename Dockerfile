FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir . && useradd --create-home --uid 1000 app

COPY alembic.ini ./
COPY migrations ./migrations
COPY contracts ./contracts

ENV LAKEFLOW_WORK_DIR=/tmp/lakeflow \
    PYTHONUNBUFFERED=1
USER app
CMD ["lakeflow", "api"]

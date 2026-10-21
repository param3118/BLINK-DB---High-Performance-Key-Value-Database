FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    BLINKDB_DATA_DIR=/data \
    BLINKDB_BIND=0.0.0.0

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY clients ./clients

RUN python -m pip install --no-cache-dir .

RUN useradd --system --uid 10001 --create-home blinkdb \
    && mkdir /data \
    && chown blinkdb:blinkdb /data

USER blinkdb

VOLUME ["/data"]
EXPOSE 6379 8080

ENTRYPOINT ["blinkdbd"]
CMD ["--data-dir", "/data", "--bind", "0.0.0.0"]

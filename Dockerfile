# ── Stage 1: build wheel ──────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build
COPY pyproject.toml ./
COPY src/ ./src/

RUN pip install --no-cache-dir --upgrade pip build && \
    python -m build --wheel

# Install into a virtual env so we can copy it cleanly
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir dist/*.whl

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.12-slim

ARG FLOWBYTE_VERSION=0.0.0

RUN groupadd -g 1000 flowbyte && \
    useradd -u 1000 -g 1000 -m -s /bin/bash flowbyte && \
    mkdir -p /etc/flowbyte/pipelines /var/log/flowbyte /app && \
    chown -R flowbyte:flowbyte /etc/flowbyte /var/log/flowbyte /app

COPY --from=builder /opt/venv /opt/venv
COPY --chown=flowbyte:flowbyte alembic/ /app/alembic/
COPY --chown=flowbyte:flowbyte alembic.ini /app/alembic.ini
COPY --chown=flowbyte:flowbyte docker/entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    FLOWBYTE_LOG_FORMAT=json \
    FLOWBYTE_LOG_LEVEL=INFO \
    FLOWBYTE_VERSION=${FLOWBYTE_VERSION}

USER flowbyte
WORKDIR /app

HEALTHCHECK --interval=30s --timeout=10s --start-period=180s --retries=3 \
    CMD flowbyte health --strict || exit 1

ENTRYPOINT ["/app/entrypoint.sh"]
CMD []

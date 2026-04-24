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

RUN groupadd -g 1000 flowbyte && \
    useradd -u 1000 -g 1000 -m -s /bin/bash flowbyte && \
    mkdir -p /etc/flowbyte/pipelines /var/log/flowbyte && \
    chown -R flowbyte:flowbyte /etc/flowbyte /var/log/flowbyte

COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    FLOWBYTE_LOG_FORMAT=json \
    FLOWBYTE_LOG_LEVEL=INFO

USER flowbyte
WORKDIR /app

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD flowbyte health || exit 1

ENTRYPOINT ["flowbyte"]
CMD ["daemon"]

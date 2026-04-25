#!/bin/bash
set -euo pipefail

_send_alert() {
    local msg="$1"
    [[ -z "${TELEGRAM_BOT_TOKEN:-}" || -z "${TELEGRAM_CHAT_ID:-}" ]] && return 0
    curl -sS -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
      --data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" \
      --data-urlencode "text=🚨 Flowbyte deployment FAILED: ${msg}" || true
}

trap '_send_alert "entrypoint error at line $LINENO"' ERR

echo "==> alembic upgrade head"
if ! alembic -c /app/alembic.ini upgrade head; then
    _send_alert "alembic upgrade head failed — container will exit"
    exit 1
fi

echo "==> log deployment event"
flowbyte log-deployment --version="${FLOWBYTE_VERSION:-unknown}" || true

echo "==> starting daemon"
exec flowbyte daemon

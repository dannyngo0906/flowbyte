"""Telegram Bot API alerter with retry on network failure."""
from __future__ import annotations

import re

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from flowbyte.alerting.deduper import AlertDeduper
from flowbyte.logging import EventName, get_logger

log = get_logger()

_TELEGRAM_API = "https://api.telegram.org"
_BOT_TOKEN_RE = re.compile(r"^\d+:[A-Za-z0-9_-]{10,}$")
_TELEGRAM_MAX_TEXT = 4096


class TelegramAlerter:
    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        deduper: AlertDeduper | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        if not _BOT_TOKEN_RE.match(bot_token):
            raise ValueError(
                "Invalid bot_token format — expected '<numeric_id>:<hash>' "
                "(only digits, letters, underscores, hyphens allowed in hash)"
            )
        self._url = f"{_TELEGRAM_API}/bot{bot_token}/sendMessage"
        # Pre-compute masked URL so token never appears in log/error strings.
        tok_id = bot_token.split(":")[0]
        self._safe_url = f"{_TELEGRAM_API}/bot{tok_id}:***/sendMessage"
        self._chat_id = chat_id
        self._deduper = deduper or AlertDeduper()
        self._client = http_client or httpx.Client(timeout=10.0)

    def send(self, text: str, key: str = "", pipeline: str = "") -> bool:
        """Send message. Returns True on success. Never raises (swallows network errors)."""
        if key and not self._deduper.should_send(key, pipeline):
            log.debug(EventName.ALERT_SKIPPED_DEDUP, key=key)
            return False

        try:
            self._send_with_retry(text)
            log.info(EventName.ALERT_SENT, pipeline=pipeline, key=key)
            return True
        except Exception as e:
            # Replace raw URL (which contains bot_token) with masked version before logging.
            safe_err = str(e).replace(self._url, self._safe_url)
            log.error(EventName.ALERT_FAILED, error=safe_err)
            return False

    @retry(
        retry=retry_if_exception_type(httpx.NetworkError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _send_with_retry(self, text: str) -> None:
        response = self._client.post(
            self._url,
            json={
                "chat_id": self._chat_id,
                "text": text[:_TELEGRAM_MAX_TEXT],
                "parse_mode": "Markdown",
            },
        )
        response.raise_for_status()

    def test(self) -> bool:
        """Send a test message. Raises on failure."""
        self._send_with_retry("🟢 Flowbyte alert test — connection OK")
        return True


def format_sync_fail_alert(pipeline: str, resource: str, error: str, sync_id: str) -> str:
    safe_error = error[:300].replace("`", "'").replace("*", "").replace("_", " ")
    return (
        f"🔴 *Sync FAILED*\n"
        f"📦 Pipeline: `{pipeline}`\n"
        f"📊 Resource: `{resource}`\n"
        f"💬 Error: `{safe_error}`\n"
        f"🔗 `flowbyte inspect {sync_id[:8]}`"
    )


def format_validation_alert(
    pipeline: str,
    resource: str,
    failed_rules: list[str],
    sync_id: str,
) -> str:
    safe_rules = ", ".join(r.replace("*", "").replace("_", " ") for r in failed_rules)
    return (
        f"⚠️ *Validation FAILED*\n"
        f"📦 Pipeline: `{pipeline}`\n"
        f"📊 Resource: `{resource}`\n"
        f"📋 Rules: `{safe_rules}`\n"
        f"🔗 `flowbyte inspect {sync_id[:8]}`"
    )


def format_scheduler_dead_alert(last_beat: str, age_hours: float) -> str:
    return (
        f"🔴 *Scheduler DEAD*\n"
        f"💤 Last heartbeat: `{last_beat}` ({age_hours:.1f}h ago)\n"
        f"⚠️ Sync jobs are NOT running.\n"
        f"Check: `docker compose logs flowbyte --tail 100`"
    )

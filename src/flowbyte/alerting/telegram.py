"""Telegram Bot API alerter with retry on network failure."""
from __future__ import annotations

import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from flowbyte.alerting.deduper import AlertDeduper
from flowbyte.logging import EventName, get_logger

log = get_logger()

_TELEGRAM_API = "https://api.telegram.org"


class TelegramAlerter:
    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        deduper: AlertDeduper | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._url = f"{_TELEGRAM_API}/bot{bot_token}/sendMessage"
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
            log.error(EventName.ALERT_FAILED, error=str(e))
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
            json={"chat_id": self._chat_id, "text": text, "parse_mode": "Markdown"},
        )
        response.raise_for_status()

    def test(self) -> bool:
        """Send a test message. Raises on failure."""
        self._send_with_retry("🟢 Flowbyte alert test — connection OK")
        return True


def format_sync_fail_alert(pipeline: str, resource: str, error: str, sync_id: str) -> str:
    safe_error = error[:300].replace("`", "'")
    return (
        f"🔴 *Sync FAILED*\n"
        f"📦 Pipeline: `{pipeline}`\n"
        f"📊 Resource: `{resource}`\n"
        f"💬 Error: `{safe_error}`\n"
        f"🔗 `flowbyte inspect {sync_id[:8]}`"
    )


def format_scheduler_dead_alert(last_beat: str, age_hours: float) -> str:
    return (
        f"🔴 *Scheduler DEAD*\n"
        f"💤 Last heartbeat: `{last_beat}` ({age_hours:.1f}h ago)\n"
        f"⚠️ Sync jobs are NOT running.\n"
        f"Check: `docker compose logs flowbyte --tail 100`"
    )

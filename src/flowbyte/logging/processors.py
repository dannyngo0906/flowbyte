"""structlog processors: deep redact of sensitive values."""
from __future__ import annotations

import re
from typing import Any

_SENSITIVE_KEY_RE = re.compile(
    r"(?i)(token|password|secret|api_key|access_token|authorization|ciphertext)"
)
_SENSITIVE_VAL_RE = re.compile(
    r"(?i)(token|password|secret|api_key|bearer)[\"']?\s*[:=]\s*[\"']?([^\s\"',&]{8,})"
)


def _redact_str(value: str) -> str:
    return _SENSITIVE_VAL_RE.sub(
        lambda m: f"{m.group(1)}=***{m.group(2)[-4:]}", value
    )


def deep_redact(obj: Any) -> Any:
    """Recursively redact sensitive keys and value patterns."""
    if isinstance(obj, str):
        return _redact_str(obj)
    if isinstance(obj, dict):
        return {
            k: "***REDACTED***" if _SENSITIVE_KEY_RE.search(k) else deep_redact(v)
            for k, v in obj.items()
        }
    if isinstance(obj, (list, tuple)):
        return type(obj)(deep_redact(v) for v in obj)
    return obj


def redact_processor(
    logger: Any, method_name: str, event_dict: dict
) -> dict:
    return deep_redact(event_dict)

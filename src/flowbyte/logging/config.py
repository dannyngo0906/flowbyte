"""structlog configuration with 3 sinks: console, JSON stdout, DB."""
from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.types import EventDict, WrappedLogger

from flowbyte.logging.processors import redact_processor


def configure_logging(
    log_level: str = "INFO",
    env: str = "prod",
    db_sink: Any | None = None,
) -> None:
    """Configure structlog + stdlib logging bridge.

    Call once at daemon startup or CLI entry.
    """
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        redact_processor,
    ]

    if env == "dev":
        renderer = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    sink_processors: list[Any] = []
    if db_sink is not None:
        sink_processors.append(db_sink)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            *sink_processors,
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=shared_processors,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level.upper())

    # Quiet noisy libraries
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def get_logger(name: str = "flowbyte") -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)

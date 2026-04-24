"""SQLAlchemy engine factory."""
from __future__ import annotations

from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@lru_cache(maxsize=1)
def get_internal_engine(db_url: str | None = None) -> Engine:
    from flowbyte.config.models import AppSettings

    url = db_url or AppSettings().db_url
    return create_engine(
        url,
        pool_size=5,
        max_overflow=5,
        pool_timeout=30,
        pool_pre_ping=True,
        echo=False,
    )


def get_dest_engine(dest_url: str) -> Engine:
    return create_engine(
        dest_url,
        pool_size=5,
        max_overflow=5,
        pool_timeout=30,
        pool_pre_ping=True,
        echo=False,
    )

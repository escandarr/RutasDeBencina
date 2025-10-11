"""Connection utilities wrapping psycopg connection pools."""
from __future__ import annotations

import contextlib
from typing import Generator

import psycopg  # type: ignore[import-not-found]
from psycopg_pool import ConnectionPool  # type: ignore[import-not-found]

from .config import DBConfig


class Database:
    """Thin wrapper around a psycopg connection pool."""

    def __init__(self, dsn: str | None = None, *, min_size: int = 1, max_size: int = 10) -> None:
        cfg = DBConfig.from_env() if dsn is None else DBConfig(dsn=dsn)
        self._pool = ConnectionPool(cfg.dsn, min_size=min_size, max_size=max_size)

    @contextlib.contextmanager
    def connection(self) -> Generator[psycopg.Connection, None, None]:
        with self._pool.connection() as conn:
            yield conn

    @contextlib.contextmanager
    def cursor(self) -> Generator[psycopg.Cursor, None, None]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                yield cur

    def close(self) -> None:
        self._pool.close()

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

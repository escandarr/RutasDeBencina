"""Configuration helpers for database connectivity."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DBConfig:
    """Resolved connection settings for PostgreSQL."""

    dsn: str

    @classmethod
    def from_env(cls) -> "DBConfig":
        dsn = os.getenv("DATABASE_URL")
        if dsn:
            return cls(dsn=dsn)

        host = os.getenv("PGHOST", os.getenv("POSTGRES_HOST", "localhost"))
        port = os.getenv("PGPORT", os.getenv("POSTGRES_PORT", "5432"))
        dbname = os.getenv("PGDATABASE", os.getenv("POSTGRES_DB", "rutasdb"))
        user = os.getenv("PGUSER", os.getenv("POSTGRES_USER", "rutas_user"))
        password = os.getenv("PGPASSWORD", os.getenv("POSTGRES_PASSWORD", "supersecretpassword"))

        parts = [
            f"host={host}",
            f"port={port}",
            f"dbname={dbname}",
            f"user={user}",
        ]
        if password:
            parts.append(f"password={password}")
        return cls(dsn=" ".join(parts))

"""The single source of PostgreSQL connection settings.

Every component reads its database settings from here. Nothing else in the
codebase should call ``os.getenv("POSTGRES_*")`` -- before this module existed,
five call sites each built their own parameter dict with four different
``POSTGRES_DB`` fallbacks, so an unset variable sent different components to
different databases.

``dbname`` is the canonical key, matching psycopg2. Passing ``database``
alongside it raises TypeError inside psycopg2, which is what the old
``PersistenceService`` did to any caller using psycopg2's own spelling.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from dotenv import load_dotenv

from db.errors import ConfigurationError

# Components are separate processes started from a variety of entry points, and
# only ingestion/ used to load .env -- so the drift service silently fell back
# to defaults when run from the host. Loading here covers every component,
# because every component reaches the database through this module.
load_dotenv()

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432
DEFAULT_DBNAME = "sentineldq"
DEFAULT_USER = "sentineldq"

# Fail rather than hang if Postgres is unreachable or a query runs away.
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_STATEMENT_TIMEOUT_MS = 30_000


@dataclass(frozen=True)
class DatabaseConfig:
    """Everything needed to open a connection."""

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    dbname: str = DEFAULT_DBNAME
    user: str = DEFAULT_USER
    password: str = ""
    connect_timeout: int = DEFAULT_CONNECT_TIMEOUT_SECONDS
    statement_timeout_ms: int = DEFAULT_STATEMENT_TIMEOUT_MS
    application_name: str = "sentineldq"

    @classmethod
    def from_env(
        cls,
        application_name: str = "sentineldq",
        overrides: Optional[Dict[str, Any]] = None,
    ) -> "DatabaseConfig":
        """Build a config from POSTGRES_* environment variables.

        Args:
            application_name: shows up in pg_stat_activity, so a connection can
                be traced back to the component that opened it.
            overrides: explicit values winning over the environment. Used by
                tests and by callers that already know their connection params.
        """
        overrides = dict(overrides or {})
        if "database" in overrides:
            raise ConfigurationError(
                "Use 'dbname', not 'database'. psycopg2 rejects both being passed together."
            )

        # An absent password is a misconfiguration; a deliberately empty one is
        # how trust auth is spelled. Distinguishing the two turns a confusing
        # authentication failure into a clear configuration error.
        password = overrides.pop("password", None)
        if password is None:
            password = os.getenv("POSTGRES_PASSWORD")
        if password is None:
            raise ConfigurationError(
                "POSTGRES_PASSWORD is not set. Set it in .env (see .env.example), "
                'or set it to the empty string explicitly if this database uses trust authentication.'
            )

        config = cls(
            host=overrides.pop("host", None) or os.getenv("POSTGRES_HOST", DEFAULT_HOST),
            port=_read_port(overrides.pop("port", None)),
            dbname=overrides.pop("dbname", None) or os.getenv("POSTGRES_DB", DEFAULT_DBNAME),
            user=overrides.pop("user", None) or os.getenv("POSTGRES_USER", DEFAULT_USER),
            password=password,
            connect_timeout=int(
                overrides.pop("connect_timeout", None)
                or os.getenv("POSTGRES_CONNECT_TIMEOUT", DEFAULT_CONNECT_TIMEOUT_SECONDS)
            ),
            statement_timeout_ms=int(
                overrides.pop("statement_timeout_ms", None)
                or os.getenv("POSTGRES_STATEMENT_TIMEOUT_MS", DEFAULT_STATEMENT_TIMEOUT_MS)
            ),
            application_name=overrides.pop("application_name", None) or application_name,
        )

        if overrides:
            raise ConfigurationError(
                f"Unknown connection settings: {', '.join(sorted(overrides))}"
            )
        return config

    def connect_kwargs(self) -> Dict[str, Any]:
        """Keyword arguments for ``psycopg2.connect``."""
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "connect_timeout": self.connect_timeout,
            "application_name": self.application_name,
            "options": f"-c statement_timeout={self.statement_timeout_ms}",
        }

    def __str__(self) -> str:
        """Describe the target without leaking the password."""
        return f"{self.user}@{self.host}:{self.port}/{self.dbname}"


def _read_port(override: Optional[Any]) -> int:
    raw = override if override is not None else os.getenv("POSTGRES_PORT", DEFAULT_PORT)
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigurationError(f"POSTGRES_PORT must be an integer, got {raw!r}") from exc

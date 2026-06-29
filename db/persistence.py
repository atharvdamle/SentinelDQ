"""Core persistence service abstraction for all persistence modules."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, Tuple

import psycopg2

logger = logging.getLogger(__name__)


class PersistenceService:
    """Single persistence service that owns connection lifecycle and transaction control."""

    def __init__(self, connection_params: Optional[Dict[str, Any]] = None):
        self.connection_params = self._build_connection_params(connection_params)
        self.connection = None

    @staticmethod
    def _build_connection_params(
        connection_params: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        params = dict(connection_params or {})
        if not params:
            params = {
                "host": os.getenv("POSTGRES_HOST", "localhost"),
                "port": int(os.getenv("POSTGRES_PORT", 5432)),
                "database": os.getenv("POSTGRES_DB", "SentinelDQ_DB"),
                "user": os.getenv("POSTGRES_USER", "postgres"),
                "password": os.getenv("POSTGRES_PASSWORD", ""),
            }
        else:
            params.setdefault("host", os.getenv("POSTGRES_HOST", "localhost"))
            params.setdefault("port", int(os.getenv("POSTGRES_PORT", 5432)))
            params.setdefault("database", os.getenv("POSTGRES_DB", "SentinelDQ_DB"))
            params.setdefault("user", os.getenv("POSTGRES_USER", "postgres"))
            params.setdefault("password", os.getenv("POSTGRES_PASSWORD", ""))
        return params

    def connect(self) -> None:
        """Open a PostgreSQL connection and enable transaction control."""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.connection.autocommit = False
            logger.info(
                "Connected to PostgreSQL at %s:%s/%s",
                self.connection_params.get("host"),
                self.connection_params.get("port"),
                self.connection_params.get("database"),
            )
        except psycopg2.Error as exc:
            logger.error("Failed to connect to PostgreSQL: %s", exc)
            raise ConnectionError(f"Failed to connect to PostgreSQL: {exc}") from exc

    def disconnect(self) -> None:
        """Close the active database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Closed PostgreSQL connection")

    def get_connection(self):
        """Return an active connection, opening one if needed."""
        if not self.connection:
            self.connect()
        return self.connection

    def commit(self) -> None:
        if self.connection:
            self.connection.commit()

    def rollback(self) -> None:
        if self.connection:
            self.connection.rollback()

    def fetch_all(self, query: str, params: Optional[Tuple[Any, ...]] = None):
        connection = self.get_connection()
        with connection.cursor() as cursor:
            cursor.execute(query, params or ())
            return cursor.fetchall()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

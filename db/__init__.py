"""PostgreSQL persistence for SentinelDQ.

Typical use::

    from db import init_schema, transaction, DriftRepository

    init_schema()                      # once, at startup
    DriftRepository().save_many(rows)  # commits, or rolls back and raises

``transaction()`` is the primitive underneath all of it: it commits on clean
exit and rolls back on any exception, so a failed statement can never leave a
connection in an aborted state for the next caller to trip over.
"""

from db.config import DatabaseConfig
from db.errors import (
    ConfigurationError,
    ConnectionFailed,
    DatabaseError,
    QueryFailed,
)
from db.pool import (
    close_pool,
    configure,
    fetch_all,
    fetch_iter,
    get_connection,
    transaction,
)
from db.repositories import DriftRepository, EventRepository, ValidationRepository
from db.schema import init_schema

__all__ = [
    "DatabaseConfig",
    "DatabaseError",
    "ConfigurationError",
    "ConnectionFailed",
    "QueryFailed",
    "configure",
    "close_pool",
    "get_connection",
    "transaction",
    "fetch_all",
    "fetch_iter",
    "init_schema",
    "EventRepository",
    "DriftRepository",
    "ValidationRepository",
]

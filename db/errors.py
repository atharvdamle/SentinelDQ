"""Exception types for the persistence layer.

Every error raised out of ``db`` is one of these, and every one that wraps a
psycopg2 failure keeps the original exception on ``__cause__`` so callers can
still branch on the psycopg2 type (``UniqueViolation``, ``OperationalError``,
...) when they need to.
"""

from __future__ import annotations


class DatabaseError(Exception):
    """Base class for every error raised by the db package."""


class ConfigurationError(DatabaseError):
    """Connection settings are missing or malformed."""


class ConnectionFailed(DatabaseError):
    """Could not establish or reuse a PostgreSQL connection."""


class QueryFailed(DatabaseError):
    """A statement failed. The transaction has already been rolled back."""

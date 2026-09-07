"""Connection pooling and the transaction boundary.

Two things live here, and everything that touches PostgreSQL goes through one
of them:

``get_connection()``  borrow a connection, return it to the pool afterwards.
``transaction()``     borrow a connection and a cursor, commit on success,
                      **roll back on any exception**.

The rollback is the point. The repositories used to run ``execute`` then
``commit`` with no try/except at all, so a single failed insert left the shared
connection in an aborted transaction and every later write on it failed the
same way -- while the callers swallowed the exception and logged a warning.
Routing every write through ``transaction()`` makes that state unreachable.
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence, Tuple

import psycopg2
from psycopg2 import pool as psycopg2_pool

from db.config import DatabaseConfig
from db.errors import ConnectionFailed, QueryFailed

logger = logging.getLogger(__name__)

DEFAULT_MIN_CONNECTIONS = 1
DEFAULT_MAX_CONNECTIONS = 10

_pool: Optional[psycopg2_pool.ThreadedConnectionPool] = None
_pool_config: Optional[DatabaseConfig] = None
_lock = threading.Lock()


def configure(
    config: Optional[DatabaseConfig] = None,
    minconn: int = DEFAULT_MIN_CONNECTIONS,
    maxconn: int = DEFAULT_MAX_CONNECTIONS,
) -> None:
    """Create the process-wide pool. Idempotent; call once at startup."""
    global _pool, _pool_config
    with _lock:
        if _pool is not None:
            return
        config = config or DatabaseConfig.from_env()
        try:
            _pool = psycopg2_pool.ThreadedConnectionPool(
                minconn, maxconn, **config.connect_kwargs()
            )
        except psycopg2.Error as exc:
            raise ConnectionFailed(f"Could not connect to PostgreSQL at {config}: {exc}") from exc
        _pool_config = config
        logger.info("PostgreSQL pool ready (%s, %s-%s connections)", config, minconn, maxconn)


def close_pool() -> None:
    """Close every pooled connection. For shutdown hooks and tests."""
    global _pool, _pool_config
    with _lock:
        if _pool is not None:
            _pool.closeall()
            logger.info("Closed PostgreSQL pool")
        _pool = None
        _pool_config = None


def _get_pool() -> psycopg2_pool.ThreadedConnectionPool:
    if _pool is None:
        configure()
    assert _pool is not None  # configure() either sets it or raises
    return _pool


@contextmanager
def get_connection() -> Iterator[Any]:
    """Borrow a connection and return it to the pool afterwards.

    A connection the server has terminated is discarded rather than handed
    back, so the pool re-establishes it. The old code checked truthiness of the
    connection object -- which stays truthy after close() -- and so returned
    dead connections forever once Postgres restarted.
    """
    connection_pool = _get_pool()
    try:
        connection = connection_pool.getconn()
    except psycopg2.Error as exc:
        raise ConnectionFailed(f"Could not borrow a connection from the pool: {exc}") from exc

    if connection.closed:
        connection_pool.putconn(connection, close=True)
        try:
            connection = connection_pool.getconn()
        except psycopg2.Error as exc:
            raise ConnectionFailed(f"Could not replace a closed connection: {exc}") from exc

    discard = False
    try:
        yield connection
    except psycopg2.OperationalError:
        # The connection itself is suspect, not just the statement.
        discard = True
        raise
    finally:
        connection_pool.putconn(connection, close=discard or connection.closed)


@contextmanager
def transaction() -> Iterator[Any]:
    """Borrow a cursor. Commit on clean exit, roll back on any exception."""
    with get_connection() as connection:
        cursor = connection.cursor()
        try:
            yield cursor
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()


def fetch_all(query: str, params: Optional[Sequence[Any]] = None) -> list:
    """Run a SELECT and return every row.

    Wrapped in ``transaction()`` so the read is committed rather than left
    open -- an uncommitted SELECT on a non-autocommit connection pins a
    snapshot and holds back VACUUM for as long as the connection lives.
    """
    try:
        with transaction() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    except psycopg2.Error as exc:
        raise QueryFailed(f"Query failed: {exc}") from exc


def fetch_iter(
    query: str,
    params: Optional[Sequence[Any]] = None,
    itersize: int = 2000,
) -> Iterator[Tuple[Any, ...]]:
    """Stream rows through a server-side cursor.

    For result sets too large to materialize -- the drift engine's baseline
    window is seven days of JSONB events, which ``fetch_all`` would pull into
    memory in one list.
    """
    try:
        with get_connection() as connection:
            # A named cursor is server-side; the name only needs to be unique
            # within the session.
            cursor = connection.cursor(name="sentineldq_stream")
            cursor.itersize = itersize
            try:
                cursor.execute(query, params)
                yield from cursor
            finally:
                cursor.close()
                connection.commit()
    except psycopg2.Error as exc:
        raise QueryFailed(f"Streaming query failed: {exc}") from exc

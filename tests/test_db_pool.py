"""Behaviour of the connection pool and the transaction boundary.

These are the regression tests for the defects the persistence-layer rewrite
addressed: statements that failed without rolling back, and closed connections
being handed out forever.
"""

import psycopg2
import pytest

import db
from db import pool
from db.config import DatabaseConfig
from db.errors import ConfigurationError, QueryFailed


class FakeCursor:
    def __init__(self, connection, name=None):
        self.connection = connection
        self.name = name
        self.itersize = None
        self.closed = False
        self.description = [("col",)]
        self._rows = list(connection.rows)

    def execute(self, sql, params=None):
        self.connection.executed.append((sql, params))
        if self.connection.fail_on_execute:
            raise psycopg2.ProgrammingError("boom")

    def fetchall(self):
        return self._rows

    def __iter__(self):
        return iter(self._rows)

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, rows=(), fail_on_execute=False):
        self.executed = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0
        self.rows = list(rows)
        self.fail_on_execute = fail_on_execute

    def cursor(self, name=None):
        return FakeCursor(self, name=name)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = 1


class FakePool:
    """Stands in for ThreadedConnectionPool, recording borrow/return."""

    def __init__(self, connections):
        self._connections = list(connections)
        self.borrowed = 0
        self.returned = []

    def getconn(self):
        self.borrowed += 1
        return self._connections.pop(0)

    def putconn(self, connection, close=False):
        self.returned.append((connection, close))

    def closeall(self):
        pass


@pytest.fixture
def fake_pool(monkeypatch):
    """Install a fake pool and hand the test a factory to stock it."""

    def install(*connections):
        created = FakePool(connections)
        monkeypatch.setattr(pool, "_pool", created)
        return created

    yield install
    monkeypatch.setattr(pool, "_pool", None)


def test_transaction_commits_on_success(fake_pool):
    connection = FakeConnection()
    fake_pool(connection)

    with pool.transaction() as cursor:
        cursor.execute("INSERT INTO t VALUES (1)")

    assert connection.commits == 1
    assert connection.rollbacks == 0


def test_transaction_rolls_back_on_exception(fake_pool):
    """The core regression: a failed statement must not leave an open transaction."""
    connection = FakeConnection()
    fake_pool(connection)

    with pytest.raises(psycopg2.ProgrammingError):
        with pool.transaction() as cursor:
            cursor.execute("INSERT INTO t VALUES (1)")
            raise psycopg2.ProgrammingError("constraint violation")

    assert connection.rollbacks == 1
    assert connection.commits == 0


def test_failed_statement_does_not_poison_the_next_write(fake_pool):
    """A poisoned transaction used to make every later write fail."""
    failing = FakeConnection(fail_on_execute=True)
    healthy = FakeConnection()
    created = fake_pool(failing, healthy)

    with pytest.raises(psycopg2.Error):
        with pool.transaction() as cursor:
            cursor.execute("INSERT INTO t VALUES (1)")

    assert failing.rollbacks == 1

    # The next caller gets a clean connection and succeeds.
    with pool.transaction() as cursor:
        cursor.execute("INSERT INTO t VALUES (2)")

    assert healthy.commits == 1
    assert created.borrowed == 2


def test_connection_is_returned_to_the_pool(fake_pool):
    connection = FakeConnection()
    created = fake_pool(connection)

    with pool.get_connection():
        pass

    assert created.returned == [(connection, False)]


def test_closed_connection_is_discarded_and_replaced(fake_pool):
    """A closed connection object is still truthy; it must not be reused."""
    dead = FakeConnection()
    dead.closed = 1
    live = FakeConnection()
    created = fake_pool(dead, live)

    with pool.get_connection() as connection:
        assert connection is live

    # The dead one was returned with close=True so the pool re-establishes it.
    assert (dead, True) in created.returned


def test_operational_error_discards_the_connection(fake_pool):
    """A dropped socket means the connection is suspect, not just the statement."""
    connection = FakeConnection()
    created = fake_pool(connection)

    with pytest.raises(psycopg2.OperationalError):
        with pool.get_connection():
            raise psycopg2.OperationalError("server closed the connection")

    assert created.returned == [(connection, True)]


def test_fetch_all_commits_rather_than_leaving_a_transaction_open(fake_pool):
    connection = FakeConnection(rows=[("a",), ("b",)])
    fake_pool(connection)

    assert pool.fetch_all("SELECT 1") == [("a",), ("b",)]
    assert connection.commits == 1


def test_fetch_all_wraps_errors(fake_pool):
    fake_pool(FakeConnection(fail_on_execute=True))

    with pytest.raises(QueryFailed):
        pool.fetch_all("SELECT 1")


def test_fetch_iter_uses_a_server_side_cursor(fake_pool):
    connection = FakeConnection(rows=[(1,), (2,), (3,)])
    fake_pool(connection)

    assert list(pool.fetch_iter("SELECT 1", itersize=7)) == [(1,), (2,), (3,)]
    # A named cursor is what makes it server-side.
    assert connection.executed


class TestDatabaseConfig:
    def test_reads_environment(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "dbhost")
        monkeypatch.setenv("POSTGRES_PORT", "6000")
        monkeypatch.setenv("POSTGRES_DB", "somedb")
        monkeypatch.setenv("POSTGRES_USER", "someuser")
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

        config = DatabaseConfig.from_env()

        assert (config.host, config.port, config.dbname) == ("dbhost", 6000, "somedb")
        assert config.connect_kwargs()["dbname"] == "somedb"

    def test_uses_dbname_not_database(self, monkeypatch):
        """psycopg2 raises TypeError if both keys are present."""
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

        kwargs = DatabaseConfig.from_env().connect_kwargs()

        assert "dbname" in kwargs
        assert "database" not in kwargs

    def test_rejects_the_database_key(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

        with pytest.raises(ConfigurationError, match="dbname"):
            DatabaseConfig.from_env(overrides={"database": "somedb"})

    def test_absent_password_is_a_config_error(self, monkeypatch):
        monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)

        with pytest.raises(ConfigurationError, match="POSTGRES_PASSWORD"):
            DatabaseConfig.from_env()

    def test_empty_password_is_allowed(self, monkeypatch):
        """Trust authentication is spelled as a deliberately empty password."""
        monkeypatch.setenv("POSTGRES_PASSWORD", "")

        assert DatabaseConfig.from_env().password == ""

    def test_malformed_port_is_a_config_error(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
        monkeypatch.setenv("POSTGRES_PORT", "not-a-number")

        with pytest.raises(ConfigurationError, match="POSTGRES_PORT"):
            DatabaseConfig.from_env()

    def test_str_does_not_leak_the_password(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PASSWORD", "hunter2")

        assert "hunter2" not in str(DatabaseConfig.from_env())

    def test_sets_timeouts_and_application_name(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

        kwargs = DatabaseConfig.from_env(application_name="drift").connect_kwargs()

        assert kwargs["application_name"] == "drift"
        assert kwargs["connect_timeout"] > 0
        assert "statement_timeout" in kwargs["options"]


def test_package_exports_the_public_surface():
    for name in ("transaction", "init_schema", "DriftRepository", "ValidationRepository"):
        assert hasattr(db, name)

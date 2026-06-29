from db import DriftPostgresWriter
from db import PersistenceService
from db.repositories import DriftRepository, ValidationRepository


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.connection.executed.append((sql, params))


class FakeConnection:
    def __init__(self):
        self.autocommit = False
        self.executed = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


def test_validation_and_drift_repositories_share_core_connector(monkeypatch):
    fake_connection = FakeConnection()

    def fake_connect(**kwargs):
        return fake_connection

    monkeypatch.setattr("db.postgres_writer.psycopg2.connect", fake_connect)

    assert hasattr(
        __import__("db", fromlist=["ValidationRepository"]), "ValidationRepository"
    )

    drift_writer = DriftPostgresWriter(
        {"host": "localhost", "database": "db", "user": "u", "password": "p"}
    )
    drift_writer.connect()

    assert isinstance(drift_writer, PersistenceService)

    validation_repo = ValidationRepository(
        {"host": "localhost", "database": "db", "user": "u", "password": "p"}
    )
    drift_repo = DriftRepository(
        {"host": "localhost", "database": "db", "user": "u", "password": "p"}
    )

    assert isinstance(validation_repo, PersistenceService)
    assert isinstance(drift_repo, PersistenceService)
    assert drift_writer.connection is fake_connection

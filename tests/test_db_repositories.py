"""SQL emitted by the repositories, and the schema they write into."""

from datetime import datetime, timezone

import psycopg2
import pytest
from psycopg2.extras import Json

from db import repositories, schema
from db.errors import QueryFailed


class RecordingCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = [("drift_id",)] + [(c,) for c in repositories.DriftRepository.COLUMNS]

    def execute(self, sql, params=None):
        self.connection.executed.append((sql, params))
        if self.connection.fail:
            raise psycopg2.IntegrityError("duplicate key")

    def fetchall(self):
        return self.connection.rows

    def close(self):
        pass


class RecordingConnection:
    def __init__(self, rows=(), fail=False):
        self.executed = []
        self.commits = 0
        self.rollbacks = 0
        self.rows = list(rows)
        self.fail = fail

    def cursor(self, name=None):
        return RecordingCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        pass


@pytest.fixture
def connection(monkeypatch):
    """Route transaction() at a recording connection.

    ``execute_values`` is intercepted rather than driven for real: it needs a
    live psycopg2 cursor to mogrify against, and what matters here is the SQL
    and the rows this code hands it.
    """
    from contextlib import contextmanager

    created = RecordingConnection()

    @contextmanager
    def fake_transaction():
        cursor = created.cursor()
        try:
            yield cursor
            created.commits += 1
        except Exception:
            created.rollbacks += 1
            raise

    def fake_execute_values(cursor, sql, rows, page_size=100, template=None):
        cursor.execute(sql, list(rows))

    monkeypatch.setattr(repositories, "transaction", fake_transaction)
    monkeypatch.setattr(repositories, "execute_values", fake_execute_values)
    monkeypatch.setattr(schema, "transaction", fake_transaction)
    return created


def drift_row(**overrides):
    now = datetime.now(timezone.utc)
    row = {
        "drift_type": "volume",
        "entity": "global",
        "field_name": None,
        "baseline_start": now,
        "baseline_end": now,
        "current_start": now,
        "current_end": now,
        "metric_name": "z_score",
        "baseline_value": {"count": 100},
        "current_value": {"count": 250},
        "drift_score": 0.8,
        "severity": "CRITICAL",
        "detected_at": now,
        "metadata": {"note": "spike"},
    }
    row.update(overrides)
    return row


class TestDriftRepository:
    def test_save_many_issues_one_statement_and_one_commit(self, connection):
        count = repositories.DriftRepository().save_many([drift_row() for _ in range(5)])

        assert count == 5
        # Previously this was one INSERT and one COMMIT per row.
        assert len(connection.executed) == 1
        assert connection.commits == 1

    def test_save_many_on_empty_input_touches_nothing(self, connection):
        assert repositories.DriftRepository().save_many([]) == 0
        assert connection.executed == []

    def test_json_columns_are_adapted_not_pre_serialized(self, connection):
        """Values reach psycopg2 as Json(), so reads and writes stay symmetric."""
        repositories.DriftRepository().save_many([drift_row()])

        _, params = connection.executed[0]
        row = params[0] if isinstance(params, (list, tuple)) else params
        columns = repositories.DriftRepository.COLUMNS
        baseline = row[columns.index("baseline_value")]
        assert isinstance(baseline, Json)
        assert baseline.adapted == {"count": 100}

    def test_none_json_stays_null(self, connection):
        repositories.DriftRepository().save_many([drift_row(baseline_value=None)])

        _, params = connection.executed[0]
        row = params[0]
        assert row[repositories.DriftRepository.COLUMNS.index("baseline_value")] is None

    def test_failure_rolls_back_and_raises_query_failed(self, connection):
        connection.fail = True

        with pytest.raises(QueryFailed):
            repositories.DriftRepository().save_many([drift_row()])

        assert connection.rollbacks == 1
        assert connection.commits == 0

    def test_critical_since_multiplies_the_interval(self, connection):
        """Written as INTERVAL '%s hours' the placeholder sits inside a quoted
        literal, which breaks for anything but an int."""
        repositories.DriftRepository().critical_since(24)

        sql, params = connection.executed[0]
        assert "INTERVAL '1 hour'" in sql
        assert "'%s hours'" not in sql
        assert params == (24,)

    def test_critical_since_accepts_a_string_hour_count(self, connection):
        repositories.DriftRepository().critical_since("24")

        sql, params = connection.executed[0]
        assert params == ("24",)

    def test_recent_orders_by_detection_time(self, connection):
        repositories.DriftRepository().recent(limit=10)

        sql, params = connection.executed[0]
        assert "ORDER BY detected_at DESC" in sql
        assert params == (10,)


class TestValidationRepository:
    def validation_row(self):
        return {
            "event_id": "evt-1",
            "source_table": "github_events",
            "status": "WARN",
            "failed_checks": ["type_check.actor.id"],
            "error_messages": ["expected int"],
            "severity": "WARNING",
            "validation_ts": datetime.now(timezone.utc),
            "processing_time_ms": 1.5,
            "metadata": {"rule_version": "1"},
            "failure_details": [{"check_name": "type_check.actor.id"}],
        }

    def test_upserts_on_event_id(self, connection):
        """Re-validating an event replaces its verdict rather than duplicating it."""
        repositories.ValidationRepository().save(self.validation_row())

        sql, _ = connection.executed[0]
        assert "ON CONFLICT (event_id) DO UPDATE" in sql

    def test_batch_is_one_statement(self, connection):
        rows = [dict(self.validation_row(), event_id=f"evt-{i}") for i in range(4)]

        assert repositories.ValidationRepository().save_many(rows) == 4
        assert len(connection.executed) == 1

    def test_duplicate_event_ids_in_one_batch_are_collapsed(self, connection):
        """ON CONFLICT DO UPDATE cannot touch the same row twice per statement."""
        rows = [self.validation_row(), dict(self.validation_row(), status="FAIL")]

        assert repositories.ValidationRepository().save_many(rows) == 1

        _, params = connection.executed[0]
        assert len(params) == 1
        # The last verdict wins.
        status = params[0][repositories.ValidationRepository.COLUMNS.index("status")]
        assert status == "FAIL"

    def test_timestamp_is_passed_as_a_datetime(self, connection):
        """It used to be sent as an isoformat string into a timestamp column."""
        repositories.ValidationRepository().save(self.validation_row())

        _, params = connection.executed[0]
        row = params[0]
        value = row[repositories.ValidationRepository.COLUMNS.index("validation_ts")]
        assert isinstance(value, datetime)


class TestEventRepository:
    def event(self, event_id="1"):
        return {
            "event_id": event_id,
            "raw": {"id": event_id, "type": "PushEvent"},
            "validation_status": "PASS",
            "columns": {
                "event_id": event_id,
                "event_type": "PushEvent",
                "repo_id": 1,
                "repo_name": "a/b",
                "repo_url": "https://example.invalid",
                "actor_id": 2,
                "actor_login": "someone",
                "actor_url": "https://example.invalid",
                "actor_avatar": "https://example.invalid",
                "payload_ref": None,
                "payload_head": None,
                "payload_before": None,
                "push_id": None,
                "public": True,
                "created_at": datetime.now(timezone.utc),
            },
        }

    def test_writes_both_tables_in_one_transaction(self, connection):
        count = repositories.EventRepository().save_batch(
            [self.event(str(i)) for i in range(3)]
        )

        assert count == 3
        assert len(connection.executed) == 2
        assert connection.commits == 1
        tables = " ".join(sql for sql, _ in connection.executed)
        assert "github_events" in tables and "github_events_processed" in tables

    def test_skips_duplicates(self, connection):
        repositories.EventRepository().save_batch([self.event()])

        for sql, _ in connection.executed:
            assert "ON CONFLICT (event_id) DO NOTHING" in sql

    def test_empty_batch_touches_nothing(self, connection):
        assert repositories.EventRepository().save_batch([]) == 0
        assert connection.executed == []


class TestSchema:
    def test_init_schema_creates_every_table(self, connection):
        schema.init_schema()

        ddl = " ".join(sql for sql, _ in connection.executed)
        for table in (
            "github_events",
            "github_events_processed",
            "drift_results",
            "validation_results",
        ):
            assert f"CREATE TABLE IF NOT EXISTS {table}" in ddl

    def test_init_schema_creates_the_indexes_that_were_missing(self, connection):
        schema.init_schema()

        ddl = " ".join(sql for sql, _ in connection.executed)
        # processed_at is the only column the drift query filters on; the drift
        # indexes existed only in the module the running path did not use.
        for index in (
            "idx_processed_at",
            "idx_drift_detected_at",
            "idx_drift_severity",
            "idx_drift_type_entity",
            "idx_validation_ts",
        ):
            assert index in ddl

    def test_timestamps_are_timezone_aware(self, connection):
        schema.init_schema()

        ddl = " ".join(sql for sql, _ in connection.executed)
        assert "TIMESTAMPTZ" in ddl
        # A naive TIMESTAMP column filled by now() stores server-local time
        # while the application writes UTC.
        assert "TIMESTAMP " not in ddl.replace("TIMESTAMPTZ", "")

    def test_validation_results_can_be_upserted(self, connection):
        """ON CONFLICT (event_id) needs the column to be unique."""
        schema.init_schema()

        ddl = " ".join(sql for sql, _ in connection.executed)
        assert "event_id VARCHAR(255) NOT NULL UNIQUE" in ddl

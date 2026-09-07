"""SQL and row mapping. No connection lifecycle, no domain imports.

Repositories used to subclass the connection owner and to import
``drift_engine.models`` and ``data_validation.models``, which made ``import db``
pull in the whole application and left the persistence layer unusable on its
own. They now take plain dictionaries and borrow a cursor from
``db.pool.transaction()``, which owns commit and rollback.

Callers pass ordinary Python values -- ``datetime`` objects for timestamps,
``dict``/``list`` for JSONB. Wrapping in ``Json`` happens here, at the boundary,
rather than in the domain models' ``to_dict()``.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Sequence

import psycopg2
from psycopg2.extras import Json, execute_values

from db.errors import QueryFailed
from db.pool import transaction

logger = logging.getLogger(__name__)


def _json(value: Any) -> Optional[Json]:
    """Adapt a Python value for a JSONB column."""
    return None if value is None else Json(value)


class EventRepository:
    """Ingested GitHub events: the flat table and the JSONB table."""

    INSERT_EVENT = """
    INSERT INTO github_events (
        event_id, event_type,
        repo_id, repo_name, repo_url,
        actor_id, actor_login, actor_url, actor_avatar,
        payload_ref, payload_head, payload_before, push_id,
        public, created_at
    ) VALUES %s
    ON CONFLICT (event_id) DO NOTHING
    """

    INSERT_PROCESSED = """
    INSERT INTO github_events_processed (
        event_id, event_data, validation_status
    ) VALUES %s
    ON CONFLICT (event_id) DO NOTHING
    """

    def save_batch(self, events: Sequence[Dict[str, Any]]) -> int:
        """Insert events into both tables in one transaction.

        Args:
            events: dicts with a ``columns`` mapping for the flat table, plus
                ``event_id``, ``raw`` and ``validation_status``.

        Returns:
            The number of events submitted (rows actually inserted may be
            fewer, since duplicates are skipped).
        """
        if not events:
            return 0

        flat_rows = [
            (
                event["columns"]["event_id"],
                event["columns"]["event_type"],
                event["columns"]["repo_id"],
                event["columns"]["repo_name"],
                event["columns"]["repo_url"],
                event["columns"]["actor_id"],
                event["columns"]["actor_login"],
                event["columns"]["actor_url"],
                event["columns"]["actor_avatar"],
                event["columns"]["payload_ref"],
                event["columns"]["payload_head"],
                event["columns"]["payload_before"],
                event["columns"]["push_id"],
                event["columns"]["public"],
                event["columns"]["created_at"],
            )
            for event in events
        ]
        processed_rows = [
            (event["event_id"], _json(event["raw"]), event["validation_status"])
            for event in events
        ]

        try:
            with transaction() as cursor:
                execute_values(cursor, self.INSERT_EVENT, flat_rows)
                execute_values(cursor, self.INSERT_PROCESSED, processed_rows)
        except psycopg2.Error as exc:
            raise QueryFailed(f"Failed to store {len(events)} events: {exc}") from exc

        logger.info("Stored %s events", len(events))
        return len(events)


class DriftRepository:
    """Drift detection results."""

    COLUMNS = (
        "drift_type",
        "entity",
        "field_name",
        "baseline_start",
        "baseline_end",
        "current_start",
        "current_end",
        "metric_name",
        "baseline_value",
        "current_value",
        "drift_score",
        "severity",
        "detected_at",
        "metadata",
    )

    JSON_COLUMNS = ("baseline_value", "current_value", "metadata")

    INSERT = f"INSERT INTO drift_results ({', '.join(COLUMNS)}) VALUES %s"

    SELECT = f"SELECT drift_id, {', '.join(COLUMNS)} FROM drift_results"

    def save_many(self, results: Sequence[Dict[str, Any]], page_size: int = 100) -> int:
        """Insert every result in one transaction.

        The previous implementation looped one INSERT and one COMMIT per row,
        with no atomicity across the batch -- a failure midway left a drift run
        partially persisted.
        """
        if not results:
            logger.info("No drift results to write")
            return 0

        rows = [self._to_row(result) for result in results]
        try:
            with transaction() as cursor:
                execute_values(cursor, self.INSERT, rows, page_size=page_size)
        except psycopg2.Error as exc:
            raise QueryFailed(f"Failed to write {len(results)} drift results: {exc}") from exc

        logger.info("Wrote %s drift results", len(results))
        return len(results)

    def _to_row(self, result: Dict[str, Any]) -> tuple:
        return tuple(
            _json(result.get(column)) if column in self.JSON_COLUMNS else result.get(column)
            for column in self.COLUMNS
        )

    def recent(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Most recently detected drifts first."""
        return self._select(f"{self.SELECT} ORDER BY detected_at DESC LIMIT %s", (limit,))

    def critical_since(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Critical drifts detected within the last ``hours`` hours."""
        # The interval is multiplied, not interpolated into a quoted literal.
        # Written as INTERVAL '%s hours', psycopg2 substitutes inside the
        # quotes: it happens to render correctly for an int and produces a
        # syntax error for a string.
        query = f"""
        {self.SELECT}
        WHERE severity = 'CRITICAL'
          AND detected_at >= now() - (%s * INTERVAL '1 hour')
        ORDER BY detected_at DESC
        """
        return self._select(query, (hours,))

    def _select(self, query: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
        try:
            with transaction() as cursor:
                cursor.execute(query, params)
                columns = [description[0] for description in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
        except psycopg2.Error as exc:
            raise QueryFailed(f"Failed to read drift results: {exc}") from exc


class ValidationRepository:
    """Per-event validation outcomes."""

    COLUMNS = (
        "event_id",
        "source_table",
        "status",
        "failed_checks",
        "error_messages",
        "severity",
        "validation_ts",
        "processing_time_ms",
        "metadata",
        "failure_details",
    )

    JSON_COLUMNS = ("failed_checks", "error_messages", "metadata", "failure_details")

    # Re-validating an event replaces its verdict rather than adding a row.
    INSERT = f"""
    INSERT INTO validation_results ({', '.join(COLUMNS)}) VALUES %s
    ON CONFLICT (event_id) DO UPDATE SET
        status = EXCLUDED.status,
        failed_checks = EXCLUDED.failed_checks,
        error_messages = EXCLUDED.error_messages,
        severity = EXCLUDED.severity,
        validation_ts = EXCLUDED.validation_ts,
        processing_time_ms = EXCLUDED.processing_time_ms,
        metadata = EXCLUDED.metadata,
        failure_details = EXCLUDED.failure_details
    """

    def save_many(self, results: Sequence[Dict[str, Any]]) -> int:
        """Insert or update every result in one statement and one commit."""
        if not results:
            return 0

        # Postgres refuses to let ON CONFLICT DO UPDATE touch the same row
        # twice in one statement, so keep the last verdict per event.
        deduplicated = {result["event_id"]: result for result in results}

        rows = [
            tuple(
                _json(result.get(column)) if column in self.JSON_COLUMNS else result.get(column)
                for column in self.COLUMNS
            )
            for result in deduplicated.values()
        ]
        try:
            with transaction() as cursor:
                execute_values(cursor, self.INSERT, rows)
        except psycopg2.Error as exc:
            raise QueryFailed(
                f"Failed to write {len(rows)} validation results: {exc}"
            ) from exc

        logger.debug("Wrote %s validation results", len(rows))
        return len(rows)

    def save(self, result: Dict[str, Any]) -> None:
        """Persist a single validation result."""
        self.save_many([result])

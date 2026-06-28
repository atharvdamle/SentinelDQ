"""Shared PostgreSQL writer primitives for validation and drift persistence."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import execute_batch

from drift_engine.models import DriftResult
from persistence.persistence import PersistenceService

logger = logging.getLogger(__name__)


class DriftPostgresWriter(PersistenceService):
    """Writes drift detection results to PostgreSQL."""

    def __init__(self, connection_params: Optional[Dict[str, Any]] = None):
        super().__init__(connection_params)
        self.table_name = "drift_results"

    def ensure_table_exists(self) -> None:
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            drift_id SERIAL PRIMARY KEY,
            drift_type VARCHAR(50) NOT NULL,
            entity VARCHAR(100),
            field_name VARCHAR(200),

            baseline_start TIMESTAMP NOT NULL,
            baseline_end TIMESTAMP NOT NULL,
            current_start TIMESTAMP NOT NULL,
            current_end TIMESTAMP NOT NULL,

            metric_name VARCHAR(100) NOT NULL,
            baseline_value JSONB,
            current_value JSONB,
            drift_score FLOAT NOT NULL,

            severity VARCHAR(20) NOT NULL,
            detected_at TIMESTAMP DEFAULT NOW(),

            metadata JSONB
        );

        CREATE INDEX IF NOT EXISTS idx_drift_detected_at ON {self.table_name}(detected_at);
        CREATE INDEX IF NOT EXISTS idx_drift_severity ON {self.table_name}(severity);
        CREATE INDEX IF NOT EXISTS idx_drift_type_entity ON {self.table_name}(drift_type, entity);
        """

        connection = self.get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(create_table_sql)
            connection.commit()
            logger.info("Ensured %s table exists", self.table_name)
        except psycopg2.Error as exc:
            connection.rollback()
            logger.error("Failed to create drift_results table: %s", exc)
            raise RuntimeError(f"Failed to create drift_results table: {exc}") from exc

    def write_results(self, results: List[DriftResult], batch_size: int = 100) -> None:
        if not results:
            logger.info("No drift results to write")
            return

        insert_sql = f"""
        INSERT INTO {self.table_name} (
            drift_type, entity, field_name,
            baseline_start, baseline_end, current_start, current_end,
            metric_name, baseline_value, current_value, drift_score,
            severity, detected_at, metadata
        ) VALUES (
            %(drift_type)s, %(entity)s, %(field_name)s,
            %(baseline_start)s, %(baseline_end)s, %(current_start)s, %(current_end)s,
            %(metric_name)s, %(baseline_value)s, %(current_value)s, %(drift_score)s,
            %(severity)s, %(detected_at)s, %(metadata)s
        )
        """

        connection = self.get_connection()
        try:
            data = [result.to_dict() for result in results]
            with connection.cursor() as cursor:
                execute_batch(cursor, insert_sql, data, page_size=batch_size)
            connection.commit()
            logger.info("Successfully wrote %s drift results to database", len(results))
        except psycopg2.Error as exc:
            connection.rollback()
            logger.error("Failed to write drift results: %s", exc)
            raise RuntimeError(f"Failed to write drift results: {exc}") from exc

    def get_recent_drifts(self, limit: int = 100) -> List[dict]:
        query = f"""
        SELECT
            drift_id, drift_type, entity, field_name,
            baseline_start, baseline_end, current_start, current_end,
            metric_name, baseline_value, current_value, drift_score,
            severity, detected_at, metadata
        FROM {self.table_name}
        ORDER BY detected_at DESC
        LIMIT %s
        """

        connection = self.get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, (limit,))
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            logger.info("Retrieved %s recent drift results", len(results))
            return results
        except psycopg2.Error as exc:
            logger.error("Failed to retrieve drift results: %s", exc)
            raise RuntimeError(f"Failed to retrieve drift results: {exc}") from exc

    def get_critical_drifts(self, hours: int = 24) -> List[dict]:
        query = f"""
        SELECT
            drift_id, drift_type, entity, field_name,
            baseline_start, baseline_end, current_start, current_end,
            metric_name, baseline_value, current_value, drift_score,
            severity, detected_at, metadata
        FROM {self.table_name}
        WHERE severity = 'CRITICAL'
          AND detected_at >= NOW() - INTERVAL '%s hours'
        ORDER BY detected_at DESC
        """

        connection = self.get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, (hours,))
                columns = [desc[0] for desc in cursor.description]
                results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            logger.info(
                "Retrieved %s critical drifts from last %s hours", len(results), hours
            )
            return results
        except psycopg2.Error as exc:
            logger.error("Failed to retrieve critical drifts: %s", exc)
            raise RuntimeError(f"Failed to retrieve critical drifts: {exc}") from exc

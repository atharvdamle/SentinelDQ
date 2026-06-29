"""Repository-style wrappers over the shared persistence service."""

from __future__ import annotations

from typing import Any, Dict, Optional

from drift_engine.models import DriftResult
from data_validation.models.validation_result import ValidationResult
from db.persistence import PersistenceService


class ValidationRepository(PersistenceService):
    """Repository for validation result persistence and lookup."""

    def __init__(self, connection_params: Optional[Dict[str, Any]] = None):
        super().__init__(connection_params)
        self.table_name = "validation_results"

    def ensure_table_exists(self) -> None:
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY,
            event_id VARCHAR(255) NOT NULL,
            table_name VARCHAR(255) NOT NULL,
            status VARCHAR(50) NOT NULL,
            failed_checks JSONB,
            error_messages JSONB,
            severity VARCHAR(50),
            validation_ts TIMESTAMP NOT NULL,
            processing_time_ms FLOAT,
            metadata JSONB,
            failure_details JSONB,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
        connection = self.get_connection()
        with connection.cursor() as cursor:
            cursor.execute(create_table_sql)
        self.commit()

    def save(self, result: ValidationResult) -> None:
        insert_sql = f"""
        INSERT INTO {self.table_name} (
            event_id, table_name, status, failed_checks, error_messages,
            severity, validation_ts, processing_time_ms, metadata, failure_details
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        payload = result.to_dict()
        values = (
            payload["event_id"],
            payload["table_name"],
            payload["status"],
            payload["failed_checks"],
            payload["error_messages"],
            payload["severity"],
            payload["validation_ts"],
            payload.get("processing_time_ms"),
            payload.get("metadata", "{}"),
            payload.get("failure_details", "[]"),
        )
        connection = self.get_connection()
        with connection.cursor() as cursor:
            cursor.execute(insert_sql, values)
        self.commit()


class DriftRepository(PersistenceService):
    """Repository for drift result persistence and lookup."""

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
        """
        connection = self.get_connection()
        with connection.cursor() as cursor:
            cursor.execute(create_table_sql)
        self.commit()

    def save(self, result: DriftResult) -> None:
        insert_sql = f"""
        INSERT INTO {self.table_name} (
            drift_type, entity, field_name,
            baseline_start, baseline_end, current_start, current_end,
            metric_name, baseline_value, current_value, drift_score,
            severity, detected_at, metadata
        ) VALUES (%(drift_type)s, %(entity)s, %(field_name)s,
            %(baseline_start)s, %(baseline_end)s, %(current_start)s, %(current_end)s,
            %(metric_name)s, %(baseline_value)s, %(current_value)s, %(drift_score)s,
            %(severity)s, %(detected_at)s, %(metadata)s)
        """
        connection = self.get_connection()
        with connection.cursor() as cursor:
            cursor.execute(insert_sql, result.to_dict())
        self.commit()

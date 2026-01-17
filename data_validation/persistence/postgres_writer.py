"""
PostgreSQL Persistence for Validation Results

Handles:
- Writing validation results to database
- Creating validation_results table
- Batch insertion for performance
- Connection management
"""

import psycopg2
from psycopg2.extras import execute_batch
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

from ..models.validation_result import ValidationResult, ValidationStatus


class PostgresValidationWriter:
    """
    Writes validation results to PostgreSQL database.

    Table Schema:
        validation_results (
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
        )
    """

    def __init__(self, connection_params: Dict[str, str]):
        """
        Initialize with database connection parameters.

        Args:
            connection_params: Dict with keys: host, port, database, user, password
        """
        self.connection_params = connection_params
        self.connection = None
        self.table_name = "validation_results"

    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(**self.connection_params)
            self.connection.autocommit = False  # Use transactions
        except psycopg2.Error as e:
            raise ConnectionError(f"Failed to connect to PostgreSQL: {e}")

    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def ensure_table_exists(self) -> None:
        """Create validation_results table if it doesn't exist."""
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
        
        -- Create indexes for common queries
        CREATE INDEX IF NOT EXISTS idx_validation_event_id 
            ON {self.table_name}(event_id);
        CREATE INDEX IF NOT EXISTS idx_validation_status 
            ON {self.table_name}(status);
        CREATE INDEX IF NOT EXISTS idx_validation_ts 
            ON {self.table_name}(validation_ts DESC);
        CREATE INDEX IF NOT EXISTS idx_validation_severity 
            ON {self.table_name}(severity);
        """

        if not self.connection:
            self.connect()

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_table_sql)
            self.connection.commit()
        except psycopg2.Error as e:
            self.connection.rollback()
            raise RuntimeError(f"Failed to create validation table: {e}")

    def write_result(self, result: ValidationResult) -> None:
        """
        Write a single validation result to database.

        Args:
            result: ValidationResult to persist
        """
        if not self.connection:
            self.connect()

        insert_sql = f"""
        INSERT INTO {self.table_name} (
            event_id, table_name, status, failed_checks, error_messages,
            severity, validation_ts, processing_time_ms, metadata, failure_details
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        result_dict = result.to_dict()

        values = (
            result_dict['event_id'],
            result_dict['table_name'],
            result_dict['status'],
            result_dict['failed_checks'],  # Already JSON string
            result_dict['error_messages'],  # Already JSON string
            result_dict['severity'],
            result_dict['validation_ts'],
            result_dict.get('processing_time_ms'),
            result_dict.get('metadata', '{}'),  # Already JSON string
            result_dict.get('failure_details', '[]')  # Already JSON string
        )

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_sql, values)
            self.connection.commit()
        except psycopg2.Error as e:
            self.connection.rollback()
            raise RuntimeError(f"Failed to write validation result: {e}")

    def write_batch(self, results: List[ValidationResult]) -> None:
        """
        Write multiple validation results in a batch (more efficient).

        Args:
            results: List of ValidationResult objects
        """
        if not results:
            return

        if not self.connection:
            self.connect()

        insert_sql = f"""
        INSERT INTO {self.table_name} (
            event_id, table_name, status, failed_checks, error_messages,
            severity, validation_ts, processing_time_ms, metadata, failure_details
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        # Prepare data for batch insert
        values_list = []
        for result in results:
            result_dict = result.to_dict()
            values = (
                result_dict['event_id'],
                result_dict['table_name'],
                result_dict['status'],
                result_dict['failed_checks'],
                result_dict['error_messages'],
                result_dict['severity'],
                result_dict['validation_ts'],
                result_dict.get('processing_time_ms'),
                result_dict.get('metadata', '{}'),
                result_dict.get('failure_details', '[]')
            )
            values_list.append(values)

        try:
            with self.connection.cursor() as cursor:
                execute_batch(cursor, insert_sql, values_list, page_size=100)
            self.connection.commit()
        except psycopg2.Error as e:
            self.connection.rollback()
            raise RuntimeError(f"Failed to write validation batch: {e}")

    def get_validation_stats(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get validation statistics from database.

        Args:
            start_date: Start of time range (optional)
            end_date: End of time range (optional)

        Returns:
            Dictionary with validation statistics
        """
        if not self.connection:
            self.connect()

        where_clauses = []
        params = []

        if start_date:
            where_clauses.append("validation_ts >= %s")
            params.append(start_date)

        if end_date:
            where_clauses.append("validation_ts <= %s")
            params.append(end_date)

        where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

        query = f"""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
            SUM(CASE WHEN status = 'WARN' THEN 1 ELSE 0 END) as warned,
            SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
            AVG(processing_time_ms) as avg_processing_time_ms,
            MIN(validation_ts) as earliest_validation,
            MAX(validation_ts) as latest_validation
        FROM {self.table_name}
        WHERE {where_sql}
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                row = cursor.fetchone()

                total = row[0] or 0

                return {
                    'total': total,
                    'passed': row[1] or 0,
                    'warned': row[2] or 0,
                    'failed': row[3] or 0,
                    'pass_rate': (row[1] or 0) / total if total > 0 else 0,
                    'warn_rate': (row[2] or 0) / total if total > 0 else 0,
                    'fail_rate': (row[3] or 0) / total if total > 0 else 0,
                    'avg_processing_time_ms': row[4] or 0,
                    'earliest_validation': row[5],
                    'latest_validation': row[6]
                }
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to get validation stats: {e}")

    def get_recent_failures(
        self,
        limit: int = 100,
        severity: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get recent validation failures for debugging.

        Args:
            limit: Maximum number of records to return
            severity: Filter by severity (CRITICAL, WARNING)

        Returns:
            List of failure records
        """
        if not self.connection:
            self.connect()

        where_sql = "status IN ('WARN', 'FAIL')"
        params = []

        if severity:
            where_sql += " AND severity = %s"
            params.append(severity)

        query = f"""
        SELECT 
            event_id,
            table_name,
            status,
            failed_checks,
            error_messages,
            severity,
            validation_ts,
            failure_details
        FROM {self.table_name}
        WHERE {where_sql}
        ORDER BY validation_ts DESC
        LIMIT %s
        """

        params.append(limit)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()

                results = []
                for row in rows:
                    results.append({
                        'event_id': row[0],
                        'table_name': row[1],
                        'status': row[2],
                        'failed_checks': json.loads(row[3]) if row[3] else [],
                        'error_messages': json.loads(row[4]) if row[4] else [],
                        'severity': row[5],
                        'validation_ts': row[6],
                        'failure_details': json.loads(row[7]) if row[7] else []
                    })

                return results
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to get recent failures: {e}")

    def check_duplicate(
        self,
        event_id: str,
        lookback_seconds: int = 3600
    ) -> bool:
        """
        Check if an event ID was recently validated (duplicate detection).

        Args:
            event_id: Event ID to check
            lookback_seconds: How far back to look

        Returns:
            True if duplicate found, False otherwise
        """
        if not self.connection:
            self.connect()

        query = f"""
        SELECT COUNT(*) 
        FROM {self.table_name}
        WHERE event_id = %s
        AND validation_ts > NOW() - INTERVAL '%s seconds'
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, (event_id, lookback_seconds))
                count = cursor.fetchone()[0]
                return count > 0
        except psycopg2.Error as e:
            raise RuntimeError(f"Failed to check duplicate: {e}")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


# Factory function for easy creation

def create_postgres_writer(
    host: str = "localhost",
    port: int = 5432,
    database: str = "sentinel_dq",
    user: str = "postgres",
    password: str = "postgres"
) -> PostgresValidationWriter:
    """
    Factory function to create PostgresValidationWriter.

    Args:
        host: PostgreSQL host
        port: PostgreSQL port
        database: Database name
        user: Database user
        password: Database password

    Returns:
        Configured PostgresValidationWriter instance
    """
    connection_params = {
        'host': host,
        'port': port,
        'database': database,
        'user': user,
        'password': password
    }

    writer = PostgresValidationWriter(connection_params)
    writer.ensure_table_exists()

    return writer

"""
PostgreSQL writer for drift results.
"""

import psycopg2
from psycopg2.extras import execute_batch
from typing import List
import logging
import os

from drift_engine.models import DriftResult

logger = logging.getLogger(__name__)


class DriftPostgresWriter:
    """
    Writes drift detection results to PostgreSQL.
    """

    def __init__(self):
        """Initialize connection parameters from environment."""
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = int(os.getenv("POSTGRES_PORT", 5432))
        self.database = os.getenv("POSTGRES_DB", "SentinelDQ_DB")
        self.user = os.getenv("POSTGRES_USER", "postgres")
        self.password = os.getenv("POSTGRES_PASSWORD", "")

        self._connection = None

    def connect(self):
        """Establish database connection."""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            logger.info(
                f"Connected to PostgreSQL at {self.host}:{self.port}/{self.database}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def close(self):
        """Close database connection."""
        if self._connection:
            self._connection.close()
            logger.info("Closed PostgreSQL connection")

    def ensure_table_exists(self):
        """Create drift_results table if it doesn't exist."""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS drift_results (
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
        
        CREATE INDEX IF NOT EXISTS idx_drift_detected_at ON drift_results(detected_at);
        CREATE INDEX IF NOT EXISTS idx_drift_severity ON drift_results(severity);
        CREATE INDEX IF NOT EXISTS idx_drift_type_entity ON drift_results(drift_type, entity);
        """

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                self._connection.commit()
                logger.info("Ensured drift_results table exists")
        except Exception as e:
            logger.error(f"Failed to create drift_results table: {e}")
            self._connection.rollback()
            raise

    def write_results(self, results: List[DriftResult], batch_size: int = 100):
        """
        Write drift results to database.

        Args:
            results: List of DriftResult objects
            batch_size: Number of records to insert per batch
        """
        if not results:
            logger.info("No drift results to write")
            return

        insert_sql = """
        INSERT INTO drift_results (
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

        try:
            # Convert results to dict format
            data = [result.to_dict() for result in results]

            with self._connection.cursor() as cursor:
                execute_batch(cursor, insert_sql, data, page_size=batch_size)
                self._connection.commit()
                logger.info(
                    f"Successfully wrote {len(results)} drift results to database"
                )

        except Exception as e:
            logger.error(f"Failed to write drift results: {e}")
            self._connection.rollback()
            raise

    def get_recent_drifts(self, limit: int = 100) -> List[dict]:
        """
        Retrieve recent drift results.

        Args:
            limit: Maximum number of results to return

        Returns:
            List of drift result dictionaries
        """
        query = """
        SELECT 
            drift_id, drift_type, entity, field_name,
            baseline_start, baseline_end, current_start, current_end,
            metric_name, baseline_value, current_value, drift_score,
            severity, detected_at, metadata
        FROM drift_results
        ORDER BY detected_at DESC
        LIMIT %s
        """

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query, (limit,))
                columns = [desc[0] for desc in cursor.description]
                results = []

                for row in cursor.fetchall():
                    result = dict(zip(columns, row))
                    results.append(result)

                logger.info(f"Retrieved {len(results)} recent drift results")
                return results

        except Exception as e:
            logger.error(f"Failed to retrieve drift results: {e}")
            raise

    def get_critical_drifts(self, hours: int = 24) -> List[dict]:
        """
        Get critical drifts from the last N hours.

        Args:
            hours: Look back period in hours

        Returns:
            List of critical drift result dictionaries
        """
        query = """
        SELECT 
            drift_id, drift_type, entity, field_name,
            baseline_start, baseline_end, current_start, current_end,
            metric_name, baseline_value, current_value, drift_score,
            severity, detected_at, metadata
        FROM drift_results
        WHERE severity = 'CRITICAL'
          AND detected_at >= NOW() - INTERVAL '%s hours'
        ORDER BY detected_at DESC
        """

        try:
            with self._connection.cursor() as cursor:
                cursor.execute(query, (hours,))
                columns = [desc[0] for desc in cursor.description]
                results = []

                for row in cursor.fetchall():
                    result = dict(zip(columns, row))
                    results.append(result)

                logger.info(
                    f"Retrieved {len(results)} critical drifts from last {hours} hours"
                )
                return results

        except Exception as e:
            logger.error(f"Failed to retrieve critical drifts: {e}")
            raise

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        self.ensure_table_exists()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

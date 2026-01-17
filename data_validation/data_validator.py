"""
SentinelDQ Data Validation Module

Main entry point for data validation system.

This module orchestrates:
1. Loading validation rules
2. Validating events
3. Persisting results
4. Exposing metrics

Usage:
    from data_validation import validate_event, validate_batch
    
    result = validate_event(event_data)
    if result.passed:
        # Insert into processed table
        pass
"""

from data_validation.metrics import get_metrics
from data_validation.persistence import PostgresValidationWriter
from data_validation.models import ValidationResult, ValidationStatus
from data_validation.engine import ValidationEngine
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


class DataValidator:
    """
    High-level API for data validation.

    Integrates engine, persistence, and metrics.
    """

    def __init__(
        self,
        rules_path: Optional[str] = None,
        db_config: Optional[Dict[str, Any]] = None,
        enable_metrics: bool = True,
        enable_persistence: bool = True
    ):
        """
        Initialize data validator.

        Args:
            rules_path: Path to validation rules YAML
            db_config: Database configuration for persistence
            enable_metrics: Whether to collect metrics
            enable_persistence: Whether to persist results to DB
        """
        # Set default rules path
        if not rules_path:
            rules_path = str(Path(__file__).parent /
                             'rules' / 'github_events.yaml')

        # Initialize engine
        self.engine = ValidationEngine(rules_path=rules_path)

        # Initialize persistence
        self.enable_persistence = enable_persistence
        self.db_writer = None
        if enable_persistence:
            # If caller didn't supply a db_config, try to build one from
            # environment variables so the validator can run in Docker
            # without extra wiring.
            if not db_config:
                import os
                db_host = os.getenv('POSTGRES_HOST', 'postgres')
                db_port = int(os.getenv('POSTGRES_PORT', 5432))
                db_name = os.getenv('POSTGRES_DB', 'sentineldq')
                db_user = os.getenv('POSTGRES_USER', 'postgres')
                db_password = os.getenv('POSTGRES_PASSWORD', 'postgres')

                db_config = {
                    'host': db_host,
                    'port': db_port,
                    'database': db_name,
                    'user': db_user,
                    'password': db_password
                }

            if db_config:
                self.db_writer = PostgresValidationWriter(db_config)
                try:
                    self.db_writer.connect()
                    self.db_writer.ensure_table_exists()
                except Exception as e:
                    # Warn but don't crash the validator; higher layers may
                    # choose to proceed without persistence.
                    print(f"Warning: could not initialize DB writer: {e}")

        # Initialize metrics
        self.enable_metrics = enable_metrics
        self.metrics = get_metrics() if enable_metrics else None

    def validate_event(
        self,
        event: Dict[str, Any],
        event_id: Optional[str] = None,
        persist: bool = True
    ) -> ValidationResult:
        """
        Validate a single event.

        Args:
            event: Event data dictionary
            event_id: Event ID (auto-extracted if not provided)
            persist: Whether to persist result to database

        Returns:
            ValidationResult
        """
        # Validate
        result = self.engine.validate_event(event, event_id)

        # Record metrics
        if self.enable_metrics and self.metrics:
            self.metrics.record_validation(result)

        # Persist to database
        if persist and self.enable_persistence and self.db_writer:
            try:
                self.db_writer.write_result(result)
            except Exception as e:
                print(f"Warning: Failed to persist validation result: {e}")

        return result

    def validate_batch(
        self,
        events: List[Dict[str, Any]],
        persist: bool = True
    ) -> List[ValidationResult]:
        """
        Validate a batch of events.

        Args:
            events: List of event dictionaries
            persist: Whether to persist results to database

        Returns:
            List of ValidationResult objects
        """
        results = []

        for event in events:
            result = self.validate_event(event, persist=False)
            results.append(result)

        # Batch persist for efficiency
        if persist and self.enable_persistence and self.db_writer:
            try:
                self.db_writer.write_batch(results)
            except Exception as e:
                print(f"Warning: Failed to persist validation batch: {e}")

        return results

    def should_process(self, result: ValidationResult) -> bool:
        """
        Determine if event should be processed based on validation result.

        Args:
            result: ValidationResult

        Returns:
            True if event should be processed, False otherwise
        """
        return result.status != ValidationStatus.FAIL

    def close(self):
        """Clean up resources."""
        if self.db_writer:
            self.db_writer.disconnect()


# Convenience functions for direct usage

_default_validator: Optional[DataValidator] = None


def get_validator() -> DataValidator:
    """Get default validator instance (singleton)."""
    global _default_validator
    if _default_validator is None:
        _default_validator = DataValidator(enable_persistence=False)
    return _default_validator


def validate_event(event: Dict[str, Any]) -> ValidationResult:
    """
    Validate a single event using default validator.

    Args:
        event: Event data

    Returns:
        ValidationResult
    """
    return get_validator().validate_event(event, persist=False)


def validate_batch(events: List[Dict[str, Any]]) -> List[ValidationResult]:
    """
    Validate multiple events using default validator.

    Args:
        events: List of event dictionaries

    Returns:
        List of ValidationResult objects
    """
    return get_validator().validate_batch(events, persist=False)


if __name__ == "__main__":
    print("SentinelDQ Data Validation Module")
    print("=" * 50)
    # Lightweight example removed for brevity when imported as a package

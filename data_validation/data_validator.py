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
from data_validation.models import ValidationResult, ValidationStatus
from data_validation.engine import ValidationEngine
import db
from db import DatabaseConfig, ValidationRepository
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

logger = logging.getLogger(__name__)


class DataValidator:
    """
    High-level API for data validation.

    Integrates engine, persistence, and metrics.
    """

    def __init__(
        self,
        rules_path: Optional[str] = None,
        persistence_config: Optional[Dict[str, Any]] = None,
        enable_metrics: bool = True,
        enable_persistence: bool = True,
    ):
        """
        Initialize data validator.

        Args:
            rules_path: Path to validation rules YAML
            persistence_config: Database configuration for persistence
            enable_metrics: Whether to collect metrics
            enable_persistence: Whether to persist results to DB
        """
        # Set default rules path
        if not rules_path:
            rules_path = str(Path(__file__).parent / "rules" / "github_events.yaml")

        # Initialize engine
        self.engine = ValidationEngine(rules_path=rules_path)

        # Initialize persistence
        self.enable_persistence = enable_persistence
        self.repository = None
        if enable_persistence:
            # Connection settings come from db.config; persistence_config is
            # only for callers that want to override them.
            try:
                db.configure(DatabaseConfig.from_env(
                    application_name="sentineldq-validator",
                    overrides=persistence_config,
                ))
                db.init_schema()
                self.repository = ValidationRepository()
            except Exception as e:
                # Warn but don't crash the validator; higher layers may
                # choose to proceed without persistence.
                logger.warning("Could not initialize persistence: %s", e)

        # Initialize metrics
        self.enable_metrics = enable_metrics
        self.metrics = get_metrics() if enable_metrics else None

    def validate_event(
        self,
        event: Dict[str, Any],
        event_id: Optional[str] = None,
        persist: bool = True,
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
        if persist and self.enable_persistence and self.repository:
            try:
                self.repository.save(result.to_dict())
            except Exception as e:
                logger.warning("Failed to persist validation result: %s", e)

        return result

    def validate_batch(
        self, events: List[Dict[str, Any]], persist: bool = True
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

        # Batch persist: one statement and one commit for the whole batch.
        if persist and self.enable_persistence and self.repository:
            try:
                self.repository.save_many([item.to_dict() for item in results])
            except Exception as e:
                logger.warning("Failed to persist validation batch: %s", e)

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
        if self.repository:
            db.close_pool()


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

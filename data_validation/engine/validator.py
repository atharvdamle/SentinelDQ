"""
Validation Engine

The core orchestrator that:
1. Loads validation rules from YAML
2. Instantiates check modules
3. Executes validations in order
4. Aggregates results
5. Determines pass/warn/fail status
6. Handles duplicate detection

This is the main entry point for event validation.
"""

import yaml
import time
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

from ..models.validation_result import (
    ValidationResult,
    ValidationFailure,
    ValidationStatus,
    Severity,
    create_pass_result,
    create_failure_result,
)
from ..checks import (
    SchemaChecker,
    TypeChecker,
    ValueChecker,
    NullChecker,
    ConsistencyChecker,
    TimestampChecker,
    get_nested_value,
)


class ValidationEngine:
    """
    Main validation orchestrator.

    Responsibilities:
    - Load and parse validation rules
    - Coordinate validation checks
    - Aggregate results
    - Manage duplicate detection
    - Track validation metrics

    Usage:
        engine = ValidationEngine(rules_path='rules/github_events.yaml')
        result = engine.validate_event(event_data)

        if result.status == ValidationStatus.FAIL:
            # Don't insert into processed table
            pass
        elif result.status == ValidationStatus.WARN:
            # Insert but mark as having warnings
            pass
        else:
            # Insert normally
            pass
    """

    def __init__(
        self,
        rules_path: Optional[str] = None,
        rules_dict: Optional[Dict[str, Any]] = None,
        duplicate_cache: Optional[Any] = None,
    ):
        """
        Initialize validation engine.

        Args:
            rules_path: Path to YAML rules file
            rules_dict: Rules as dictionary (alternative to file)
            duplicate_cache: Optional cache for duplicate detection
        """
        # Load rules
        if rules_path:
            self.rules = self._load_rules_from_file(rules_path)
        elif rules_dict:
            self.rules = rules_dict
        else:
            raise ValueError("Must provide either rules_path or rules_dict")

        # Initialize check modules
        self._initialize_checkers()

        # Duplicate detection
        self.duplicate_cache = duplicate_cache or set()
        self.duplicate_config = self.rules.get("duplicate_check", {})

        # Metadata
        self.rules_version = self.rules.get("version", "unknown")
        self.event_type = self.rules.get("event_type", "unknown")

        # Statistics
        self.stats = {"total_validated": 0, "passed": 0, "warned": 0, "failed": 0, "total_failures": 0}

    def _load_rules_from_file(self, rules_path: str) -> Dict[str, Any]:
        """Load validation rules from YAML file."""
        path = Path(rules_path)

        if not path.exists():
            raise FileNotFoundError(f"Rules file not found: {rules_path}")

        with open(path, "r", encoding="utf-8") as f:
            rules = yaml.safe_load(f)

        return rules

    def _initialize_checkers(self) -> None:
        """Initialize all validation check modules."""
        # Schema checker
        schema_rules = self.rules.get("schema", {})
        self.schema_checker = SchemaChecker(schema_rules)

        # Type checker
        type_rules = self.rules.get("type_checks", [])
        self.type_checker = TypeChecker(type_rules)

        # Value checker
        value_rules = self.rules.get("value_checks", [])
        self.value_checker = ValueChecker(value_rules)

        # Null checker
        null_rules = self.rules.get("null_checks", [])
        self.null_checker = NullChecker(null_rules)

        # Timestamp checker
        timestamp_rules = self.rules.get("timestamp_checks", [])
        self.timestamp_checker = TimestampChecker(timestamp_rules)

        # Consistency checker
        consistency_rules = self.rules.get("consistency_checks", [])
        self.consistency_checker = ConsistencyChecker(consistency_rules)

    def validate_event(
        self, event: Dict[str, Any], event_id: Optional[str] = None, table_name: str = "github_events_raw"
    ) -> ValidationResult:
        """
        Validate a single event against all rules.

        Validation order (critical to least critical):
        1. Schema checks (required fields)
        2. Type checks
        3. Null/empty checks
        4. Value checks (regex, enum, range)
        5. Timestamp checks
        6. Consistency checks
        7. Duplicate detection

        Args:
            event: The event data to validate
            event_id: Event identifier (extracted if not provided)
            table_name: Source table name

        Returns:
            ValidationResult with status and failures
        """
        start_time = time.time()

        # Extract event ID if not provided
        if event_id is None:
            event_id = str(get_nested_value(event, "id") or "unknown")

        # Initialize result
        result = ValidationResult(
            event_id=event_id,
            table_name=table_name,
            status=ValidationStatus.PASS,
            failures=[],
            metadata={
                "rules_version": self.rules_version,
                "event_type": get_nested_value(event, "type"),
                "validation_engine": "SentinelDQ",
            },
        )

        # Run all validation checks
        try:
            # 1. Schema validation
            schema_failures = self.schema_checker.validate(event)
            for failure in schema_failures:
                result.add_failure(failure)

            # 2. Type validation
            type_failures = self.type_checker.validate(event)
            for failure in type_failures:
                result.add_failure(failure)

            # 3. Null/empty validation
            null_failures = self.null_checker.validate(event)
            for failure in null_failures:
                result.add_failure(failure)

            # 4. Value validation (regex, enum, range)
            value_failures = self.value_checker.validate(event)
            for failure in value_failures:
                result.add_failure(failure)

            # 5. Timestamp validation
            timestamp_failures = self.timestamp_checker.validate(event)
            for failure in timestamp_failures:
                result.add_failure(failure)

            # 6. Consistency validation
            consistency_failures = self.consistency_checker.validate(event)
            for failure in consistency_failures:
                result.add_failure(failure)

            # 7. Duplicate detection
            if self.duplicate_config.get("enabled", False):
                duplicate_failure = self._check_duplicate(event)
                if duplicate_failure:
                    result.add_failure(duplicate_failure)

            # Update statistics
            self._update_stats(result)

        except Exception as e:
            # Catch any unexpected errors in validation
            result.add_failure(
                ValidationFailure(
                    check_name="validation_engine.error",
                    field_path="<engine>",
                    check_type="system",
                    severity=Severity.CRITICAL,
                    error_message=f"Validation engine error: {str(e)}",
                    actual_value=str(e),
                )
            )

        # Calculate processing time
        end_time = time.time()
        result.processing_time_ms = (end_time - start_time) * 1000

        return result

    def validate_batch(
        self, events: List[Dict[str, Any]], table_name: str = "github_events_raw"
    ) -> List[ValidationResult]:
        """
        Validate a batch of events.

        Args:
            events: List of event dictionaries
            table_name: Source table name

        Returns:
            List of ValidationResult objects
        """
        results = []

        for event in events:
            event_id = str(get_nested_value(event, "id") or "unknown")
            result = self.validate_event(event, event_id, table_name)
            results.append(result)

        return results

    def _check_duplicate(self, event: Dict[str, Any]) -> Optional[ValidationFailure]:
        """
        Check for duplicate events.

        In production, this would query the database for recent events.
        For now, we use an in-memory cache.

        Args:
            event: Event to check

        Returns:
            ValidationFailure if duplicate, None otherwise
        """
        key_field = self.duplicate_config.get("key_field", "id")
        event_key = get_nested_value(event, key_field)

        if event_key is None:
            return None  # Can't check without key

        # Check cache
        if event_key in self.duplicate_cache:
            severity_str = self.duplicate_config.get("severity", "FAIL")
            severity = self._parse_severity(severity_str)
            error_message = self.duplicate_config.get("error_message", "Duplicate event detected")

            return ValidationFailure(
                check_name=f"duplicate.{key_field}",
                field_path=key_field,
                check_type="duplicate",
                severity=severity,
                error_message=error_message,
                expected_value="unique value",
                actual_value=f"duplicate: {event_key}",
            )

        # Add to cache
        self.duplicate_cache.add(event_key)

        # TODO: In production, implement time-based cache cleanup
        # based on lookback_window_seconds

        return None

    def _update_stats(self, result: ValidationResult) -> None:
        """Update engine statistics."""
        self.stats["total_validated"] += 1

        if result.status == ValidationStatus.PASS:
            self.stats["passed"] += 1
        elif result.status == ValidationStatus.WARN:
            self.stats["warned"] += 1
        elif result.status == ValidationStatus.FAIL:
            self.stats["failed"] += 1

        self.stats["total_failures"] += len(result.failures)

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get validation statistics.

        Returns:
            Dictionary with validation stats
        """
        total = self.stats["total_validated"]

        return {
            **self.stats,
            "pass_rate": self.stats["passed"] / total if total > 0 else 0,
            "warn_rate": self.stats["warned"] / total if total > 0 else 0,
            "fail_rate": self.stats["failed"] / total if total > 0 else 0,
            "avg_failures_per_event": (self.stats["total_failures"] / total if total > 0 else 0),
        }

    def reset_statistics(self) -> None:
        """Reset validation statistics."""
        self.stats = {"total_validated": 0, "passed": 0, "warned": 0, "failed": 0, "total_failures": 0}

    @staticmethod
    def _parse_severity(severity_str: str) -> Severity:
        """Convert severity string to Severity enum."""
        severity_map = {
            "FAIL": Severity.CRITICAL,
            "CRITICAL": Severity.CRITICAL,
            "WARN": Severity.WARNING,
            "WARNING": Severity.WARNING,
            "INFO": Severity.INFO,
        }
        return severity_map.get(severity_str.upper(), Severity.WARNING)

    def __repr__(self) -> str:
        """String representation of engine."""
        return (
            f"ValidationEngine("
            f"rules_version={self.rules_version}, "
            f"event_type={self.event_type}, "
            f"checks_enabled={len([c for c in [self.schema_checker, self.type_checker] if c])}"
            f")"
        )


# Factory function for easy engine creation


def create_validation_engine(
    rules_file: str = "rules/github_events.yaml", base_path: Optional[str] = None
) -> ValidationEngine:
    """
    Factory function to create validation engine with default settings.

    Args:
        rules_file: Name of rules file
        base_path: Base directory (defaults to data_validation/)

    Returns:
        Configured ValidationEngine instance
    """
    if base_path is None:
        # Assume we're in data_validation/engine/
        base_path = Path(__file__).parent.parent
    else:
        base_path = Path(base_path)

    rules_path = base_path / rules_file

    return ValidationEngine(rules_path=str(rules_path))

"""
Validation Result Data Models

Defines the core data structures for representing validation outcomes.

Design Philosophy:
- Immutable where possible (use dataclasses with frozen=True)
- Type-safe (use enums for status, proper typing)
- Self-documenting (rich error messages)
- Serializable (can be converted to JSON for storage)
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import List, Optional, Dict, Any
import json


class ValidationStatus(Enum):
    """
    Three-tier validation outcome classification.

    PASS: Event meets all validation criteria
    WARN: Event has non-critical issues but can be processed
    FAIL: Event has critical issues and should not be processed
    """
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class Severity(Enum):
    """
    Severity level for individual validation failures.
    Maps to ValidationStatus in aggregation.
    """
    CRITICAL = "FAIL"    # Maps to FAIL status
    WARNING = "WARN"     # Maps to WARN status
    INFO = "INFO"        # Informational only


@dataclass(frozen=True)
class ValidationFailure:
    """
    Represents a single validation check failure.

    Attributes:
        check_name: Name of the failed check (e.g., "type_check.actor.id")
        field_path: JSON path to the field (e.g., "actor.id")
        check_type: Category of check (e.g., "type", "regex", "required")
        severity: Severity level (CRITICAL, WARNING, INFO)
        error_message: Human-readable error description
        expected_value: What value/type was expected (optional)
        actual_value: What value was found (optional)
        rule_definition: Reference to the rule that failed (optional)
    """
    check_name: str
    field_path: str
    check_type: str
    severity: Severity
    error_message: str
    expected_value: Optional[Any] = None
    actual_value: Optional[Any] = None
    rule_definition: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'check_name': self.check_name,
            'field_path': self.field_path,
            'check_type': self.check_type,
            'severity': self.severity.value,
            'error_message': self.error_message,
            'expected_value': self._serialize_value(self.expected_value),
            'actual_value': self._serialize_value(self.actual_value),
            'rule_definition': self.rule_definition
        }

    @staticmethod
    def _serialize_value(value: Any) -> Any:
        """Safely serialize values for JSON storage."""
        if value is None:
            return None
        if isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, (list, dict)):
            return value
        return str(value)

    def __str__(self) -> str:
        """Human-readable representation."""
        msg = f"[{self.severity.value}] {self.check_name}: {self.error_message}"
        if self.expected_value is not None:
            msg += f" (expected: {self.expected_value}, got: {self.actual_value})"
        return msg


@dataclass(frozen=True)
class FieldValidationResult:
    """
    Validation result for a specific field.
    Useful for detailed reporting and debugging.
    """
    field_path: str
    passed: bool
    failures: List[ValidationFailure] = field(default_factory=list)

    @property
    def severity(self) -> Optional[Severity]:
        """Return highest severity level among failures."""
        if not self.failures:
            return None

        # CRITICAL > WARNING > INFO
        if any(f.severity == Severity.CRITICAL for f in self.failures):
            return Severity.CRITICAL
        if any(f.severity == Severity.WARNING for f in self.failures):
            return Severity.WARNING
        return Severity.INFO


@dataclass
class ValidationResult:
    """
    Complete validation result for a single event.

    This is the primary output of the validation engine.
    It aggregates all validation failures and determines the overall status.

    Attributes:
        event_id: Unique identifier for the event
        table_name: Source table (e.g., "github_events_raw")
        status: Overall validation status (PASS/WARN/FAIL)
        failures: List of all validation failures
        validation_timestamp: When validation was performed
        processing_time_ms: How long validation took
        metadata: Additional context (event type, rule version, etc.)
    """
    event_id: str
    table_name: str
    status: ValidationStatus
    failures: List[ValidationFailure] = field(default_factory=list)
    validation_timestamp: datetime = field(default_factory=datetime.utcnow)
    processing_time_ms: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        """Check if validation passed (PASS or WARN)."""
        return self.status in (ValidationStatus.PASS, ValidationStatus.WARN)

    @property
    def failed_checks(self) -> List[str]:
        """Get list of failed check names."""
        return [f.check_name for f in self.failures]

    @property
    def error_messages(self) -> List[str]:
        """Get list of error messages."""
        return [f.error_message for f in self.failures]

    @property
    def critical_failures(self) -> List[ValidationFailure]:
        """Get only critical failures."""
        return [f for f in self.failures if f.severity == Severity.CRITICAL]

    @property
    def warning_failures(self) -> List[ValidationFailure]:
        """Get only warning failures."""
        return [f for f in self.failures if f.severity == Severity.WARNING]

    def add_failure(self, failure: ValidationFailure) -> None:
        """
        Add a validation failure and update status.

        Status determination logic:
        - If any CRITICAL failure exists: status = FAIL
        - If any WARNING failure exists (and no CRITICAL): status = WARN
        - If no failures: status = PASS
        """
        self.failures.append(failure)
        self._update_status()

    def _update_status(self) -> None:
        """Recalculate validation status based on failures."""
        if any(f.severity == Severity.CRITICAL for f in self.failures):
            self.status = ValidationStatus.FAIL
        elif any(f.severity == Severity.WARNING for f in self.failures):
            self.status = ValidationStatus.WARN
        else:
            self.status = ValidationStatus.PASS

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database persistence."""
        return {
            'event_id': self.event_id,
            'table_name': self.table_name,
            'status': self.status.value,
            'failed_checks': json.dumps(self.failed_checks),
            'error_messages': json.dumps(self.error_messages),
            'severity': self._get_overall_severity(),
            'validation_ts': self.validation_timestamp.isoformat(),
            'processing_time_ms': self.processing_time_ms,
            'metadata': json.dumps(self.metadata),
            'failure_details': json.dumps([f.to_dict() for f in self.failures])
        }

    def _get_overall_severity(self) -> str:
        """Get highest severity level as string."""
        if self.critical_failures:
            return "CRITICAL"
        if self.warning_failures:
            return "WARNING"
        return "PASS"

    def __str__(self) -> str:
        """Human-readable summary."""
        summary = f"ValidationResult(event_id={self.event_id}, status={self.status.value})"
        if self.failures:
            summary += f"\n  Failures ({len(self.failures)}):"
            for failure in self.failures:
                summary += f"\n    - {failure}"
        return summary

    def get_summary_stats(self) -> Dict[str, int]:
        """Get summary statistics for reporting."""
        return {
            'total_checks': len(self.failures) + 1,  # Approximate
            'critical_failures': len(self.critical_failures),
            'warning_failures': len(self.warning_failures),
            'passed': 1 if self.status == ValidationStatus.PASS else 0
        }


# Helper functions for creating validation results

def create_pass_result(
    event_id: str,
    table_name: str = "github_events_raw",
    metadata: Optional[Dict[str, Any]] = None
) -> ValidationResult:
    """Create a validation result for a fully passing event."""
    return ValidationResult(
        event_id=event_id,
        table_name=table_name,
        status=ValidationStatus.PASS,
        failures=[],
        metadata=metadata or {}
    )


def create_failure_result(
    event_id: str,
    failures: List[ValidationFailure],
    table_name: str = "github_events_raw",
    metadata: Optional[Dict[str, Any]] = None
) -> ValidationResult:
    """Create a validation result with failures (status auto-determined)."""
    result = ValidationResult(
        event_id=event_id,
        table_name=table_name,
        status=ValidationStatus.PASS,  # Will be updated
        failures=failures,
        metadata=metadata or {}
    )
    result._update_status()
    return result

"""
Consistency Validation Checks

Validates cross-field relationships and business logic constraints.
"""

from datetime import datetime, timedelta, timezone
import re
from typing import Dict, Any, List, Optional
from ..models.validation_result import ValidationFailure, Severity
from .schema import get_nested_value, field_exists


class ConsistencyChecker:
    """
    Validates cross-field consistency and business logic rules.

    Checks:
    - Conditional field requirements (if X then Y must exist)
    - All-or-none field groups
    - Field value relationships
    """

    def __init__(self, rules: List[Dict[str, Any]]):
        """
        Initialize with consistency check rules from YAML.

        Args:
            rules: List of consistency validation rules
        """
        self.rules = rules

    def validate(self, event: Dict[str, Any]) -> List[ValidationFailure]:
        """
        Validate cross-field consistency in event.

        Args:
            event: The event data to validate

        Returns:
            List of validation failures
        """
        failures = []

        for rule in self.rules:
            name = rule.get('name', 'consistency_check')
            check_type = rule.get('check_type', 'conditional')
            severity_str = rule.get('severity', 'WARN')
            severity = self._parse_severity(severity_str)

            if check_type == 'conditional' or 'rules' in rule:
                # Conditional field requirements
                failures.extend(self._check_conditional_requirements(
                    event, rule, name, severity
                ))

            elif check_type == 'all_or_none':
                # All-or-none field group
                failures.extend(self._check_all_or_none(
                    event, rule, name, severity
                ))

        return failures

    def _check_conditional_requirements(
        self,
        event: Dict[str, Any],
        rule: Dict[str, Any],
        rule_name: str,
        severity: Severity
    ) -> List[ValidationFailure]:
        """
        Check conditional field requirements.

        Example: If type == "PushEvent", then payload.ref must exist
        """
        failures = []
        conditional_rules = rule.get('rules', [])

        for cond_rule in conditional_rules:
            if_field = cond_rule.get('if_field')
            equals = cond_rule.get('equals')
            then_required = cond_rule.get('then_required', [])
            error_message = cond_rule.get('error_message',
                                          f"Conditional requirement failed")

            # Check if condition is met
            field_value = get_nested_value(event, if_field)

            if field_value == equals:
                # Condition met, check required fields
                missing_fields = []
                for required_field in then_required:
                    if not field_exists(event, required_field):
                        missing_fields.append(required_field)

                if missing_fields:
                    failures.append(ValidationFailure(
                        check_name=f"consistency.{rule_name}",
                        field_path=if_field,
                        check_type="consistency",
                        severity=severity,
                        error_message=error_message,
                        expected_value=f"fields {then_required} present when {if_field}={equals}",
                        actual_value=f"missing fields: {missing_fields}",
                        rule_definition=str(cond_rule)
                    ))

        return failures

    def _check_all_or_none(
        self,
        event: Dict[str, Any],
        rule: Dict[str, Any],
        rule_name: str,
        severity: Severity
    ) -> List[ValidationFailure]:
        """
        Check that either all fields in group exist or none exist.

        Example: actor.id, actor.login, actor.url should all exist together
        """
        failures = []
        fields = rule.get('fields', [])
        error_message = rule.get('error_message',
                                 f"Field group incomplete")

        # Check which fields exist
        present_fields = [f for f in fields if field_exists(event, f)]

        # Either all or none should be present
        if len(present_fields) > 0 and len(present_fields) < len(fields):
            missing_fields = [f for f in fields if f not in present_fields]
            failures.append(ValidationFailure(
                check_name=f"consistency.all_or_none.{rule_name}",
                field_path=','.join(fields),
                check_type="consistency",
                severity=severity,
                error_message=error_message,
                expected_value=f"all fields {fields} present or all absent",
                actual_value=f"present: {present_fields}, missing: {missing_fields}",
                rule_definition=str(rule)
            ))

        return failures

    @staticmethod
    def _parse_severity(severity_str: str) -> Severity:
        """Convert severity string to Severity enum."""
        severity_map = {
            'FAIL': Severity.CRITICAL,
            'CRITICAL': Severity.CRITICAL,
            'WARN': Severity.WARNING,
            'WARNING': Severity.WARNING,
            'INFO': Severity.INFO
        }
        return severity_map.get(severity_str.upper(), Severity.WARNING)


# Timestamp validation checker


class TimestampChecker:
    """
    Validates timestamp fields for format and sanity.

    Checks:
    - ISO8601 format
    - Not in future (with tolerance)
    - Not too old
    - Parseable
    """

    def __init__(self, rules: List[Dict[str, Any]]):
        """
        Initialize with timestamp check rules from YAML.

        Args:
            rules: List of timestamp validation rules
        """
        self.rules = rules

    def validate(self, event: Dict[str, Any]) -> List[ValidationFailure]:
        """
        Validate timestamp fields in event.

        Args:
            event: The event data to validate

        Returns:
            List of validation failures
        """
        failures = []

        for rule in self.rules:
            field = rule['field']
            check_type = rule['check_type']
            severity_str = rule.get('severity', 'FAIL')
            error_message = rule.get('error_message',
                                     f"Timestamp validation failed for '{field}'")

            value = get_nested_value(event, field)

            # Skip if field doesn't exist
            if value is None:
                continue

            severity = self._parse_severity(severity_str)

            # Parse timestamp first
            try:
                dt = self._parse_timestamp(value)
            except Exception as e:
                if check_type in ['parseable', 'format']:
                    failures.append(ValidationFailure(
                        check_name=f"timestamp.parse.{field}",
                        field_path=field,
                        check_type="timestamp",
                        severity=severity,
                        error_message=error_message,
                        expected_value="parseable ISO8601 timestamp",
                        actual_value=str(value)
                    ))
                continue

            # Perform specific checks
            if check_type == 'format' or check_type == 'ISO8601':
                # Already validated by parsing
                pass

            elif check_type == 'not_future':
                tolerance_seconds = rule.get('tolerance_seconds', 0)
                if self._is_future(dt, tolerance_seconds):
                    failures.append(ValidationFailure(
                        check_name=f"timestamp.not_future.{field}",
                        field_path=field,
                        check_type="timestamp",
                        severity=severity,
                        error_message=error_message,
                        expected_value=f"not more than {tolerance_seconds}s in future",
                        actual_value=str(value)
                    ))

            elif check_type == 'not_too_old':
                max_age_days = rule.get('max_age_days', 365)
                if self._is_too_old(dt, max_age_days):
                    failures.append(ValidationFailure(
                        check_name=f"timestamp.not_too_old.{field}",
                        field_path=field,
                        check_type="timestamp",
                        severity=severity,
                        error_message=error_message,
                        expected_value=f"not older than {max_age_days} days",
                        actual_value=str(value)
                    ))

        return failures

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        """
        Parse timestamp string to datetime object.

        Supports:
        - ISO8601 with timezone
        - ISO8601 without timezone (assumes UTC)

        Raises:
            ValueError if parsing fails
        """
        if not isinstance(value, str):
            raise ValueError(f"Timestamp must be string, got {type(value)}")

        # Try ISO8601 parsing
        try:
            # Python 3.7+ supports ISO8601 parsing
            if value.endswith('Z'):
                value = value[:-1] + '+00:00'
            return datetime.fromisoformat(value)
        except ValueError:
            # Try other common formats
            formats = [
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
            ]
            for fmt in formats:
                try:
                    dt = datetime.strptime(value, fmt)
                    # Assume UTC if no timezone
                    return dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    continue

            raise ValueError(f"Cannot parse timestamp: {value}")

    @staticmethod
    def _is_future(dt: datetime, tolerance_seconds: int = 0) -> bool:
        """Check if timestamp is in the future beyond tolerance."""
        now = datetime.now(timezone.utc)
        # Make dt timezone-aware if it isn't
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        future_limit = now + timedelta(seconds=tolerance_seconds)
        return dt > future_limit

    @staticmethod
    def _is_too_old(dt: datetime, max_age_days: int) -> bool:
        """Check if timestamp is older than max age."""
        now = datetime.now(timezone.utc)
        # Make dt timezone-aware if it isn't
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        age_limit = now - timedelta(days=max_age_days)
        return dt < age_limit

    @staticmethod
    def _parse_severity(severity_str: str) -> Severity:
        """Convert severity string to Severity enum."""
        severity_map = {
            'FAIL': Severity.CRITICAL,
            'CRITICAL': Severity.CRITICAL,
            'WARN': Severity.WARNING,
            'WARNING': Severity.WARNING,
            'INFO': Severity.INFO
        }
        return severity_map.get(severity_str.upper(), Severity.WARNING)


# Utility functions

def validate_timestamp_format(
    value: str,
    field_name: str = "timestamp"
) -> Optional[ValidationFailure]:
    """
    Quick timestamp format validation.

    Args:
        value: Timestamp string
        field_name: Field name for error message

    Returns:
        ValidationFailure if invalid, None if valid
    """
    try:
        TimestampChecker._parse_timestamp(value)
        return None
    except Exception:
        return ValidationFailure(
            check_name=f"timestamp.{field_name}",
            field_path=field_name,
            check_type="timestamp",
            severity=Severity.CRITICAL,
            error_message="Invalid timestamp format",
            expected_value="ISO8601 format",
            actual_value=value
        )

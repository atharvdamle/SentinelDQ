"""
Schema Validation Checks

Validates the presence of required fields in nested JSON structures.
"""

from typing import Dict, Any, List, Optional
from ..models.validation_result import ValidationFailure, Severity


def get_nested_value(data: Dict[str, Any], path: str) -> Optional[Any]:
    """
    Safely retrieve a value from nested dictionary using dot notation.

    Args:
        data: The dictionary to search
        path: Dot-separated path (e.g., "actor.id", "payload.issue.number")

    Returns:
        The value if found, None otherwise

    Examples:
        >>> get_nested_value({"actor": {"id": 123}}, "actor.id")
        123
        >>> get_nested_value({"actor": {"id": 123}}, "actor.login")
        None
    """
    keys = path.split('.')
    current = data

    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
        if current is None:
            return None

    return current


def field_exists(data: Dict[str, Any], path: str) -> bool:
    """
    Check if a field exists in nested structure (even if value is None).

    This is different from get_nested_value because it distinguishes between
    a field that exists with None value vs. a field that doesn't exist.

    Args:
        data: The dictionary to search
        path: Dot-separated path

    Returns:
        True if field exists (even if None), False otherwise
    """
    keys = path.split('.')
    current = data

    for i, key in enumerate(keys):
        if not isinstance(current, dict):
            return False

        if key not in current:
            return False

        # If this is the last key, field exists
        if i == len(keys) - 1:
            return True

        current = current[key]
        if current is None:
            # Field exists but is None - can't go deeper
            return i == len(keys) - 1

    return True


class SchemaChecker:
    """
    Validates event schema against required field definitions.

    Checks:
    - Required fields are present
    - Optional fields (warnings only)
    - Nested field structure
    """

    def __init__(self, rules: Dict[str, Any]):
        """
        Initialize with schema rules from YAML.

        Args:
            rules: The 'schema' section from validation rules
        """
        self.required_fields = rules.get('required_fields', [])
        self.optional_fields = rules.get('optional_fields', [])

    def validate(self, event: Dict[str, Any]) -> List[ValidationFailure]:
        """
        Validate event schema.

        Args:
            event: The event data to validate

        Returns:
            List of validation failures (empty if all checks pass)
        """
        failures = []

        # Check required fields
        for field_rule in self.required_fields:
            path = field_rule['path']
            severity_str = field_rule.get('severity', 'FAIL')
            description = field_rule.get(
                'description', f'Required field: {path}')

            if not field_exists(event, path):
                severity = self._parse_severity(severity_str)
                failures.append(ValidationFailure(
                    check_name=f"schema.required_field.{path}",
                    field_path=path,
                    check_type="schema",
                    severity=severity,
                    error_message=f"Required field '{path}' is missing",
                    expected_value="field present",
                    actual_value="field missing",
                    rule_definition=description
                ))

        # Check optional fields (warning level)
        for field_rule in self.optional_fields:
            path = field_rule['path']
            severity_str = field_rule.get('severity', 'WARN')
            description = field_rule.get(
                'description', f'Optional field: {path}')

            if not field_exists(event, path):
                severity = self._parse_severity(severity_str)
                # Only add if severity is not INFO (to avoid clutter)
                if severity != Severity.INFO:
                    failures.append(ValidationFailure(
                        check_name=f"schema.optional_field.{path}",
                        field_path=path,
                        check_type="schema",
                        severity=severity,
                        error_message=f"Optional field '{path}' is missing",
                        expected_value="field present (optional)",
                        actual_value="field missing",
                        rule_definition=description
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


# Utility functions for common schema operations

def validate_required_fields(
    event: Dict[str, Any],
    required_paths: List[str],
    severity: Severity = Severity.CRITICAL
) -> List[ValidationFailure]:
    """
    Quick validation of required fields without full rule structure.

    Useful for ad-hoc validation or testing.

    Args:
        event: Event data
        required_paths: List of required field paths
        severity: Severity level for failures

    Returns:
        List of validation failures
    """
    failures = []

    for path in required_paths:
        if not field_exists(event, path):
            failures.append(ValidationFailure(
                check_name=f"schema.required.{path}",
                field_path=path,
                check_type="schema",
                severity=severity,
                error_message=f"Required field '{path}' is missing",
                expected_value="field present",
                actual_value="field missing"
            ))

    return failures


def get_missing_fields(
    event: Dict[str, Any],
    expected_paths: List[str]
) -> List[str]:
    """
    Get list of missing fields from event.

    Args:
        event: Event data
        expected_paths: List of expected field paths

    Returns:
        List of missing field paths
    """
    return [path for path in expected_paths if not field_exists(event, path)]


def get_present_fields(
    event: Dict[str, Any],
    field_paths: List[str]
) -> List[str]:
    """
    Get list of present fields from event.

    Args:
        event: Event data
        field_paths: List of field paths to check

    Returns:
        List of present field paths
    """
    return [path for path in field_paths if field_exists(event, path)]

"""
Type Validation Checks

Validates data types of fields in events.
"""

from typing import Dict, Any, List, Optional
from ..models.validation_result import ValidationFailure, Severity
from .schema import get_nested_value


class TypeChecker:
    """
    Validates that field values match expected data types.

    Supported types:
    - string
    - integer
    - float
    - boolean
    - list/array
    - dict/object
    - null
    """

    # Type mapping from YAML string to Python types
    TYPE_MAP = {
        'string': str,
        'str': str,
        'integer': int,
        'int': int,
        'float': float,
        'number': (int, float),
        'boolean': bool,
        'bool': bool,
        'list': list,
        'array': list,
        'dict': dict,
        'object': dict,
        'null': type(None),
        'none': type(None)
    }

    def __init__(self, rules: List[Dict[str, Any]]):
        """
        Initialize with type check rules from YAML.

        Args:
            rules: List of type check rules
        """
        self.rules = rules

    def validate(self, event: Dict[str, Any]) -> List[ValidationFailure]:
        """
        Validate field types in event.

        Args:
            event: The event data to validate

        Returns:
            List of validation failures
        """
        failures = []

        for rule in self.rules:
            field = rule['field']
            expected_type_str = rule['expected_type']
            severity_str = rule.get('severity', 'FAIL')
            error_message = rule.get('error_message',
                                     f"Field '{field}' has incorrect type")

            # Get the value
            value = get_nested_value(event, field)

            # Skip if field doesn't exist (schema checker handles this)
            if value is None and not self._field_exists_with_none(event, field):
                continue

            # Validate type
            expected_type = self._parse_type(expected_type_str)
            if not self._check_type(value, expected_type):
                severity = self._parse_severity(severity_str)
                failures.append(ValidationFailure(
                    check_name=f"type_check.{field}",
                    field_path=field,
                    check_type="type",
                    severity=severity,
                    error_message=error_message,
                    expected_value=expected_type_str,
                    actual_value=type(value).__name__,
                    rule_definition=f"Expected type: {expected_type_str}"
                ))

        return failures

    def _parse_type(self, type_str: str) -> Any:
        """Convert type string to Python type(s)."""
        type_str = type_str.lower()
        return self.TYPE_MAP.get(type_str, str)

    def _check_type(self, value: Any, expected_type: Any) -> bool:
        """
        Check if value matches expected type.

        Args:
            value: The value to check
            expected_type: Python type or tuple of types

        Returns:
            True if type matches, False otherwise
        """
        if expected_type == (int, float):
            # Special case for 'number' type
            return isinstance(value, (int, float))

        return isinstance(value, expected_type)

    def _field_exists_with_none(self, event: Dict[str, Any], path: str) -> bool:
        """
        Check if field exists but has None value.
        This is different from field not existing at all.
        """
        keys = path.split('.')
        current = event

        for i, key in enumerate(keys):
            if not isinstance(current, dict) or key not in current:
                return False

            if i == len(keys) - 1:
                return True  # Field exists (even if value is None)

            current = current[key]
            if current is None:
                return False  # Can't traverse deeper

        return False

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


# Utility functions for type checking

def check_field_type(
    event: Dict[str, Any],
    field_path: str,
    expected_type: str,
    severity: Severity = Severity.CRITICAL
) -> Optional[ValidationFailure]:
    """
    Quick type check for a single field.

    Args:
        event: Event data
        field_path: Path to field
        expected_type: Expected type as string
        severity: Severity level

    Returns:
        ValidationFailure if type mismatch, None if valid
    """
    checker = TypeChecker([{
        'field': field_path,
        'expected_type': expected_type,
        'severity': severity.value
    }])

    failures = checker.validate(event)
    return failures[0] if failures else None


def validate_types(
    event: Dict[str, Any],
    type_rules: Dict[str, str],
    severity: Severity = Severity.CRITICAL
) -> List[ValidationFailure]:
    """
    Validate multiple fields with simple dict mapping.

    Args:
        event: Event data
        type_rules: Dict mapping field paths to type names
        severity: Severity level

    Returns:
        List of validation failures

    Example:
        >>> validate_types(event, {
        ...     'id': 'string',
        ...     'actor.id': 'integer',
        ...     'public': 'boolean'
        ... })
    """
    rules = [
        {
            'field': field,
            'expected_type': type_name,
            'severity': severity.value
        }
        for field, type_name in type_rules.items()
    ]

    checker = TypeChecker(rules)
    return checker.validate(event)


def infer_type(value: Any) -> str:
    """
    Infer the type name of a value.

    Args:
        value: Any value

    Returns:
        Type name as string
    """
    if value is None:
        return 'null'

    type_obj = type(value)

    # Reverse lookup in TYPE_MAP
    for type_name, py_type in TypeChecker.TYPE_MAP.items():
        if isinstance(py_type, tuple):
            if type_obj in py_type:
                return type_name
        elif type_obj == py_type:
            return type_name

    return type_obj.__name__

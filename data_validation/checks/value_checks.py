"""
Value Validation Checks

Validates field values against specific constraints:
- Regex patterns
- Enum/allowed values
- Numeric ranges
- String length
"""

import re
from typing import Dict, Any, List, Optional, Union
from ..models.validation_result import ValidationFailure, Severity
from .schema import get_nested_value


class ValueChecker:
    """
    Validates field values against defined constraints.

    Supported check types:
    - regex: Pattern matching
    - enum: Value in allowed list
    - range: Numeric min/max
    - length: String length constraints
    """

    def __init__(self, rules: List[Dict[str, Any]]):
        """
        Initialize with value check rules from YAML.

        Args:
            rules: List of value validation rules
        """
        self.rules = rules
        # Compile regex patterns for performance
        self._compiled_patterns: Dict[str, re.Pattern] = {}
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        """Pre-compile all regex patterns for better performance."""
        for rule in self.rules:
            if rule.get('check_type') == 'regex':
                field = rule['field']
                pattern = rule['pattern']
                try:
                    self._compiled_patterns[field] = re.compile(pattern)
                except re.error as e:
                    print(
                        f"Warning: Invalid regex pattern for field '{field}': {e}")

    def validate(self, event: Dict[str, Any]) -> List[ValidationFailure]:
        """
        Validate field values in event.

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
                                     f"Value check failed for field '{field}'")

            # Get the value
            value = get_nested_value(event, field)

            # Skip if field doesn't exist (schema checker handles this)
            if value is None:
                continue

            # Perform appropriate validation
            failure = None
            severity = self._parse_severity(severity_str)

            if check_type == 'regex':
                failure = self._check_regex(
                    field, value, rule, error_message, severity)
            elif check_type == 'enum':
                failure = self._check_enum(
                    field, value, rule, error_message, severity)
            elif check_type == 'range':
                failure = self._check_range(
                    field, value, rule, error_message, severity)
            elif check_type == 'length':
                failure = self._check_length(
                    field, value, rule, error_message, severity)

            if failure:
                failures.append(failure)

        return failures

    def _check_regex(
        self,
        field: str,
        value: Any,
        rule: Dict[str, Any],
        error_message: str,
        severity: Severity
    ) -> Optional[ValidationFailure]:
        """Check if value matches regex pattern."""
        pattern = self._compiled_patterns.get(field)

        if pattern is None:
            return None

        # Convert value to string for regex matching
        str_value = str(value)

        if not pattern.match(str_value):
            return ValidationFailure(
                check_name=f"value_check.regex.{field}",
                field_path=field,
                check_type="regex",
                severity=severity,
                error_message=error_message,
                expected_value=f"matches pattern: {rule['pattern']}",
                actual_value=str_value,
                rule_definition=rule.get('pattern')
            )

        return None

    def _check_enum(
        self,
        field: str,
        value: Any,
        rule: Dict[str, Any],
        error_message: str,
        severity: Severity
    ) -> Optional[ValidationFailure]:
        """Check if value is in allowed list."""
        allowed_values = rule.get('allowed_values', [])

        if value not in allowed_values:
            return ValidationFailure(
                check_name=f"value_check.enum.{field}",
                field_path=field,
                check_type="enum",
                severity=severity,
                error_message=error_message,
                expected_value=f"one of {allowed_values}",
                actual_value=value,
                rule_definition=str(allowed_values)
            )

        return None

    def _check_range(
        self,
        field: str,
        value: Any,
        rule: Dict[str, Any],
        error_message: str,
        severity: Severity
    ) -> Optional[ValidationFailure]:
        """Check if numeric value is within range."""
        min_value = rule.get('min_value')
        max_value = rule.get('max_value')

        # Value must be numeric
        if not isinstance(value, (int, float)):
            return ValidationFailure(
                check_name=f"value_check.range.{field}",
                field_path=field,
                check_type="range",
                severity=severity,
                error_message=f"Field '{field}' must be numeric for range check",
                expected_value="numeric value",
                actual_value=type(value).__name__
            )

        # Check min
        if min_value is not None and value < min_value:
            return ValidationFailure(
                check_name=f"value_check.range.{field}",
                field_path=field,
                check_type="range",
                severity=severity,
                error_message=error_message,
                expected_value=f">= {min_value}",
                actual_value=value
            )

        # Check max
        if max_value is not None and value > max_value:
            return ValidationFailure(
                check_name=f"value_check.range.{field}",
                field_path=field,
                check_type="range",
                severity=severity,
                error_message=error_message,
                expected_value=f"<= {max_value}",
                actual_value=value
            )

        return None

    def _check_length(
        self,
        field: str,
        value: Any,
        rule: Dict[str, Any],
        error_message: str,
        severity: Severity
    ) -> Optional[ValidationFailure]:
        """Check if string/list length is within range."""
        min_length = rule.get('min_length')
        max_length = rule.get('max_length')

        # Value must have length
        if not hasattr(value, '__len__'):
            return ValidationFailure(
                check_name=f"value_check.length.{field}",
                field_path=field,
                check_type="length",
                severity=severity,
                error_message=f"Field '{field}' must have length for length check",
                expected_value="string or list",
                actual_value=type(value).__name__
            )

        length = len(value)

        # Check min
        if min_length is not None and length < min_length:
            return ValidationFailure(
                check_name=f"value_check.length.{field}",
                field_path=field,
                check_type="length",
                severity=severity,
                error_message=error_message,
                expected_value=f"length >= {min_length}",
                actual_value=f"length = {length}"
            )

        # Check max
        if max_length is not None and length > max_length:
            return ValidationFailure(
                check_name=f"value_check.length.{field}",
                field_path=field,
                check_type="length",
                severity=severity,
                error_message=error_message,
                expected_value=f"length <= {max_length}",
                actual_value=f"length = {length}"
            )

        return None

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


# Null and empty value checker

class NullChecker:
    """
    Validates that fields don't contain null or empty values.

    Checks:
    - Null values
    - Empty strings
    - Empty lists/dicts
    """

    def __init__(self, rules: List[Dict[str, Any]]):
        """
        Initialize with null check rules from YAML.

        Args:
            rules: List of null/empty validation rules
        """
        self.rules = rules

    def validate(self, event: Dict[str, Any]) -> List[ValidationFailure]:
        """
        Validate null/empty values in event.

        Args:
            event: The event data to validate

        Returns:
            List of validation failures
        """
        failures = []

        for rule in self.rules:
            field = rule['field']
            allow_null = rule.get('allow_null', True)
            allow_empty = rule.get('allow_empty', True)
            severity_str = rule.get('severity', 'FAIL')
            error_message = rule.get('error_message',
                                     f"Field '{field}' has invalid null/empty value")

            value = get_nested_value(event, field)
            severity = self._parse_severity(severity_str)

            # Check null
            if not allow_null and value is None:
                failures.append(ValidationFailure(
                    check_name=f"null_check.{field}",
                    field_path=field,
                    check_type="null",
                    severity=severity,
                    error_message=error_message,
                    expected_value="non-null value",
                    actual_value="null"
                ))
                continue  # Don't check empty if null

            # Check empty (only for strings, lists, dicts)
            if not allow_empty and value is not None:
                if isinstance(value, (str, list, dict)) and len(value) == 0:
                    failures.append(ValidationFailure(
                        check_name=f"empty_check.{field}",
                        field_path=field,
                        check_type="empty",
                        severity=severity,
                        error_message=error_message,
                        expected_value="non-empty value",
                        actual_value=f"empty {type(value).__name__}"
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


# Utility functions

def validate_regex(
    value: str,
    pattern: str,
    field_name: str = "value"
) -> Optional[ValidationFailure]:
    """
    Quick regex validation for a single value.

    Args:
        value: Value to check
        pattern: Regex pattern
        field_name: Field name for error message

    Returns:
        ValidationFailure if no match, None if valid
    """
    try:
        compiled = re.compile(pattern)
        if not compiled.match(value):
            return ValidationFailure(
                check_name=f"regex.{field_name}",
                field_path=field_name,
                check_type="regex",
                severity=Severity.CRITICAL,
                error_message=f"Value does not match pattern",
                expected_value=f"matches {pattern}",
                actual_value=value
            )
    except re.error:
        return ValidationFailure(
            check_name=f"regex.{field_name}",
            field_path=field_name,
            check_type="regex",
            severity=Severity.CRITICAL,
            error_message="Invalid regex pattern",
            expected_value=pattern,
            actual_value="invalid pattern"
        )

    return None


def validate_enum(
    value: Any,
    allowed_values: List[Any],
    field_name: str = "value"
) -> Optional[ValidationFailure]:
    """
    Quick enum validation for a single value.

    Args:
        value: Value to check
        allowed_values: List of allowed values
        field_name: Field name for error message

    Returns:
        ValidationFailure if not in list, None if valid
    """
    if value not in allowed_values:
        return ValidationFailure(
            check_name=f"enum.{field_name}",
            field_path=field_name,
            check_type="enum",
            severity=Severity.CRITICAL,
            error_message=f"Value not in allowed list",
            expected_value=f"one of {allowed_values}",
            actual_value=value
        )

    return None

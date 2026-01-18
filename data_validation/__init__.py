"""
SentinelDQ Data Validation Package

Production-grade data validation system for event-driven data.

Main Components:
- engine: Validation orchestration
- checks: Individual validation implementations
- models: Data structures for validation results
- persistence: Database storage
- metrics: Prometheus-compatible metrics
- rules: YAML-based validation rules

Quick Start:
    from data_validation import validate_event

    result = validate_event(event_data)

    if result.passed:
        # Process event
        pass
"""

from .data_validator import DataValidator, validate_event, validate_batch, get_validator
from .models import ValidationResult, ValidationFailure, ValidationStatus, Severity
from .engine import ValidationEngine
from .metrics import get_metrics

__version__ = "1.0.0"

__all__ = [
    "DataValidator",
    "validate_event",
    "validate_batch",
    "get_validator",
    "ValidationResult",
    "ValidationFailure",
    "ValidationStatus",
    "Severity",
    "ValidationEngine",
    "get_metrics",
]

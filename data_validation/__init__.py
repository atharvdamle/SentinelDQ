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

from importlib import import_module
from typing import Any

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


def __getattr__(name: str) -> Any:
    if name in {"DataValidator", "validate_event", "validate_batch", "get_validator"}:
        module = import_module(".data_validator", __name__)
        return getattr(module, name)
    if name in {
        "ValidationResult",
        "ValidationFailure",
        "ValidationStatus",
        "Severity",
    }:
        module = import_module(".models", __name__)
        return getattr(module, name)
    if name == "ValidationEngine":
        module = import_module(".engine", __name__)
        return getattr(module, name)
    if name == "get_metrics":
        module = import_module(".metrics", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

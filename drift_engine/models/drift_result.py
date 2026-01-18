"""
Drift result models and data structures.
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum
import json


class DriftType(Enum):
    """Types of drift that can be detected."""

    SCHEMA = "schema"
    DISTRIBUTION = "distribution"
    VOLUME = "volume"


class Severity(Enum):
    """Severity levels for drift findings."""

    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"

    def __lt__(self, other):
        """Allow severity comparison."""
        order = {"INFO": 1, "WARNING": 2, "CRITICAL": 3}
        return order[self.value] < order[other.value]


@dataclass
class TimeWindow:
    """Represents a time range for drift analysis."""

    start: datetime
    end: datetime

    def __str__(self):
        return f"{self.start.strftime('%Y-%m-%d %H:%M')} to {self.end.strftime('%Y-%m-%d %H:%M')}"

    def duration_hours(self) -> float:
        """Calculate window duration in hours."""
        return (self.end - self.start).total_seconds() / 3600

    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary."""
        return {"start": self.start.isoformat(), "end": self.end.isoformat()}


@dataclass
class DriftResult:
    """
    Represents a single drift detection result.

    This is the core data structure persisted to PostgreSQL.
    """

    drift_type: DriftType
    entity: Optional[str]  # e.g., 'global', 'event_type', 'repo'
    # e.g., 'type', 'payload.size', or None for entity-level
    field_name: Optional[str]

    baseline_window: TimeWindow
    current_window: TimeWindow

    metric_name: str  # e.g., 'psi', 'ks_statistic', 'z_score', 'field_added'
    baseline_value: Any  # Flexible: could be distribution dict, count, etc.
    current_value: Any
    drift_score: float  # Normalized 0-1 score where higher = more drift

    severity: Severity
    detected_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database insertion."""
        return {
            "drift_type": self.drift_type.value,
            "entity": self.entity,
            "field_name": self.field_name,
            "baseline_start": self.baseline_window.start,
            "baseline_end": self.baseline_window.end,
            "current_start": self.current_window.start,
            "current_end": self.current_window.end,
            "metric_name": self.metric_name,
            "baseline_value": (json.dumps(self.baseline_value) if self.baseline_value is not None else None),
            "current_value": (json.dumps(self.current_value) if self.current_value is not None else None),
            "drift_score": self.drift_score,
            "severity": self.severity.value,
            "detected_at": self.detected_at,
            "metadata": json.dumps(self.metadata),
        }

    def __str__(self):
        """Human-readable representation."""
        return (
            f"[{self.severity.value}] {self.drift_type.value.upper()} DRIFT: "
            f"{self.entity or 'global'}.{self.field_name or 'N/A'} - "
            f"{self.metric_name}={self.drift_score:.3f}"
        )


@dataclass
class DriftSummary:
    """
    Aggregated summary of drift detection run.
    """

    run_timestamp: datetime
    baseline_window: TimeWindow
    current_window: TimeWindow

    total_checks: int
    total_drifts: int

    critical_count: int = 0
    warning_count: int = 0
    info_count: int = 0

    drifts_by_type: Dict[str, int] = field(default_factory=dict)

    results: List[DriftResult] = field(default_factory=list)

    def add_result(self, result: DriftResult):
        """Add a drift result to summary."""
        self.results.append(result)
        self.total_drifts += 1

        if result.severity == Severity.CRITICAL:
            self.critical_count += 1
        elif result.severity == Severity.WARNING:
            self.warning_count += 1
        else:
            self.info_count += 1

        drift_type_key = result.drift_type.value
        self.drifts_by_type[drift_type_key] = self.drifts_by_type.get(drift_type_key, 0) + 1

    def get_critical_drifts(self) -> List[DriftResult]:
        """Get only critical severity drifts."""
        return [r for r in self.results if r.severity == Severity.CRITICAL]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "run_timestamp": self.run_timestamp.isoformat(),
            "baseline_window": self.baseline_window.to_dict(),
            "current_window": self.current_window.to_dict(),
            "total_checks": self.total_checks,
            "total_drifts": self.total_drifts,
            "critical_count": self.critical_count,
            "warning_count": self.warning_count,
            "info_count": self.info_count,
            "drifts_by_type": self.drifts_by_type,
        }

    def __str__(self):
        """Human-readable summary."""
        return (
            f"Drift Detection Summary ({self.run_timestamp.strftime('%Y-%m-%d %H:%M UTC')})\n"
            f"Windows: {self.baseline_window} (baseline) vs {self.current_window} (current)\n"
            f"Total Checks: {self.total_checks}\n"
            f"Total Drifts: {self.total_drifts}\n"
            f"  - CRITICAL: {self.critical_count}\n"
            f"  - WARNING: {self.warning_count}\n"
            f"  - INFO: {self.info_count}\n"
            f"By Type: {self.drifts_by_type}"
        )

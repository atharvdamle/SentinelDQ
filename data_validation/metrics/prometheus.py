"""
Prometheus Metrics for Data Validation

Exposes validation metrics in Prometheus format for monitoring and alerting.

Metrics Exposed:
- sentineldq_validation_total: Total events validated
- sentineldq_validation_passed: Events that passed validation
- sentineldq_validation_warned: Events with warnings
- sentineldq_validation_failed: Events that failed validation
- sentineldq_validation_duration_seconds: Validation processing time
- sentineldq_validation_check_failures: Failures by check type
"""

from typing import Dict, Any, Optional
from collections import defaultdict
import time

from ..models.validation_result import ValidationResult, ValidationStatus


class PrometheusMetrics:
    """
    Collects and formats validation metrics for Prometheus.

    Metrics follow Prometheus naming conventions:
    - Counter: sentineldq_validation_total (monotonically increasing)
    - Gauge: sentineldq_validation_failures_by_check (current value)
    - Histogram: sentineldq_validation_duration_seconds (distribution)

    Usage:
        metrics = PrometheusMetrics()

        # Record validation result
        metrics.record_validation(result)

        # Export metrics
        print(metrics.export_text())
    """

    def __init__(self):
        """Initialize metrics collectors."""
        # Counters (monotonically increasing)
        self.total_validations = 0
        self.passed_validations = 0
        self.warned_validations = 0
        self.failed_validations = 0

        # Failures by check type (for alerting)
        self.failures_by_check: Dict[str, int] = defaultdict(int)

        # Failures by severity
        self.failures_by_severity: Dict[str, int] = defaultdict(int)

        # Processing time histogram (buckets in seconds)
        self.duration_buckets = [0.001, 0.005,
                                 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
        self.duration_counts = defaultdict(int)
        self.duration_sum = 0.0
        self.duration_count = 0

        # Recent failures (for debugging)
        self.recent_failures = []
        self.max_recent_failures = 100

        # Start time for uptime metric
        self.start_time = time.time()

    def record_validation(self, result: ValidationResult) -> None:
        """
        Record a validation result and update metrics.

        Args:
            result: ValidationResult to record
        """
        # Update counters
        self.total_validations += 1

        if result.status == ValidationStatus.PASS:
            self.passed_validations += 1
        elif result.status == ValidationStatus.WARN:
            self.warned_validations += 1
        elif result.status == ValidationStatus.FAIL:
            self.failed_validations += 1

        # Record failures by check type
        for failure in result.failures:
            self.failures_by_check[failure.check_type] += 1
            self.failures_by_severity[failure.severity.value] += 1

        # Record processing time
        if result.processing_time_ms:
            duration_seconds = result.processing_time_ms / 1000.0
            self.duration_sum += duration_seconds
            self.duration_count += 1

            # Update histogram buckets
            for bucket in self.duration_buckets:
                if duration_seconds <= bucket:
                    self.duration_counts[bucket] += 1

        # Track recent failures
        if result.status in (ValidationStatus.WARN, ValidationStatus.FAIL):
            self.recent_failures.append({
                'event_id': result.event_id,
                'status': result.status.value,
                'failures': len(result.failures),
                'timestamp': result.validation_timestamp.isoformat()
            })

            # Keep only recent failures
            if len(self.recent_failures) > self.max_recent_failures:
                self.recent_failures = self.recent_failures[-self.max_recent_failures:]

    def export_text(self) -> str:
        """
        Export metrics in Prometheus text format.

        Returns:
            Metrics formatted as Prometheus text exposition format
        """
        lines = []

        # Header
        lines.append("# SentinelDQ Validation Metrics")
        lines.append("")

        # Total validations
        lines.append(
            "# HELP sentineldq_validation_total Total number of events validated")
        lines.append("# TYPE sentineldq_validation_total counter")
        lines.append(f"sentineldq_validation_total {self.total_validations}")
        lines.append("")

        # Passed validations
        lines.append(
            "# HELP sentineldq_validation_passed Events that passed validation")
        lines.append("# TYPE sentineldq_validation_passed counter")
        lines.append(f"sentineldq_validation_passed {self.passed_validations}")
        lines.append("")

        # Warned validations
        lines.append(
            "# HELP sentineldq_validation_warned Events with warnings")
        lines.append("# TYPE sentineldq_validation_warned counter")
        lines.append(f"sentineldq_validation_warned {self.warned_validations}")
        lines.append("")

        # Failed validations
        lines.append(
            "# HELP sentineldq_validation_failed Events that failed validation")
        lines.append("# TYPE sentineldq_validation_failed counter")
        lines.append(f"sentineldq_validation_failed {self.failed_validations}")
        lines.append("")

        # Failures by check type
        lines.append(
            "# HELP sentineldq_validation_check_failures Validation failures by check type")
        lines.append("# TYPE sentineldq_validation_check_failures counter")
        for check_type, count in sorted(self.failures_by_check.items()):
            lines.append(
                f'sentineldq_validation_check_failures{{check_type="{check_type}"}} {count}')
        lines.append("")

        # Failures by severity
        lines.append(
            "# HELP sentineldq_validation_severity_failures Failures by severity level")
        lines.append("# TYPE sentineldq_validation_severity_failures counter")
        for severity, count in sorted(self.failures_by_severity.items()):
            lines.append(
                f'sentineldq_validation_severity_failures{{severity="{severity}"}} {count}')
        lines.append("")

        # Processing duration histogram
        lines.append(
            "# HELP sentineldq_validation_duration_seconds Validation processing time distribution")
        lines.append("# TYPE sentineldq_validation_duration_seconds histogram")
        cumulative = 0
        for bucket in sorted(self.duration_buckets):
            cumulative += self.duration_counts[bucket]
            lines.append(
                f'sentineldq_validation_duration_seconds_bucket{{le="{bucket}"}} {cumulative}')
        lines.append(
            f'sentineldq_validation_duration_seconds_bucket{{le="+Inf"}} {self.duration_count}')
        lines.append(
            f'sentineldq_validation_duration_seconds_sum {self.duration_sum:.6f}')
        lines.append(
            f'sentineldq_validation_duration_seconds_count {self.duration_count}')
        lines.append("")

        # Pass rate (derived metric)
        pass_rate = (self.passed_validations /
                     self.total_validations) if self.total_validations > 0 else 0
        lines.append(
            "# HELP sentineldq_validation_pass_rate Ratio of passed validations")
        lines.append("# TYPE sentineldq_validation_pass_rate gauge")
        lines.append(f"sentineldq_validation_pass_rate {pass_rate:.4f}")
        lines.append("")

        # Uptime
        uptime = time.time() - self.start_time
        lines.append(
            "# HELP sentineldq_validation_uptime_seconds Time since metrics started")
        lines.append("# TYPE sentineldq_validation_uptime_seconds counter")
        lines.append(f"sentineldq_validation_uptime_seconds {uptime:.2f}")
        lines.append("")

        return "\n".join(lines)

    def export_json(self) -> Dict[str, Any]:
        """
        Export metrics as JSON (for logging/debugging).

        Returns:
            Metrics as dictionary
        """
        pass_rate = (self.passed_validations /
                     self.total_validations) if self.total_validations > 0 else 0
        warn_rate = (self.warned_validations /
                     self.total_validations) if self.total_validations > 0 else 0
        fail_rate = (self.failed_validations /
                     self.total_validations) if self.total_validations > 0 else 0
        avg_duration = (self.duration_sum /
                        self.duration_count) if self.duration_count > 0 else 0

        return {
            'total_validations': self.total_validations,
            'passed_validations': self.passed_validations,
            'warned_validations': self.warned_validations,
            'failed_validations': self.failed_validations,
            'pass_rate': pass_rate,
            'warn_rate': warn_rate,
            'fail_rate': fail_rate,
            'avg_processing_time_seconds': avg_duration,
            'total_processing_time_seconds': self.duration_sum,
            'failures_by_check': dict(self.failures_by_check),
            'failures_by_severity': dict(self.failures_by_severity),
            'recent_failures': self.recent_failures[-10:],  # Last 10
            'uptime_seconds': time.time() - self.start_time
        }

    def reset(self) -> None:
        """Reset all metrics (useful for testing)."""
        self.total_validations = 0
        self.passed_validations = 0
        self.warned_validations = 0
        self.failed_validations = 0
        self.failures_by_check.clear()
        self.failures_by_severity.clear()
        self.duration_counts.clear()
        self.duration_sum = 0.0
        self.duration_count = 0
        self.recent_failures.clear()
        self.start_time = time.time()

    def get_alert_conditions(self) -> Dict[str, bool]:
        """
        Check conditions that should trigger alerts.

        Returns:
            Dictionary of alert conditions and their status
        """
        total = self.total_validations

        if total == 0:
            return {'no_data': True}

        fail_rate = self.failed_validations / total
        warn_rate = self.warned_validations / total
        avg_duration = self.duration_sum / \
            self.duration_count if self.duration_count > 0 else 0

        return {
            'high_failure_rate': fail_rate > 0.05,  # More than 5% failures
            'high_warning_rate': warn_rate > 0.20,  # More than 20% warnings
            'slow_validation': avg_duration > 0.1,  # Slower than 100ms average
            'no_data': total == 0,
            'failure_rate': fail_rate,
            'warning_rate': warn_rate,
            'avg_duration_seconds': avg_duration
        }


# Global metrics instance (singleton pattern)
_global_metrics: Optional[PrometheusMetrics] = None


def get_metrics() -> PrometheusMetrics:
    """
    Get global metrics instance (singleton).

    Returns:
        Global PrometheusMetrics instance
    """
    global _global_metrics
    if _global_metrics is None:
        _global_metrics = PrometheusMetrics()
    return _global_metrics


def reset_metrics() -> None:
    """Reset global metrics instance."""
    global _global_metrics
    if _global_metrics:
        _global_metrics.reset()


# HTTP endpoint function (for integration with web frameworks)

def metrics_endpoint() -> str:
    """
    HTTP endpoint handler for Prometheus scraping.

    Returns:
        Metrics in Prometheus text format

    Usage with Flask:
        @app.route('/metrics')
        def metrics():
            return Response(metrics_endpoint(), mimetype='text/plain')
    """
    return get_metrics().export_text()

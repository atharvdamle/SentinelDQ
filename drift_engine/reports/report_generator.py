"""
Drift detection report generator.
"""

from typing import List, Optional
import json
from datetime import datetime

from drift_engine.models import DriftSummary, DriftResult, Severity


class ReportGenerator:
    """
    Generates human-readable reports from drift detection results.
    """

    @staticmethod
    def generate_text_report(summary: DriftSummary) -> str:
        """
        Generate a text-based summary report.

        Args:
            summary: DriftSummary object

        Returns:
            Formatted text report
        """
        lines = []
        lines.append("=" * 80)
        lines.append("SENTINELDQ DRIFT DETECTION REPORT")
        lines.append("=" * 80)
        lines.append("")
        lines.append(
            f"Run Timestamp: {summary.run_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        lines.append("")
        lines.append(f"Baseline Window: {summary.baseline_window}")
        lines.append(f"Current Window:  {summary.current_window}")
        lines.append("")
        lines.append("-" * 80)
        lines.append("SUMMARY")
        lines.append("-" * 80)
        lines.append(f"Total Checks Performed: {summary.total_checks}")
        lines.append(f"Total Drifts Detected:  {summary.total_drifts}")
        lines.append("")
        lines.append(f"  CRITICAL: {summary.critical_count}")
        lines.append(f"  WARNING:  {summary.warning_count}")
        lines.append(f"  INFO:     {summary.info_count}")
        lines.append("")

        if summary.drifts_by_type:
            lines.append("Drifts by Type:")
            for drift_type, count in sorted(summary.drifts_by_type.items()):
                lines.append(f"  {drift_type}: {count}")

        lines.append("")

        if not summary.results:
            lines.append("No drift detected. Data is stable.")
            lines.append("=" * 80)
            return "\n".join(lines)

        # Group results by severity
        critical = [r for r in summary.results if r.severity == Severity.CRITICAL]
        warning = [r for r in summary.results if r.severity == Severity.WARNING]
        info = [r for r in summary.results if r.severity == Severity.INFO]

        # Critical drifts
        if critical:
            lines.append("-" * 80)
            lines.append(f"CRITICAL DRIFTS ({len(critical)})")
            lines.append("-" * 80)
            for drift in critical:
                lines.extend(ReportGenerator._format_drift_detail(drift))
            lines.append("")

        # Warning drifts
        if warning:
            lines.append("-" * 80)
            lines.append(f"WARNING DRIFTS ({len(warning)})")
            lines.append("-" * 80)
            for drift in warning:
                lines.extend(ReportGenerator._format_drift_detail(drift))
            lines.append("")

        # Info drifts (summarize, don't detail)
        if info:
            lines.append("-" * 80)
            lines.append(f"INFO DRIFTS ({len(info)})")
            lines.append("-" * 80)
            for drift in info[:10]:  # Show first 10
                lines.append(f"  • {drift}")
            if len(info) > 10:
                lines.append(f"  ... and {len(info) - 10} more")
            lines.append("")

        lines.append("=" * 80)
        lines.append("END OF REPORT")
        lines.append("=" * 80)

        return "\n".join(lines)

    @staticmethod
    def _format_drift_detail(drift: DriftResult) -> List[str]:
        """Format a single drift result with details."""
        lines = []
        lines.append("")
        lines.append(f"[{drift.severity.value}] {drift.drift_type.value.upper()} DRIFT")
        lines.append(f"  Entity: {drift.entity or 'global'}")
        lines.append(f"  Field:  {drift.field_name or 'N/A'}")
        lines.append(f"  Metric: {drift.metric_name}")
        lines.append(f"  Drift Score: {drift.drift_score:.3f}")

        # Add specific metadata
        if drift.metadata:
            lines.append("  Details:")
            for key, value in drift.metadata.items():
                if isinstance(value, (int, float)):
                    lines.append(f"    {key}: {value}")
                elif isinstance(value, dict) and len(value) <= 5:
                    lines.append(f"    {key}: {value}")
                elif isinstance(value, str):
                    lines.append(f"    {key}: {value}")

        # Show baseline vs current values for certain drift types
        if drift.metric_name in ["field_added", "field_removed", "type_change"]:
            lines.append(f"  Baseline: {drift.baseline_value}")
            lines.append(f"  Current:  {drift.current_value}")

        return lines

    @staticmethod
    def generate_json_report(summary: DriftSummary) -> str:
        """
        Generate a JSON report.

        Args:
            summary: DriftSummary object

        Returns:
            JSON string
        """
        report = summary.to_dict()

        # Add detailed results
        report["results"] = []
        for drift in summary.results:
            report["results"].append(
                {
                    "drift_type": drift.drift_type.value,
                    "entity": drift.entity,
                    "field_name": drift.field_name,
                    "metric_name": drift.metric_name,
                    "drift_score": drift.drift_score,
                    "severity": drift.severity.value,
                    "baseline_value": drift.baseline_value,
                    "current_value": drift.current_value,
                    "metadata": drift.metadata,
                    "detected_at": drift.detected_at.isoformat(),
                }
            )

        return json.dumps(report, indent=2, default=str)

    @staticmethod
    def generate_markdown_report(summary: DriftSummary) -> str:
        """
        Generate a Markdown report.

        Args:
            summary: DriftSummary object

        Returns:
            Markdown formatted report
        """
        lines = []
        lines.append("# SentinelDQ Drift Detection Report")
        lines.append("")
        lines.append(
            f"**Run Timestamp:** {summary.run_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        lines.append("")
        lines.append(f"**Baseline Window:** {summary.baseline_window}")
        lines.append(f"**Current Window:** {summary.current_window}")
        lines.append("")

        lines.append("## Summary")
        lines.append("")
        lines.append(f"- **Total Checks:** {summary.total_checks}")
        lines.append(f"- **Total Drifts:** {summary.total_drifts}")
        lines.append(f"  - CRITICAL: {summary.critical_count}")
        lines.append(f"  - WARNING: {summary.warning_count}")
        lines.append(f"  - INFO: {summary.info_count}")
        lines.append("")

        if not summary.results:
            lines.append("✅ **No drift detected. Data is stable.**")
            return "\n".join(lines)

        # Critical drifts
        critical = [r for r in summary.results if r.severity == Severity.CRITICAL]
        if critical:
            lines.append(f"## 🚨 Critical Drifts ({len(critical)})")
            lines.append("")
            for drift in critical:
                lines.append(
                    f"### {drift.entity or 'global'}.{drift.field_name or 'N/A'}"
                )
                lines.append(f"- **Type:** {drift.drift_type.value}")
                lines.append(f"- **Metric:** {drift.metric_name}")
                lines.append(f"- **Drift Score:** {drift.drift_score:.3f}")
                lines.append(f"- **Details:** {drift.metadata}")
                lines.append("")

        # Warning drifts
        warning = [r for r in summary.results if r.severity == Severity.WARNING]
        if warning:
            lines.append(f"## ⚠️ Warning Drifts ({len(warning)})")
            lines.append("")
            for drift in warning:
                lines.append(
                    f"- `{drift.entity or 'global'}.{drift.field_name or 'N/A'}`: {drift.metric_name} = {drift.drift_score:.3f}"
                )
            lines.append("")

        # Info drifts
        info = [r for r in summary.results if r.severity == Severity.INFO]
        if info:
            lines.append(f"## ℹ️ Info Drifts ({len(info)})")
            lines.append("")
            for drift in info[:10]:
                lines.append(
                    f"- `{drift.entity or 'global'}.{drift.field_name or 'N/A'}`: {drift.metric_name}"
                )
            if len(info) > 10:
                lines.append(f"- ... and {len(info) - 10} more")
            lines.append("")

        return "\n".join(lines)

    @staticmethod
    def save_report(summary: DriftSummary, output_path: str, format: str = "text"):
        """
        Save report to file.

        Args:
            summary: DriftSummary object
            output_path: Path to save report
            format: 'text', 'json', or 'markdown'
        """
        if format == "json":
            content = ReportGenerator.generate_json_report(summary)
        elif format == "markdown":
            content = ReportGenerator.generate_markdown_report(summary)
        else:
            content = ReportGenerator.generate_text_report(summary)

        with open(output_path, "w") as f:
            f.write(content)

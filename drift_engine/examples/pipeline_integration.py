"""
Example: Drift detection integration with SentinelDQ pipeline.

This demonstrates how to integrate drift detection into the existing
data pipeline workflow.
"""

from drift_engine.models import Severity
from drift_engine.reports import ReportGenerator
from drift_engine.engine import DriftRunner
import logging
from datetime import datetime
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_drift_detection_pipeline():
    """
    Complete drift detection pipeline example.

    This simulates a scheduled job that:
    1. Runs drift detection
    2. Generates reports
    3. Takes action based on severity
    """

    logger.info("=" * 80)
    logger.info("STARTING SENTINELDQ DRIFT DETECTION PIPELINE")
    logger.info("=" * 80)

    try:
        # 1. Initialize drift runner
        logger.info("Initializing drift detection engine...")
        runner = DriftRunner()

        # 2. Run drift detection
        logger.info("Executing drift detection...")
        summary = runner.run()

        # 3. Generate report
        logger.info("Generating drift report...")
        text_report = ReportGenerator.generate_text_report(summary)

        # Print to console
        print("\n" + text_report + "\n")

        # Save to file with timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        report_dir = Path(__file__).parent.parent / "reports" / "generated"
        report_dir.mkdir(parents=True, exist_ok=True)

        # Save in multiple formats
        text_path = report_dir / f"drift_report_{timestamp}.txt"
        json_path = report_dir / f"drift_report_{timestamp}.json"
        md_path = report_dir / f"drift_report_{timestamp}.md"

        with open(text_path, "w") as f:
            f.write(text_report)
        logger.info(f"Saved text report: {text_path}")

        with open(json_path, "w") as f:
            f.write(ReportGenerator.generate_json_report(summary))
        logger.info(f"Saved JSON report: {json_path}")

        with open(md_path, "w") as f:
            f.write(ReportGenerator.generate_markdown_report(summary))
        logger.info(f"Saved Markdown report: {md_path}")

        # 4. Take action based on severity
        if summary.critical_count > 0:
            logger.critical(
                f"🚨 CRITICAL ALERT: {summary.critical_count} critical drifts detected!"
            )

            # In production, this would:
            # - Send Slack/email alerts
            # - Create PagerDuty incidents
            # - Trigger automated remediation

            critical_drifts = summary.get_critical_drifts()
            logger.critical("Critical drift details:")
            for drift in critical_drifts:
                logger.critical(f"  - {drift}")

            return 1  # Exit code 1 for critical issues

        elif summary.warning_count > 0:
            logger.warning(
                f"⚠️  WARNING: {summary.warning_count} warning-level drifts detected"
            )

            # In production:
            # - Send Slack notifications
            # - Update monitoring dashboards

            return 0  # Exit code 0, but warnings noted

        else:
            logger.info("✅ No significant drift detected. Data is stable.")
            return 0

    except Exception as e:
        logger.error(f"Drift detection pipeline failed: {e}", exc_info=True)

        # In production:
        # - Alert on-call engineer
        # - Create error ticket

        return 2  # Exit code 2 for execution failure


def example_custom_analysis():
    """
    Example of custom drift analysis with programmatic access.
    """
    logger.info("\n" + "=" * 80)
    logger.info("CUSTOM DRIFT ANALYSIS EXAMPLE")
    logger.info("=" * 80)

    runner = DriftRunner()
    summary = runner.run()

    # Custom analysis: Group drifts by entity
    drifts_by_entity = {}
    for drift in summary.results:
        entity = drift.entity or "global"
        if entity not in drifts_by_entity:
            drifts_by_entity[entity] = []
        drifts_by_entity[entity].append(drift)

    print("\nDrifts Grouped by Entity:")
    print("-" * 80)
    for entity, drifts in sorted(drifts_by_entity.items()):
        print(f"\n{entity.upper()} ({len(drifts)} drifts):")
        for drift in drifts:
            print(
                f"  [{drift.severity.value}] {drift.metric_name}: {drift.drift_score:.3f}"
            )

    # Custom analysis: Identify fields with multiple drift types
    field_drift_types = {}
    for drift in summary.results:
        field_key = f"{drift.entity}.{drift.field_name}"
        if field_key not in field_drift_types:
            field_drift_types[field_key] = set()
        field_drift_types[field_key].add(drift.drift_type.value)

    multi_drift_fields = {
        field: types for field, types in field_drift_types.items() if len(types) > 1
    }

    if multi_drift_fields:
        print("\nFields with Multiple Drift Types (high concern):")
        print("-" * 80)
        for field, drift_types in multi_drift_fields.items():
            print(f"  {field}: {', '.join(drift_types)}")


def example_threshold_tuning():
    """
    Example of analyzing drift scores to tune thresholds.
    """
    logger.info("\n" + "=" * 80)
    logger.info("THRESHOLD TUNING ANALYSIS")
    logger.info("=" * 80)

    runner = DriftRunner()
    summary = runner.run()

    # Analyze drift score distributions by type
    score_distributions = {}
    for drift in summary.results:
        drift_type = drift.drift_type.value
        if drift_type not in score_distributions:
            score_distributions[drift_type] = []
        score_distributions[drift_type].append(drift.drift_score)

    print("\nDrift Score Distributions (for threshold tuning):")
    print("-" * 80)
    for drift_type, scores in score_distributions.items():
        if scores:
            avg_score = sum(scores) / len(scores)
            max_score = max(scores)
            min_score = min(scores)
            print(f"\n{drift_type.upper()}:")
            print(f"  Count: {len(scores)}")
            print(f"  Average: {avg_score:.3f}")
            print(f"  Range: [{min_score:.3f}, {max_score:.3f}]")

    # Recommendation
    print("\nThreshold Tuning Recommendations:")
    print("-" * 80)
    for drift_type, scores in score_distributions.items():
        if scores:
            avg = sum(scores) / len(scores)
            if avg > 0.8:
                print(
                    f"  {drift_type}: Consider LOWERING thresholds (many high-score drifts)"
                )
            elif avg < 0.3:
                print(
                    f"  {drift_type}: Consider RAISING thresholds (many low-score drifts)"
                )
            else:
                print(f"  {drift_type}: Current thresholds appear well-calibrated")


if __name__ == "__main__":
    # Run main pipeline
    exit_code = run_drift_detection_pipeline()

    # Optionally run custom analyses
    if exit_code == 0:  # Only if no critical issues
        try:
            example_custom_analysis()
            example_threshold_tuning()
        except Exception as e:
            logger.error(f"Custom analysis failed: {e}")

    sys.exit(exit_code)

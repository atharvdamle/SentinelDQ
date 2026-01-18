#!/usr/bin/env python3
"""
Scheduled Drift Detection Service

Runs drift detection periodically based on the configured schedule.
"""

from drift_engine.reports import ReportGenerator
from drift_engine.engine import DriftRunner
import logging
import time
import schedule
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_drift_detection():
    """Run drift detection and log results."""
    try:
        logger.info("="*60)
        logger.info("Starting scheduled drift detection run")
        logger.info("="*60)

        # Initialize drift runner
        runner = DriftRunner()

        # Run drift detection
        summary = runner.run()

        # Generate and log text report
        report = ReportGenerator.generate_text_report(summary)
        print("\n" + report)

        # Log summary
        if summary.critical_count > 0:
            logger.warning(
                f"Drift detection complete: {summary.critical_count} CRITICAL, "
                f"{summary.warning_count} WARNING, {summary.info_count} INFO drifts"
            )
        elif summary.warning_count > 0:
            logger.info(
                f"Drift detection complete: {summary.warning_count} WARNING, "
                f"{summary.info_count} INFO drifts"
            )
        else:
            logger.info(
                f"Drift detection complete: {summary.info_count} INFO drifts")

    except Exception as e:
        logger.error(f"Drift detection failed: {e}", exc_info=True)


def main():
    """Main service entry point."""
    logger.info("Drift Detection Service starting...")

    # Load schedule configuration
    runner = DriftRunner()
    schedule_cron = runner.config.get("execution", {}).get(
        "schedule_cron", "0 */6 * * *")

    # Parse cron: "0 */6 * * *" means every 6 hours
    # For simplicity, we'll use schedule library with hours
    # Extract interval from cron (assuming format: "0 */N * * *")
    try:
        parts = schedule_cron.split()
        if len(parts) >= 2 and parts[1].startswith("*/"):
            interval_hours = int(parts[1].replace("*/", ""))
            logger.info(
                f"Scheduling drift detection every {interval_hours} hours")
            schedule.every(interval_hours).hours.do(run_drift_detection)
        else:
            # Default to 6 hours if cron parsing fails
            logger.warning(
                f"Could not parse cron '{schedule_cron}', using default 6 hours")
            schedule.every(6).hours.do(run_drift_detection)
    except Exception as e:
        logger.error(f"Error parsing schedule: {e}, using default 6 hours")
        schedule.every(6).hours.do(run_drift_detection)

    # Run immediately on startup
    logger.info("Running initial drift detection...")
    run_drift_detection()

    # Keep running scheduled tasks
    logger.info("Drift Detection Service is now running. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Drift Detection Service shutting down...")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

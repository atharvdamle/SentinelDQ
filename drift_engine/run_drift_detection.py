#!/usr/bin/env python3
"""
SentinelDQ Drift Detection CLI

Command-line interface for running drift detection.
"""

from drift_engine.reports import ReportGenerator
from drift_engine.engine import DriftRunner
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def setup_logging(level: str = "INFO"):
    """Configure logging."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description="SentinelDQ Drift Detection Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run drift detection with default configuration
  python run_drift_detection.py
  
  # Run and save report as JSON
  python run_drift_detection.py --output report.json --format json
  
  # Run with custom config and debug logging
  python run_drift_detection.py --config custom_config.yaml --log-level DEBUG
  
  # Print report to console only
  python run_drift_detection.py --console-only
        """,
    )

    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to drift_config.yaml (default: drift_engine/config/drift_config.yaml)",
    )

    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Path to save report (default: print to console)",
    )

    parser.add_argument(
        "--format",
        type=str,
        choices=["text", "json", "markdown"],
        default="text",
        help="Report output format (default: text)",
    )

    parser.add_argument(
        "--console-only",
        action="store_true",
        help="Only print to console, don't save to file",
    )

    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)

    try:
        # Initialize drift runner
        logger.info("Initializing Drift Detection Engine...")
        runner = DriftRunner(config_path=args.config)

        # Run drift detection
        logger.info("Running drift detection...")
        summary = runner.run()

        # Generate report
        if args.format == "json":
            report = ReportGenerator.generate_json_report(summary)
        elif args.format == "markdown":
            report = ReportGenerator.generate_markdown_report(summary)
        else:
            report = ReportGenerator.generate_text_report(summary)

        # Output report
        if args.output and not args.console_only:
            with open(args.output, "w") as f:
                f.write(report)
            logger.info(f"Report saved to {args.output}")

        # Always print to console
        print("\n" + report)

        # Exit with appropriate code
        if summary.critical_count > 0:
            logger.warning(f"Detected {summary.critical_count} CRITICAL drifts")
            sys.exit(1)
        elif summary.warning_count > 0:
            logger.info(f"Detected {summary.warning_count} WARNING drifts")
            sys.exit(0)
        else:
            logger.info("No significant drift detected")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Drift detection failed: {e}", exc_info=True)
        sys.exit(2)


if __name__ == "__main__":
    main()

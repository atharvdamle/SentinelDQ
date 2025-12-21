"""
Main drift detection engine - orchestrates profiling and drift detection.
"""

import psycopg2
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
import yaml
import os

from drift_engine.models import TimeWindow, DriftSummary
from drift_engine.profiles import SchemaProfile, StatisticalProfile, VolumeProfile
from drift_engine.detectors import SchemaDriftDetector, DistributionDriftDetector, VolumeDriftDetector
from drift_engine.persistence import DriftPostgresWriter

logger = logging.getLogger(__name__)


class DriftRunner:
    """
    Main drift detection engine.

    Responsibilities:
    1. Load configuration
    2. Define time windows
    3. Fetch data from PostgreSQL
    4. Build profiles for baseline and current windows
    5. Run drift detectors
    6. Persist results
    7. Generate summary reports
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize drift runner.

        Args:
            config_path: Path to drift_config.yaml (optional)
        """
        # Load configuration
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__),
                "..",
                "config",
                "drift_config.yaml"
            )

        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        logger.info(f"Loaded drift configuration from {config_path}")

        # Database connection parameters
        self.db_host = os.getenv("POSTGRES_HOST", "localhost")
        self.db_port = int(os.getenv("POSTGRES_PORT", 5432))
        self.db_name = os.getenv("POSTGRES_DB", "SentinelDQ_DB")
        self.db_user = os.getenv("POSTGRES_USER", "postgres")
        self.db_password = os.getenv("POSTGRES_PASSWORD", "")

        # Initialize detectors
        self.schema_detector = SchemaDriftDetector(
            self.config["thresholds"]["schema"])
        self.distribution_detector = DistributionDriftDetector(
            self.config["thresholds"]["distribution"])
        self.volume_detector = VolumeDriftDetector(
            self.config["thresholds"]["volume"])

        self.min_sample_size = self.config["profiling"]["min_sample_size"]

    def run(self, reference_time: Optional[datetime] = None) -> DriftSummary:
        """
        Execute complete drift detection pipeline.

        Args:
            reference_time: Time to use as "now" (defaults to current UTC time)

        Returns:
            DriftSummary with all detected drifts
        """
        if reference_time is None:
            reference_time = datetime.utcnow()

        logger.info(f"Starting drift detection run at {reference_time}")

        # 1. Define time windows
        baseline_window, current_window = self._define_windows(reference_time)

        logger.info(f"Baseline window: {baseline_window}")
        logger.info(f"Current window: {current_window}")

        # 2. Fetch data for each window
        baseline_data = self._fetch_data(baseline_window)
        current_data = self._fetch_data(current_window)

        logger.info(f"Fetched {len(baseline_data)} baseline records")
        logger.info(f"Fetched {len(current_data)} current records")

        # Check minimum sample size
        if len(baseline_data) < self.min_sample_size:
            logger.warning(
                f"Baseline data ({len(baseline_data)} records) below minimum "
                f"sample size ({self.min_sample_size}). Skipping drift detection."
            )
            return DriftSummary(
                run_timestamp=reference_time,
                baseline_window=baseline_window,
                current_window=current_window,
                total_checks=0,
                total_drifts=0
            )

        if len(current_data) < self.min_sample_size:
            logger.warning(
                f"Current data ({len(current_data)} records) below minimum "
                f"sample size ({self.min_sample_size}). Skipping drift detection."
            )
            return DriftSummary(
                run_timestamp=reference_time,
                baseline_window=baseline_window,
                current_window=current_window,
                total_checks=0,
                total_drifts=0
            )

        # 3. Build profiles
        logger.info("Building profiles...")

        baseline_schema_profile = self._build_schema_profile(baseline_data)
        current_schema_profile = self._build_schema_profile(current_data)

        baseline_stat_profile = self._build_statistical_profile(baseline_data)
        current_stat_profile = self._build_statistical_profile(current_data)

        baseline_volume_profile = self._build_volume_profile(baseline_data)
        current_volume_profile = self._build_volume_profile(current_data)

        # 4. Run drift detection
        logger.info("Running drift detectors...")

        summary = DriftSummary(
            run_timestamp=reference_time,
            baseline_window=baseline_window,
            current_window=current_window,
            total_checks=0,
            total_drifts=0
        )

        # Schema drift
        if self.config["targets"]["schema_drift"]["enabled"]:
            schema_drifts = self.schema_detector.detect(
                baseline_schema_profile,
                current_schema_profile,
                baseline_window,
                current_window
            )
            for drift in schema_drifts:
                summary.add_result(drift)
            summary.total_checks += len(baseline_schema_profile.fields) + \
                len(current_schema_profile.fields)

        # Distribution drift
        if self.config["targets"]["distribution_drift"]["enabled"]:
            distribution_drifts = self.distribution_detector.detect(
                baseline_stat_profile,
                current_stat_profile,
                baseline_window,
                current_window
            )
            for drift in distribution_drifts:
                summary.add_result(drift)
            summary.total_checks += (
                len(baseline_stat_profile.categorical) +
                len(baseline_stat_profile.numerical)
            )

        # Volume drift
        if self.config["targets"]["volume_drift"]["enabled"]:
            volume_drifts = self.volume_detector.detect(
                baseline_volume_profile,
                current_volume_profile,
                baseline_window,
                current_window
            )
            for drift in volume_drifts:
                summary.add_result(drift)
            summary.total_checks += 1 + len(baseline_volume_profile.per_entity)

        logger.info(
            f"Drift detection complete: {summary.total_drifts} drifts detected")

        # 5. Persist results
        if summary.total_drifts > 0:
            self._persist_results(summary.results)

        return summary

    def _define_windows(self, reference_time: datetime) -> tuple[TimeWindow, TimeWindow]:
        """
        Define baseline and current time windows.

        Returns:
            (baseline_window, current_window)
        """
        baseline_days = self.config["windowing"]["baseline"]["days"]
        current_hours = self.config["windowing"]["current"]["hours"]
        gap_hours = self.config["windowing"].get("gap_hours", 0)

        # Current window: last N hours
        current_end = reference_time
        current_start = current_end - timedelta(hours=current_hours)
        current_window = TimeWindow(start=current_start, end=current_end)

        # Baseline window: previous M days (before gap)
        baseline_end = current_start - timedelta(hours=gap_hours)
        baseline_start = baseline_end - timedelta(days=baseline_days)
        baseline_window = TimeWindow(start=baseline_start, end=baseline_end)

        return baseline_window, current_window

    def _fetch_data(self, window: TimeWindow) -> List[Dict[str, Any]]:
        """
        Fetch processed events from PostgreSQL for the given time window.

        Args:
            window: Time window to fetch data for

        Returns:
            List of event records as dictionaries
        """
        query = """
        SELECT event_data
        FROM github_events_processed
        WHERE processed_at >= %s AND processed_at < %s
        ORDER BY processed_at
        """

        try:
            conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password
            )

            with conn.cursor() as cursor:
                cursor.execute(query, (window.start, window.end))

                records = []
                for row in cursor.fetchall():
                    event_data = row[0]  # JSONB column
                    records.append(event_data)

            conn.close()
            return records

        except Exception as e:
            logger.error(f"Failed to fetch data from PostgreSQL: {e}")
            raise

    def _build_schema_profile(self, records: List[Dict[str, Any]]) -> SchemaProfile:
        """Build schema profile from records."""
        max_cardinality = self.config["profiling"]["max_cardinality_track"]
        return SchemaProfile.from_records(records, max_cardinality_track=max_cardinality)

    def _build_statistical_profile(self, records: List[Dict[str, Any]]) -> StatisticalProfile:
        """Build statistical profile from records."""
        categorical_fields = self.config["targets"]["distribution_drift"]["categorical_fields"]
        numerical_fields = self.config["targets"]["distribution_drift"]["numerical_fields"]
        max_categories = self.config["profiling"]["categorical_max_categories"]

        return StatisticalProfile.from_records(
            records,
            categorical_fields=categorical_fields,
            numerical_fields=numerical_fields,
            max_categories=max_categories
        )

    def _build_volume_profile(self, records: List[Dict[str, Any]]) -> VolumeProfile:
        """Build volume profile from records."""
        # Use categorical fields as entity dimensions
        entity_fields = self.config["targets"]["distribution_drift"]["categorical_fields"]
        top_n = self.config["targets"]["volume_drift"]["top_n_entities"]

        return VolumeProfile.from_records(
            records,
            entity_fields=entity_fields,
            top_n=top_n
        )

    def _persist_results(self, results: List):
        """Persist drift results to PostgreSQL."""
        batch_size = self.config["database"]["batch_insert_size"]

        try:
            with DriftPostgresWriter() as writer:
                writer.write_results(results, batch_size=batch_size)
            logger.info(f"Persisted {len(results)} drift results to database")
        except Exception as e:
            logger.error(f"Failed to persist drift results: {e}")
            # Don't raise - we still want to return the summary

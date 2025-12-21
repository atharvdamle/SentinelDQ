"""
Volume drift detector - identifies traffic and count anomalies.
"""

from typing import List, Dict, Any
import logging
import math

from drift_engine.models import DriftResult, DriftType, Severity, TimeWindow
from drift_engine.profiles import VolumeProfile

logger = logging.getLogger(__name__)


class VolumeDriftDetector:
    """
    Detects volume-level drift using statistical anomaly detection.

    Uses:
    - Z-score for global volume changes
    - Percentage change for per-entity volumes
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize detector with configuration.

        Args:
            config: Volume drift thresholds from drift_config.yaml
        """
        self.config = config
        self.z_score_info_threshold = config.get(
            "z_score", {}).get("info", 2.0)
        self.z_score_warning_threshold = config.get(
            "z_score", {}).get("warning", 3.0)
        self.percent_info_threshold = config.get(
            "percent_change", {}).get("info", 0.20)
        self.percent_warning_threshold = config.get(
            "percent_change", {}).get("warning", 0.50)

    def detect(
        self,
        baseline_profile: VolumeProfile,
        current_profile: VolumeProfile,
        baseline_window: TimeWindow,
        current_window: TimeWindow,
        baseline_profiles: List[VolumeProfile] = None
    ) -> List[DriftResult]:
        """
        Detect volume drift between two profiles.

        Args:
            baseline_profile: Baseline volume profile
            current_profile: Current volume profile
            baseline_window: Baseline time window
            current_window: Current time window
            baseline_profiles: Historical baseline profiles for std calculation (optional)

        Returns:
            List of DriftResult objects
        """
        results = []

        # 1. Detect global volume drift
        results.extend(self._detect_global_volume_drift(
            baseline_profile,
            current_profile,
            baseline_window,
            current_window,
            baseline_profiles
        ))

        # 2. Detect per-entity volume drift
        results.extend(self._detect_per_entity_volume_drift(
            baseline_profile,
            current_profile,
            baseline_window,
            current_window
        ))

        logger.info(
            f"Volume drift detection complete: {len(results)} drifts detected")
        return results

    def _detect_global_volume_drift(
        self,
        baseline_profile: VolumeProfile,
        current_profile: VolumeProfile,
        baseline_window: TimeWindow,
        current_window: TimeWindow,
        baseline_profiles: List[VolumeProfile] = None
    ) -> List[DriftResult]:
        """Detect drift in global event counts."""
        results = []

        baseline_count = baseline_profile.total_count
        current_count = current_profile.total_count

        # Normalize by window duration (to get events per hour)
        baseline_duration = baseline_window.duration_hours()
        current_duration = current_window.duration_hours()

        if baseline_duration == 0 or current_duration == 0:
            logger.warning(
                "Window duration is zero, skipping global volume drift detection")
            return results

        baseline_rate = baseline_count / baseline_duration
        current_rate = current_count / current_duration

        # Calculate z-score if we have historical data
        if baseline_profiles and len(baseline_profiles) > 1:
            # Calculate mean and std from historical profiles
            historical_rates = []
            for profile in baseline_profiles:
                # Assume each profile represents same duration
                rate = profile.total_count / baseline_duration
                historical_rates.append(rate)

            mean_rate = sum(historical_rates) / len(historical_rates)
            variance = sum((r - mean_rate) **
                           2 for r in historical_rates) / len(historical_rates)
            std_rate = math.sqrt(
                variance) if variance > 0 else baseline_rate * 0.1

            z_score = (current_rate - mean_rate) / \
                std_rate if std_rate > 0 else 0
        else:
            # Fall back to simple comparison with baseline
            std_rate = baseline_rate * 0.1  # Assume 10% std dev
            z_score = (current_rate - baseline_rate) / \
                std_rate if std_rate > 0 else 0

        # Calculate percentage change
        percent_change = (current_rate - baseline_rate) / \
            baseline_rate if baseline_rate > 0 else 0

        # Determine severity based on z-score
        abs_z_score = abs(z_score)

        if abs_z_score >= self.z_score_warning_threshold or abs(percent_change) >= self.percent_warning_threshold:
            severity = Severity.CRITICAL
            drift_score = min(1.0, abs_z_score / 5.0)
        elif abs_z_score >= self.z_score_info_threshold or abs(percent_change) >= self.percent_info_threshold:
            severity = Severity.WARNING
            drift_score = abs_z_score / 5.0
        else:
            return results  # No significant drift

        direction = "increase" if current_rate > baseline_rate else "decrease"

        result = DriftResult(
            drift_type=DriftType.VOLUME,
            entity="global",
            field_name=None,
            baseline_window=baseline_window,
            current_window=current_window,
            metric_name="event_rate_change",
            baseline_value=baseline_rate,
            current_value=current_rate,
            drift_score=drift_score,
            severity=severity,
            metadata={
                "z_score": round(z_score, 2),
                "percent_change": round(percent_change * 100, 2),
                "direction": direction,
                "baseline_total": baseline_count,
                "current_total": current_count,
                "baseline_rate_per_hour": round(baseline_rate, 2),
                "current_rate_per_hour": round(current_rate, 2)
            }
        )
        results.append(result)
        logger.info(
            f"[{severity.value}] Global volume drift: "
            f"{baseline_rate:.1f} -> {current_rate:.1f} events/hour "
            f"(z={z_score:.2f}, {percent_change:+.1%})"
        )

        return results

    def _detect_per_entity_volume_drift(
        self,
        baseline_profile: VolumeProfile,
        current_profile: VolumeProfile,
        baseline_window: TimeWindow,
        current_window: TimeWindow
    ) -> List[DriftResult]:
        """Detect drift in per-entity volumes (e.g., per event type, per repo)."""
        results = []

        # Check each entity dimension
        for entity_field in baseline_profile.per_entity.keys():
            if entity_field not in current_profile.per_entity:
                continue

            baseline_entities = baseline_profile.per_entity[entity_field]
            current_entities = current_profile.per_entity[entity_field]

            # Check entities that exist in both
            common_entities = set(baseline_entities.keys()) & set(
                current_entities.keys())

            for entity_value in common_entities:
                baseline_count = baseline_entities[entity_value]
                current_count = current_entities[entity_value]

                # Calculate percentage change
                if baseline_count > 0:
                    percent_change = (
                        current_count - baseline_count) / baseline_count
                else:
                    percent_change = 1.0 if current_count > 0 else 0.0

                abs_percent_change = abs(percent_change)

                # Determine severity
                if abs_percent_change >= self.percent_warning_threshold:
                    severity = Severity.WARNING
                    drift_score = min(1.0, abs_percent_change)
                elif abs_percent_change >= self.percent_info_threshold:
                    severity = Severity.INFO
                    drift_score = abs_percent_change
                else:
                    continue  # No significant drift

                direction = "increase" if current_count > baseline_count else "decrease"

                result = DriftResult(
                    drift_type=DriftType.VOLUME,
                    entity=entity_field,
                    field_name=entity_value,
                    baseline_window=baseline_window,
                    current_window=current_window,
                    metric_name="entity_count_change",
                    baseline_value=baseline_count,
                    current_value=current_count,
                    drift_score=drift_score,
                    severity=severity,
                    metadata={
                        "percent_change": round(percent_change * 100, 2),
                        "direction": direction,
                        "baseline_count": baseline_count,
                        "current_count": current_count
                    }
                )
                results.append(result)
                logger.info(
                    f"[{severity.value}] Volume drift in {entity_field}='{entity_value}': "
                    f"{baseline_count} -> {current_count} ({percent_change:+.1%})"
                )

        return results

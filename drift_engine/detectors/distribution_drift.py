"""
Distribution drift detector - identifies statistical distribution changes.
"""

from typing import List, Dict, Any, Optional
import logging
import math

from drift_engine.models import DriftResult, DriftType, Severity, TimeWindow
from drift_engine.profiles import StatisticalProfile

logger = logging.getLogger(__name__)


class DistributionDriftDetector:
    """
    Detects distribution-level drift using statistical tests.

    Uses:
    - PSI (Population Stability Index) for categorical distributions
    - KS test for numerical distributions
    - Null ratio changes
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize detector with configuration.

        Args:
            config: Distribution drift thresholds from drift_config.yaml
        """
        self.config = config
        self.psi_info_threshold = config.get("psi", {}).get("info", 0.1)
        self.psi_warning_threshold = config.get("psi", {}).get("warning", 0.25)
        self.ks_info_pvalue = config.get("ks_test", {}).get("info_pvalue", 0.05)
        self.ks_warning_pvalue = config.get("ks_test", {}).get("warning_pvalue", 0.01)
        self.null_warning_threshold = config.get("null_ratio_change", {}).get(
            "warning", 0.1
        )
        self.null_critical_threshold = config.get("null_ratio_change", {}).get(
            "critical", 0.25
        )

    def detect(
        self,
        baseline_profile: StatisticalProfile,
        current_profile: StatisticalProfile,
        baseline_window: TimeWindow,
        current_window: TimeWindow,
    ) -> List[DriftResult]:
        """
        Detect distribution drift between two profiles.

        Returns:
            List of DriftResult objects
        """
        results = []

        # 1. Detect categorical distribution drift (PSI)
        results.extend(
            self._detect_categorical_drift(
                baseline_profile, current_profile, baseline_window, current_window
            )
        )

        # 2. Detect numerical distribution drift (KS test)
        results.extend(
            self._detect_numerical_drift(
                baseline_profile, current_profile, baseline_window, current_window
            )
        )

        # 3. Detect null ratio changes
        results.extend(
            self._detect_null_ratio_drift(
                baseline_profile, current_profile, baseline_window, current_window
            )
        )

        logger.info(
            f"Distribution drift detection complete: {len(results)} drifts detected"
        )
        return results

    def _detect_categorical_drift(
        self,
        baseline_profile: StatisticalProfile,
        current_profile: StatisticalProfile,
        baseline_window: TimeWindow,
        current_window: TimeWindow,
    ) -> List[DriftResult]:
        """Detect drift in categorical field distributions using PSI."""
        results = []

        # Get common categorical fields
        baseline_fields = set(baseline_profile.categorical.keys())
        current_fields = set(current_profile.categorical.keys())
        common_fields = baseline_fields & current_fields

        for field in common_fields:
            baseline_dist = baseline_profile.categorical[field]
            current_dist = current_profile.categorical[field]

            # Calculate PSI
            psi_score = self._calculate_psi(baseline_dist, current_dist)

            # Determine severity
            if psi_score >= self.psi_warning_threshold:
                severity = Severity.CRITICAL
                drift_score = min(1.0, psi_score / 0.5)
            elif psi_score >= self.psi_info_threshold:
                severity = Severity.WARNING
                drift_score = psi_score / 0.5
            else:
                continue  # No significant drift

            result = DriftResult(
                drift_type=DriftType.DISTRIBUTION,
                entity="categorical",
                field_name=field,
                baseline_window=baseline_window,
                current_window=current_window,
                metric_name="psi",
                baseline_value=baseline_dist,
                current_value=current_dist,
                drift_score=drift_score,
                severity=severity,
                metadata={
                    "psi_score": round(psi_score, 4),
                    "baseline_top_values": self._get_top_values(baseline_dist, 5),
                    "current_top_values": self._get_top_values(current_dist, 5),
                },
            )
            results.append(result)
            logger.info(f"[{severity.value}] PSI drift in {field}: {psi_score:.4f}")

        return results

    def _detect_numerical_drift(
        self,
        baseline_profile: StatisticalProfile,
        current_profile: StatisticalProfile,
        baseline_window: TimeWindow,
        current_window: TimeWindow,
    ) -> List[DriftResult]:
        """Detect drift in numerical field distributions using statistical comparisons."""
        results = []

        # Get common numerical fields
        baseline_fields = set(baseline_profile.numerical.keys())
        current_fields = set(current_profile.numerical.keys())
        common_fields = baseline_fields & current_fields

        for field in common_fields:
            baseline_stats = baseline_profile.numerical[field]
            current_stats = current_profile.numerical[field]

            # Compare mean shift (normalized by baseline std)
            baseline_mean = baseline_stats.get("mean", 0)
            baseline_std = baseline_stats.get("std", 1)
            current_mean = current_stats.get("mean", 0)

            if baseline_std > 0:
                mean_shift = abs(current_mean - baseline_mean) / baseline_std
            else:
                mean_shift = 0

            # Determine severity based on mean shift
            if mean_shift >= 3.0:
                severity = Severity.CRITICAL
                drift_score = min(1.0, mean_shift / 5.0)
            elif mean_shift >= 2.0:
                severity = Severity.WARNING
                drift_score = mean_shift / 5.0
            elif mean_shift >= 1.0:
                severity = Severity.INFO
                drift_score = mean_shift / 5.0
            else:
                continue  # No significant drift

            result = DriftResult(
                drift_type=DriftType.DISTRIBUTION,
                entity="numerical",
                field_name=field,
                baseline_window=baseline_window,
                current_window=current_window,
                metric_name="mean_shift",
                baseline_value=baseline_stats,
                current_value=current_stats,
                drift_score=drift_score,
                severity=severity,
                metadata={
                    "mean_shift_std_units": round(mean_shift, 2),
                    "baseline_mean": baseline_mean,
                    "current_mean": current_mean,
                    "baseline_std": baseline_std,
                },
            )
            results.append(result)
            logger.info(
                f"[{severity.value}] Mean shift in {field}: {mean_shift:.2f} std units"
            )

        return results

    def _detect_null_ratio_drift(
        self,
        baseline_profile: StatisticalProfile,
        current_profile: StatisticalProfile,
        baseline_window: TimeWindow,
        current_window: TimeWindow,
    ) -> List[DriftResult]:
        """Detect changes in null ratios."""
        results = []

        # Get common fields with null ratio tracking
        baseline_fields = set(baseline_profile.null_ratios.keys())
        current_fields = set(current_profile.null_ratios.keys())
        common_fields = baseline_fields & current_fields

        for field in common_fields:
            baseline_null_ratio = baseline_profile.null_ratios[field]
            current_null_ratio = current_profile.null_ratios[field]

            # Calculate absolute change
            null_ratio_change = abs(current_null_ratio - baseline_null_ratio)

            # Determine severity
            if null_ratio_change >= self.null_critical_threshold:
                severity = Severity.CRITICAL
                drift_score = min(1.0, null_ratio_change / 0.5)
            elif null_ratio_change >= self.null_warning_threshold:
                severity = Severity.WARNING
                drift_score = null_ratio_change / 0.5
            else:
                continue  # No significant drift

            result = DriftResult(
                drift_type=DriftType.DISTRIBUTION,
                entity="null_ratio",
                field_name=field,
                baseline_window=baseline_window,
                current_window=current_window,
                metric_name="null_ratio_change",
                baseline_value=baseline_null_ratio,
                current_value=current_null_ratio,
                drift_score=drift_score,
                severity=severity,
                metadata={
                    "absolute_change": round(null_ratio_change, 4),
                    "baseline_null_ratio": round(baseline_null_ratio, 4),
                    "current_null_ratio": round(current_null_ratio, 4),
                },
            )
            results.append(result)
            logger.info(
                f"[{severity.value}] Null ratio drift in {field}: "
                f"{baseline_null_ratio:.2%} -> {current_null_ratio:.2%}"
            )

        return results

    @staticmethod
    def _calculate_psi(
        baseline_dist: Dict[str, float], current_dist: Dict[str, float]
    ) -> float:
        """
        Calculate Population Stability Index (PSI).

        PSI = Σ (current% - baseline%) × ln(current% / baseline%)

        Args:
            baseline_dist: Baseline distribution {value: proportion}
            current_dist: Current distribution {value: proportion}

        Returns:
            PSI score (higher = more drift)
        """
        psi = 0.0

        # Get all unique values
        all_values = set(baseline_dist.keys()) | set(current_dist.keys())

        for value in all_values:
            # Use small epsilon to avoid log(0)
            baseline_pct = baseline_dist.get(value, 1e-10)
            current_pct = current_dist.get(value, 1e-10)

            # Ensure minimum proportion to avoid division by zero
            if baseline_pct < 1e-10:
                baseline_pct = 1e-10
            if current_pct < 1e-10:
                current_pct = 1e-10

            psi += (current_pct - baseline_pct) * math.log(current_pct / baseline_pct)

        return abs(psi)

    @staticmethod
    def _get_top_values(distribution: Dict[str, float], n: int = 5) -> Dict[str, float]:
        """Get top N values from distribution."""
        sorted_items = sorted(distribution.items(), key=lambda x: x[1], reverse=True)
        return dict(sorted_items[:n])

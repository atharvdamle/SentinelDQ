"""
Schema drift detector - identifies structural changes in data.
"""

from typing import List, Dict, Any
import logging

from drift_engine.models import DriftResult, DriftType, Severity, TimeWindow
from drift_engine.profiles import SchemaProfile

logger = logging.getLogger(__name__)


class SchemaDriftDetector:
    """
    Detects schema-level drift between baseline and current data.

    Detects:
    - Field additions
    - Field removals
    - Type changes
    - Cardinality explosions
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize detector with configuration.

        Args:
            config: Schema drift thresholds from drift_config.yaml
        """
        self.config = config
        self.cardinality_warning_ratio = config.get("cardinality_warning_ratio", 2.0)
        self.cardinality_critical_ratio = config.get("cardinality_critical_ratio", 5.0)

    def detect(
        self,
        baseline_profile: SchemaProfile,
        current_profile: SchemaProfile,
        baseline_window: TimeWindow,
        current_window: TimeWindow,
    ) -> List[DriftResult]:
        """
        Detect schema drift between two profiles.

        Returns:
            List of DriftResult objects
        """
        results = []

        baseline_fields = set(baseline_profile.get_field_names())
        current_fields = set(current_profile.get_field_names())

        # 1. Detect field additions
        new_fields = current_fields - baseline_fields
        for field in new_fields:
            field_info = current_profile.fields[field]
            nullable = field_info.get("nullable", True)

            severity = Severity.INFO if nullable else Severity.WARNING

            result = DriftResult(
                drift_type=DriftType.SCHEMA,
                entity="schema",
                field_name=field,
                baseline_window=baseline_window,
                current_window=current_window,
                metric_name="field_added",
                baseline_value=None,
                current_value=field_info,
                drift_score=0.5 if nullable else 0.8,
                severity=severity,
                metadata={
                    "change_type": "addition",
                    "nullable": nullable,
                    "type": field_info.get("type"),
                },
            )
            results.append(result)
            logger.info(f"[{severity.value}] New field detected: {field}")

        # 2. Detect field removals
        removed_fields = baseline_fields - current_fields
        for field in removed_fields:
            field_info = baseline_profile.fields[field]

            result = DriftResult(
                drift_type=DriftType.SCHEMA,
                entity="schema",
                field_name=field,
                baseline_window=baseline_window,
                current_window=current_window,
                metric_name="field_removed",
                baseline_value=field_info,
                current_value=None,
                drift_score=1.0,  # Field removal is critical
                severity=Severity.CRITICAL,
                metadata={
                    "change_type": "removal",
                    "baseline_type": field_info.get("type"),
                    "baseline_presence_ratio": field_info.get("presence_ratio"),
                },
            )
            results.append(result)
            logger.warning(f"[CRITICAL] Field removed: {field}")

        # 3. Detect type changes and cardinality explosions (for common fields)
        common_fields = baseline_fields & current_fields
        for field in common_fields:
            baseline_info = baseline_profile.fields[field]
            current_info = current_profile.fields[field]

            # Check type change
            baseline_type = baseline_info.get("type")
            current_type = current_info.get("type")

            if baseline_type != current_type:
                result = DriftResult(
                    drift_type=DriftType.SCHEMA,
                    entity="schema",
                    field_name=field,
                    baseline_window=baseline_window,
                    current_window=current_window,
                    metric_name="type_change",
                    baseline_value=baseline_type,
                    current_value=current_type,
                    drift_score=1.0,
                    severity=Severity.CRITICAL,
                    metadata={
                        "change_type": "type_mutation",
                        "from_type": baseline_type,
                        "to_type": current_type,
                    },
                )
                results.append(result)
                logger.warning(
                    f"[CRITICAL] Type change in {field}: {baseline_type} -> {current_type}"
                )

            # Check cardinality explosion
            baseline_card = baseline_info.get("cardinality", 0)
            current_card = current_info.get("cardinality", 0)

            if baseline_card > 0 and current_card > 0:
                card_ratio = current_card / baseline_card

                if card_ratio >= self.cardinality_critical_ratio:
                    severity = Severity.CRITICAL
                    drift_score = min(1.0, card_ratio / 10)
                elif card_ratio >= self.cardinality_warning_ratio:
                    severity = Severity.WARNING
                    drift_score = card_ratio / 10
                else:
                    continue  # No significant cardinality change

                result = DriftResult(
                    drift_type=DriftType.SCHEMA,
                    entity="schema",
                    field_name=field,
                    baseline_window=baseline_window,
                    current_window=current_window,
                    metric_name="cardinality_explosion",
                    baseline_value=baseline_card,
                    current_value=current_card,
                    drift_score=drift_score,
                    severity=severity,
                    metadata={
                        "change_type": "cardinality_increase",
                        "ratio": round(card_ratio, 2),
                        "baseline_cardinality": baseline_card,
                        "current_cardinality": current_card,
                    },
                )
                results.append(result)
                logger.info(
                    f"[{severity.value}] Cardinality explosion in {field}: "
                    f"{baseline_card} -> {current_card} ({card_ratio:.1f}x)"
                )

        logger.info(f"Schema drift detection complete: {len(results)} drifts detected")
        return results

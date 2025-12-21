"""
Statistical profiler - extracts distribution and statistical metadata from data.
"""

from typing import Dict, Any, List, Optional
from collections import Counter
import logging
import math

logger = logging.getLogger(__name__)


class StatisticalProfile:
    """
    Represents statistical characteristics of a dataset.

    Tracks:
    - Categorical field distributions
    - Numerical field statistics (mean, std, percentiles)
    - Null ratios
    """

    def __init__(self):
        # field -> {value: proportion}
        self.categorical: Dict[str, Dict[str, float]] = {}
        # field -> {mean, std, min, max, ...}
        self.numerical: Dict[str, Dict[str, float]] = {}
        self.null_ratios: Dict[str, float] = {}
        self.row_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "categorical": self.categorical,
            "numerical": self.numerical,
            "null_ratios": self.null_ratios,
            "row_count": self.row_count
        }

    @classmethod
    def from_records(
        cls,
        records: List[Dict[str, Any]],
        categorical_fields: List[str],
        numerical_fields: List[str],
        max_categories: int = 100
    ) -> 'StatisticalProfile':
        """
        Build statistical profile from records.

        Args:
            records: List of dictionaries
            categorical_fields: Fields to treat as categorical
            numerical_fields: Fields to treat as numerical
            max_categories: Max unique values to track for categorical fields

        Returns:
            StatisticalProfile instance
        """
        profile = cls()
        profile.row_count = len(records)

        if not records:
            return profile

        # Profile categorical fields
        for field in categorical_fields:
            values = []
            null_count = 0

            for record in records:
                value = cls._extract_field(record, field)
                if value is None:
                    null_count += 1
                else:
                    values.append(str(value))

            # Calculate null ratio
            profile.null_ratios[field] = null_count / \
                profile.row_count if profile.row_count > 0 else 0

            # Calculate distribution
            if values:
                counter = Counter(values)
                total = len(values)

                # Only track if cardinality is reasonable
                if len(counter) <= max_categories:
                    distribution = {
                        value: count / total
                        for value, count in counter.most_common()
                    }
                    profile.categorical[field] = distribution
                else:
                    logger.warning(
                        f"Field '{field}' has {len(counter)} unique values, "
                        f"exceeding max_categories={max_categories}. Skipping distribution."
                    )

        # Profile numerical fields
        for field in numerical_fields:
            values = []
            null_count = 0

            for record in records:
                value = cls._extract_field(record, field)
                if value is None:
                    null_count += 1
                else:
                    try:
                        values.append(float(value))
                    except (ValueError, TypeError):
                        # Not a valid number
                        null_count += 1

            # Calculate null ratio
            profile.null_ratios[field] = null_count / \
                profile.row_count if profile.row_count > 0 else 0

            # Calculate statistics
            if values:
                sorted_values = sorted(values)
                n = len(sorted_values)

                mean_val = sum(values) / n
                variance = sum((x - mean_val) ** 2 for x in values) / n
                std_val = math.sqrt(variance)

                profile.numerical[field] = {
                    "mean": round(mean_val, 4),
                    "std": round(std_val, 4),
                    "min": sorted_values[0],
                    "max": sorted_values[-1],
                    "p50": cls._percentile(sorted_values, 50),
                    "p95": cls._percentile(sorted_values, 95),
                    "p99": cls._percentile(sorted_values, 99)
                }

        logger.info(
            f"Built statistical profile: "
            f"{len(profile.categorical)} categorical, "
            f"{len(profile.numerical)} numerical fields"
        )
        return profile

    @staticmethod
    def _extract_field(record: Dict[str, Any], field_path: str) -> Any:
        """
        Extract field value from nested record using dot notation.

        Example:
            record = {"actor": {"login": "user1"}}
            _extract_field(record, "actor.login") -> "user1"
        """
        parts = field_path.split('.')
        value = record

        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None

            if value is None:
                return None

        return value

    @staticmethod
    def _percentile(sorted_values: List[float], percentile: int) -> float:
        """Calculate percentile from sorted values."""
        if not sorted_values:
            return 0.0

        n = len(sorted_values)
        index = (percentile / 100) * (n - 1)

        if index.is_integer():
            return sorted_values[int(index)]
        else:
            lower = sorted_values[int(index)]
            upper = sorted_values[int(index) + 1]
            fraction = index - int(index)
            return lower + (upper - lower) * fraction

    def get_distribution(self, field: str) -> Optional[Dict[str, float]]:
        """Get categorical distribution for a field."""
        return self.categorical.get(field)

    def get_statistics(self, field: str) -> Optional[Dict[str, float]]:
        """Get numerical statistics for a field."""
        return self.numerical.get(field)

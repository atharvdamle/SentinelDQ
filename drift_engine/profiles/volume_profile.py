"""
Volume profiler - extracts count and volume metadata from data.
"""

from typing import Dict, Any, List, Optional
from collections import Counter
import logging

logger = logging.getLogger(__name__)


class VolumeProfile:
    """
    Represents volume characteristics of a dataset.

    Tracks:
    - Total row count
    - Counts per entity (e.g., per event type, per repo)
    - Time-based volume patterns (if timestamp available)
    """

    def __init__(self):
        self.total_count: int = 0
        # entity_type -> {entity_value: count}
        self.per_entity: Dict[str, Dict[str, int]] = {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_count": self.total_count,
            "per_entity": self.per_entity
        }

    @classmethod
    def from_records(
        cls,
        records: List[Dict[str, Any]],
        entity_fields: List[str],
        top_n: int = 50
    ) -> 'VolumeProfile':
        """
        Build volume profile from records.

        Args:
            records: List of dictionaries
            entity_fields: Fields to aggregate counts by (e.g., ['type', 'repo.name'])
            top_n: Keep only top N entities per field

        Returns:
            VolumeProfile instance
        """
        profile = cls()
        profile.total_count = len(records)

        if not records:
            return profile

        # Count per entity
        for field in entity_fields:
            values = []

            for record in records:
                value = cls._extract_field(record, field)
                if value is not None:
                    values.append(str(value))

            if values:
                counter = Counter(values)
                # Keep only top N
                top_entities = dict(counter.most_common(top_n))
                profile.per_entity[field] = top_entities

        logger.info(
            f"Built volume profile: {profile.total_count} total records, "
            f"{len(profile.per_entity)} entity dimensions"
        )
        return profile

    @staticmethod
    def _extract_field(record: Dict[str, Any], field_path: str) -> Any:
        """Extract field value from nested record using dot notation."""
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

    def get_entity_count(self, entity_field: str, entity_value: str) -> int:
        """Get count for a specific entity value."""
        return self.per_entity.get(entity_field, {}).get(entity_value, 0)

    def get_top_entities(self, entity_field: str, n: int = 10) -> Dict[str, int]:
        """Get top N entities for a field."""
        entities = self.per_entity.get(entity_field, {})
        sorted_entities = sorted(
            entities.items(), key=lambda x: x[1], reverse=True)
        return dict(sorted_entities[:n])

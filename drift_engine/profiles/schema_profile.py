"""
Schema profiler - extracts structural metadata from data.
"""

from typing import Dict, Any, List, Optional
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class SchemaProfile:
    """
    Represents the schema profile of a dataset.

    Tracks:
    - Field names and paths (including nested fields)
    - Data types
    - Nullability
    - Cardinality (unique value counts)
    - Presence counts
    """

    def __init__(self):
        self.fields: Dict[str, Dict[str, Any]] = {}
        self.row_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "fields": self.fields,
            "row_count": self.row_count
        }

    @classmethod
    def from_records(cls, records: List[Dict[str, Any]], max_cardinality_track: int = 1000) -> 'SchemaProfile':
        """
        Build a schema profile from a list of records.

        Args:
            records: List of dictionaries (JSON-like records)
            max_cardinality_track: Don't track unique values if cardinality exceeds this

        Returns:
            SchemaProfile instance
        """
        profile = cls()
        profile.row_count = len(records)

        if not records:
            return profile

        # Track field metadata
        field_stats = defaultdict(lambda: {
            "types": defaultdict(int),
            "null_count": 0,
            "present_count": 0,
            "unique_values": set()
        })

        for record in records:
            # Flatten nested structure
            flat = cls._flatten_dict(record)

            # Track all possible fields from all records
            for field_path, value in flat.items():
                stats = field_stats[field_path]
                stats["present_count"] += 1

                if value is None:
                    stats["null_count"] += 1
                else:
                    # Track type
                    value_type = type(value).__name__
                    stats["types"][value_type] += 1

                    # Track cardinality (up to limit)
                    if len(stats["unique_values"]) < max_cardinality_track:
                        stats["unique_values"].add(str(value))

        # Build final field profiles
        for field_path, stats in field_stats.items():
            # Determine dominant type
            dominant_type = max(stats["types"].items(), key=lambda x: x[1])[
                0] if stats["types"] else "null"

            # Calculate nullability
            nullable = stats["null_count"] > 0
            null_ratio = stats["null_count"] / \
                profile.row_count if profile.row_count > 0 else 0

            # Cardinality
            cardinality = len(stats["unique_values"])
            cardinality_exact = cardinality < max_cardinality_track

            profile.fields[field_path] = {
                "type": dominant_type,
                "nullable": nullable,
                "null_ratio": round(null_ratio, 4),
                "present_count": stats["present_count"],
                "presence_ratio": round(stats["present_count"] / profile.row_count, 4),
                "cardinality": cardinality,
                "cardinality_exact": cardinality_exact
            }

        logger.info(
            f"Built schema profile: {len(profile.fields)} fields, {profile.row_count} rows")
        return profile

    @staticmethod
    def _flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '.') -> Dict[str, Any]:
        """
        Flatten nested dictionary.

        Example:
            {"a": {"b": 1}} -> {"a.b": 1}
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            if isinstance(v, dict):
                items.extend(SchemaProfile._flatten_dict(
                    v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # For arrays, just track the field existence, not individual elements
                items.append((new_key, f"<array[{len(v)}]>"))
            else:
                items.append((new_key, v))

        return dict(items)

    def get_field_names(self) -> List[str]:
        """Get list of all field names."""
        return list(self.fields.keys())

    def get_field_type(self, field_name: str) -> Optional[str]:
        """Get type of a specific field."""
        return self.fields.get(field_name, {}).get("type")

    def get_cardinality(self, field_name: str) -> Optional[int]:
        """Get cardinality of a specific field."""
        return self.fields.get(field_name, {}).get("cardinality")

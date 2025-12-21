"""
Example: Simulate drift scenarios for testing.

This script demonstrates how to create synthetic drift scenarios
to validate the drift detection engine.
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any
import random
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_baseline_events(count: int = 1000) -> List[Dict[str, Any]]:
    """
    Generate baseline (normal) GitHub events.

    Characteristics:
    - PushEvent: 45%
    - CreateEvent: 20%
    - WatchEvent: 15%
    - IssuesEvent: 10%
    - PullRequestEvent: 10%
    - Payload sizes: mean=1200, std=300
    - Consistent null ratios
    """
    events = []
    event_types = [
        ("PushEvent", 0.45),
        ("CreateEvent", 0.20),
        ("WatchEvent", 0.15),
        ("IssuesEvent", 0.10),
        ("PullRequestEvent", 0.10)
    ]

    repos = [f"user{i % 50}/repo{i % 100}" for i in range(200)]
    actors = [f"user{i}" for i in range(100)]

    for i in range(count):
        # Select event type based on distribution
        rand = random.random()
        cumulative = 0
        selected_type = "PushEvent"
        for event_type, prob in event_types:
            cumulative += prob
            if rand < cumulative:
                selected_type = event_type
                break

        # Generate event
        event = {
            "id": str(i),
            "type": selected_type,
            "actor": {
                "login": random.choice(actors)
            },
            "repo": {
                "name": random.choice(repos)
            },
            "payload": {
                "size": max(0, int(random.gauss(1200, 300)))
            },
            "created_at": (datetime.utcnow() - timedelta(days=random.randint(1, 7))).isoformat()
        }

        # Random nulls (5% chance for payload.size)
        if random.random() < 0.05:
            event["payload"]["size"] = None

        events.append(event)

    logger.info(f"Generated {len(events)} baseline events")
    return events


def generate_schema_drift_events(count: int = 200) -> List[Dict[str, Any]]:
    """
    Generate events with SCHEMA DRIFT.

    Drift scenarios:
    1. New field added: payload.security_advisory
    2. Field removed: payload.size (missing in 50% of events)
    3. Type change: id becomes integer instead of string
    4. Cardinality explosion: 3x more unique repos
    """
    events = []
    event_types = ["PushEvent", "CreateEvent",
                   "SecurityAdvisoryEvent"]  # New type!

    # 3x more repos (cardinality explosion)
    repos = [f"user{i % 150}/repo{i % 300}" for i in range(600)]
    actors = [f"user{i}" for i in range(100)]

    for i in range(count):
        event = {
            "id": i,  # Type change: integer instead of string!
            "type": random.choice(event_types),
            "actor": {
                "login": random.choice(actors)
            },
            "repo": {
                "name": random.choice(repos)
            },
            "payload": {},
            "created_at": datetime.utcnow().isoformat()
        }

        # 50% missing payload.size (field removal)
        if random.random() > 0.5:
            event["payload"]["size"] = int(random.gauss(1200, 300))

        # New field: security_advisory
        if event["type"] == "SecurityAdvisoryEvent":
            event["payload"]["security_advisory"] = {
                "severity": random.choice(["low", "medium", "high", "critical"])
            }

        events.append(event)

    logger.info(f"Generated {len(events)} events with SCHEMA DRIFT")
    return events


def generate_distribution_drift_events(count: int = 200) -> List[Dict[str, Any]]:
    """
    Generate events with DISTRIBUTION DRIFT.

    Drift scenarios:
    1. Event type distribution shifted (PushEvent: 45% → 65%)
    2. Payload size mean shifted (1200 → 2500)
    3. Null ratio increased (5% → 30%)
    """
    events = []

    # Shifted distribution (PushEvent now dominant)
    event_types = [
        ("PushEvent", 0.65),      # Was 0.45 (+44% increase!)
        ("CreateEvent", 0.15),    # Was 0.20
        ("WatchEvent", 0.10),     # Was 0.15
        ("IssuesEvent", 0.05),    # Was 0.10
        ("PullRequestEvent", 0.05)  # Was 0.10
    ]

    repos = [f"user{i % 50}/repo{i % 100}" for i in range(200)]
    actors = [f"user{i}" for i in range(100)]

    for i in range(count):
        rand = random.random()
        cumulative = 0
        selected_type = "PushEvent"
        for event_type, prob in event_types:
            cumulative += prob
            if rand < cumulative:
                selected_type = event_type
                break

        event = {
            "id": str(i),
            "type": selected_type,
            "actor": {
                "login": random.choice(actors)
            },
            "repo": {
                "name": random.choice(repos)
            },
            "payload": {
                # Mean shifted up!
                "size": max(0, int(random.gauss(2500, 400)))
            },
            "created_at": datetime.utcnow().isoformat()
        }

        # Increased null ratio (30% instead of 5%)
        if random.random() < 0.30:
            event["payload"]["size"] = None

        events.append(event)

    logger.info(f"Generated {len(events)} events with DISTRIBUTION DRIFT")
    return events


def generate_volume_drift_events(count: int = 3000) -> List[Dict[str, Any]]:
    """
    Generate events with VOLUME DRIFT.

    Drift scenarios:
    1. 3x volume spike (1000 → 3000 events in 24h)
    2. One repo has sudden spike (50 → 500 events)
    """
    events = []
    event_types = ["PushEvent", "CreateEvent",
                   "WatchEvent", "IssuesEvent", "PullRequestEvent"]

    repos = [f"user{i % 50}/repo{i % 100}" for i in range(200)]
    actors = [f"user{i}" for i in range(100)]

    # Special repo with spike
    spike_repo = "viral-user/trending-repo"

    for i in range(count):
        # 1 in 6 events from the spiking repo
        if i % 6 == 0:
            repo = spike_repo
        else:
            repo = random.choice(repos)

        event = {
            "id": str(i),
            "type": random.choice(event_types),
            "actor": {
                "login": random.choice(actors)
            },
            "repo": {
                "name": repo
            },
            "payload": {
                "size": max(0, int(random.gauss(1200, 300)))
            },
            "created_at": datetime.utcnow().isoformat()
        }

        if random.random() < 0.05:
            event["payload"]["size"] = None

        events.append(event)

    logger.info(f"Generated {len(events)} events with VOLUME DRIFT (3x spike)")
    return events


def demonstrate_drift_detection():
    """
    Demonstrate drift detection with synthetic data.

    NOTE: This is a simulation. In production, data comes from PostgreSQL.
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))

    from drift_engine.profiles import SchemaProfile, StatisticalProfile, VolumeProfile
    from drift_engine.detectors import SchemaDriftDetector, DistributionDriftDetector, VolumeDriftDetector
    from drift_engine.models import TimeWindow

    # Define windows
    baseline_window = TimeWindow(
        start=datetime.utcnow() - timedelta(days=8),
        end=datetime.utcnow() - timedelta(days=1)
    )
    current_window = TimeWindow(
        start=datetime.utcnow() - timedelta(hours=24),
        end=datetime.utcnow()
    )

    print("\n" + "=" * 80)
    print("DRIFT DETECTION DEMONSTRATION")
    print("=" * 80)

    # Scenario 1: Schema Drift
    print("\n" + "-" * 80)
    print("SCENARIO 1: SCHEMA DRIFT")
    print("-" * 80)

    baseline_data = generate_baseline_events(1000)
    drift_data = generate_schema_drift_events(200)

    baseline_profile = SchemaProfile.from_records(baseline_data)
    current_profile = SchemaProfile.from_records(drift_data)

    detector = SchemaDriftDetector(
        {"cardinality_warning_ratio": 2.0, "cardinality_critical_ratio": 5.0})
    drifts = detector.detect(
        baseline_profile, current_profile, baseline_window, current_window)

    print(f"\nDetected {len(drifts)} schema drifts:")
    for drift in drifts:
        print(f"  [{drift.severity.value}] {drift.metric_name}: {drift.field_name}")
        print(f"    Score: {drift.drift_score:.3f}")
        print(f"    Details: {drift.metadata}")

    # Scenario 2: Distribution Drift
    print("\n" + "-" * 80)
    print("SCENARIO 2: DISTRIBUTION DRIFT")
    print("-" * 80)

    baseline_data = generate_baseline_events(1000)
    drift_data = generate_distribution_drift_events(200)

    baseline_profile = StatisticalProfile.from_records(
        baseline_data,
        categorical_fields=["type"],
        numerical_fields=["payload.size"],
        max_categories=100
    )
    current_profile = StatisticalProfile.from_records(
        drift_data,
        categorical_fields=["type"],
        numerical_fields=["payload.size"],
        max_categories=100
    )

    detector = DistributionDriftDetector({
        "psi": {"info": 0.1, "warning": 0.25},
        "ks_test": {"info_pvalue": 0.05, "warning_pvalue": 0.01},
        "null_ratio_change": {"warning": 0.1, "critical": 0.25}
    })
    drifts = detector.detect(
        baseline_profile, current_profile, baseline_window, current_window)

    print(f"\nDetected {len(drifts)} distribution drifts:")
    for drift in drifts:
        print(f"  [{drift.severity.value}] {drift.metric_name}: {drift.field_name}")
        print(f"    Score: {drift.drift_score:.3f}")
        if "psi_score" in drift.metadata:
            print(f"    PSI: {drift.metadata['psi_score']:.4f}")
        if "mean_shift_std_units" in drift.metadata:
            print(
                f"    Mean shift: {drift.metadata['mean_shift_std_units']:.2f} std units")

    # Scenario 3: Volume Drift
    print("\n" + "-" * 80)
    print("SCENARIO 3: VOLUME DRIFT")
    print("-" * 80)

    baseline_data = generate_baseline_events(1000)
    drift_data = generate_volume_drift_events(3000)

    baseline_profile = VolumeProfile.from_records(
        baseline_data,
        entity_fields=["type", "repo.name"],
        top_n=50
    )
    current_profile = VolumeProfile.from_records(
        drift_data,
        entity_fields=["type", "repo.name"],
        top_n=50
    )

    detector = VolumeDriftDetector({
        "z_score": {"info": 2.0, "warning": 3.0},
        "percent_change": {"info": 0.20, "warning": 0.50}
    })
    drifts = detector.detect(
        baseline_profile, current_profile, baseline_window, current_window)

    print(f"\nDetected {len(drifts)} volume drifts:")
    for drift in drifts[:5]:  # Show first 5
        print(
            f"  [{drift.severity.value}] {drift.entity}.{drift.field_name or 'global'}")
        print(f"    Score: {drift.drift_score:.3f}")
        if "z_score" in drift.metadata:
            print(f"    Z-score: {drift.metadata['z_score']:.2f}")
        if "percent_change" in drift.metadata:
            print(f"    Change: {drift.metadata['percent_change']:+.1f}%")

    print("\n" + "=" * 80)
    print("DEMONSTRATION COMPLETE")
    print("=" * 80)
    print("\nKey Takeaways:")
    print("  1. Schema drift catches structural changes (new fields, type changes)")
    print("  2. Distribution drift catches behavior changes (PSI, mean shifts)")
    print("  3. Volume drift catches traffic anomalies (spikes, drops)")
    print("  4. All drift is classified by severity (INFO/WARNING/CRITICAL)")
    print("  5. Drift scores are normalized (0-1) for consistent interpretation")


if __name__ == "__main__":
    demonstrate_drift_detection()

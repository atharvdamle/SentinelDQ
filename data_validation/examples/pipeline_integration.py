"""
Integration Example: Data Validation in Ingestion Pipeline

This demonstrates how validation integrates with the existing ingestion system.
"""

import json
from data_validation import DataValidator, ValidationStatus
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def simulate_ingestion_pipeline():
    """
    Simulate the full ingestion pipeline with validation.

    Flow:
    1. Event consumed from Kafka
    2. Stored in github_events_raw (JSONB)
    3. VALIDATION LAYER ← NEW
    4. If PASS/WARN: Insert into github_events_processed
    5. If FAIL: Log and skip
    """

    print("="*70)
    print("SIMULATED INGESTION PIPELINE WITH VALIDATION")
    print("="*70)

    # Initialize validator (would be done once at startup)
    validator = DataValidator(enable_persistence=False, enable_metrics=True)

    # Simulate events from Kafka
    kafka_events = [
        # Event 1: Valid
        {
            "id": "event_001",
            "type": "PushEvent",
            "actor": {"id": 100, "login": "alice", "url": "https://api.github.com/users/alice"},
            "repo": {"id": 200, "name": "alice/repo1", "url": "https://api.github.com/repos/alice/repo1"},
            "payload": {"ref": "refs/heads/main", "commits": []},
            "public": True,
            "created_at": "2025-12-19T10:00:00Z"
        },
        # Event 2: Warning (unknown type)
        {
            "id": "event_002",
            "type": "CustomEvent",  # Not in enum
            "actor": {"id": 101, "login": "bob", "url": "https://api.github.com/users/bob"},
            "repo": {"id": 201, "name": "bob/repo2", "url": "https://api.github.com/repos/bob/repo2"},
            "payload": {},
            "public": True,
            "created_at": "2025-12-19T10:01:00Z"
        },
        # Event 3: Failure (missing required field)
        {
            "id": "event_003",
            "type": "IssuesEvent",
            "actor": {"id": 102, "login": "charlie"},  # Missing url
            "repo": {"id": 202, "name": "charlie/repo3", "url": "https://api.github.com/repos/charlie/repo3"},
            # Missing created_at (CRITICAL)
            "public": True
        },
        # Event 4: Valid
        {
            "id": "event_004",
            "type": "WatchEvent",
            "actor": {"id": 103, "login": "diana", "url": "https://api.github.com/users/diana"},
            "repo": {"id": 203, "name": "diana/repo4", "url": "https://api.github.com/repos/diana/repo4"},
            "payload": {},
            "public": True,
            "created_at": "2025-12-19T10:03:00Z"
        }
    ]

    # Statistics
    stats = {
        'consumed': 0,
        'stored_raw': 0,
        'validated': 0,
        'processed': 0,
        'failed': 0,
        'warned': 0
    }

    # Process each event
    for i, event in enumerate(kafka_events, 1):
        print(f"\n{'-'*70}")
        print(f"Processing Event {i}/{len(kafka_events)}: {event.get('id')}")
        print(f"{'-'*70}")

        # Step 1: Consume from Kafka
        stats['consumed'] += 1
        print(f"✓ Consumed from Kafka topic 'github-events'")

        # Step 2: Store in github_events_raw
        # INSERT INTO github_events_raw (event_id, payload, ingestion_ts)
        # VALUES (event['id'], event, NOW())
        stats['stored_raw'] += 1
        print(f"✓ Stored in github_events_raw (JSONB)")

        # Step 3: VALIDATE (NEW LAYER)
        print(f"\n[VALIDATION LAYER]")
        result = validator.validate_event(event)
        stats['validated'] += 1

        print(f"  Status: {result.status.value}")
        print(f"  Failures: {len(result.failures)}")
        print(f"  Processing Time: {result.processing_time_ms:.2f}ms")

        if result.failures:
            print(f"\n  Top Failures:")
            for failure in result.failures[:3]:  # Show first 3
                print(
                    f"    - [{failure.severity.value}] {failure.error_message}")

        # Step 4: Decision based on validation result
        if result.status == ValidationStatus.FAIL:
            print(f"\n✗ REJECTED: Not inserted into github_events_processed")
            print(f"  Reason: Critical validation failures")
            stats['failed'] += 1

            # In production, you might:
            # - Log to dead letter queue
            # - Send alert for investigation
            # - Store in failed_events table

        elif result.status == ValidationStatus.WARN:
            print(f"\n⚠ PROCESSED WITH WARNINGS")
            print(f"  Action: Inserted into github_events_processed")
            print(f"  Marked: validation_warnings = true")
            stats['warned'] += 1
            stats['processed'] += 1

            # INSERT INTO github_events_processed (..., has_warnings)
            # VALUES (..., true)

        else:  # PASS
            print(f"\n✓ PROCESSED SUCCESSFULLY")
            print(f"  Action: Inserted into github_events_processed")
            stats['processed'] += 1

            # INSERT INTO github_events_processed (...)
            # VALUES (...)

        # Step 5: Validation result stored
        # INSERT INTO validation_results (event_id, status, ...)
        # VALUES (result.event_id, result.status, ...)
        print(f"✓ Validation result logged to validation_results table")

    # Final Summary
    print(f"\n{'='*70}")
    print("PIPELINE SUMMARY")
    print(f"{'='*70}")
    print(f"\nEvents Consumed from Kafka: {stats['consumed']}")
    print(f"Events Stored in Raw Table: {stats['stored_raw']}")
    print(f"Events Validated: {stats['validated']}")
    print(f"\nValidation Results:")
    print(
        f"  ✓ Processed Successfully: {stats['processed'] - stats['warned']}")
    print(f"  ⚠ Processed with Warnings: {stats['warned']}")
    print(f"  ✗ Rejected (Failed): {stats['failed']}")
    print(f"\nProcessed Table Inserts: {stats['processed']}")
    print(
        f"Processing Rate: {stats['processed'] / stats['consumed'] * 100:.1f}%")

    # Show metrics
    print(f"\n{'='*70}")
    print("PROMETHEUS METRICS SNAPSHOT")
    print(f"{'='*70}")

    from data_validation.metrics import get_metrics
    metrics = get_metrics()

    print(f"\nsentineldq_validation_total: {metrics.total_validations}")
    print(f"sentineldq_validation_passed: {metrics.passed_validations}")
    print(f"sentineldq_validation_warned: {metrics.warned_validations}")
    print(f"sentineldq_validation_failed: {metrics.failed_validations}")

    avg_time = (metrics.duration_sum / metrics.duration_count *
                1000) if metrics.duration_count > 0 else 0
    print(f"\nAverage Validation Time: {avg_time:.2f}ms")

    print(f"\nAlert Conditions:")
    alerts = metrics.get_alert_conditions()
    for alert, triggered in alerts.items():
        status = "🔴 TRIGGERED" if triggered and alert != 'no_data' else "✓ OK"
        print(f"  {alert}: {status}")

    print(f"\n{'='*70}")
    print("Pipeline simulation complete!")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    simulate_ingestion_pipeline()

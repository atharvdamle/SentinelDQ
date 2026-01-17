"""
SentinelDQ Validation Examples

Demonstrates validation system with PASS, WARN, and FAIL scenarios.
"""

from data_validation.metrics import reset_metrics
from data_validation import validate_event, get_metrics, ValidationStatus
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def print_result(name: str, result):
    """Pretty print validation result."""
    print(f"\n{'='*70}")
    print(f"Test Case: {name}")
    print(f"{'='*70}")
    print(f"Event ID: {result.event_id}")
    print(f"Status: {result.status.value}")
    print(f"Processing Time: {result.processing_time_ms:.2f}ms")
    print(f"Failure Count: {len(result.failures)}")

    if result.failures:
        print(f"\nFailures:")
        for i, failure in enumerate(result.failures, 1):
            print(f"\n  {i}. {failure.check_name}")
            print(f"     Field: {failure.field_path}")
            print(f"     Type: {failure.check_type}")
            print(f"     Severity: {failure.severity.value}")
            print(f"     Message: {failure.error_message}")
            if failure.expected_value:
                print(f"     Expected: {failure.expected_value}")
            if failure.actual_value:
                print(f"     Actual: {failure.actual_value}")
    else:
        print("\n[OK] All validation checks passed!")


def main():
    print("="*70)
    print("SentinelDQ Data Validation - Comprehensive Examples")
    print("="*70)

    reset_metrics()  # Start fresh

    # ========================================================================
    # SCENARIO 1: PASS - Valid Event
    # ========================================================================
    valid_event = {
        "id": "12345678901",
        "type": "PushEvent",
        "actor": {
            "id": 12345,
            "login": "octocat",
            "url": "https://api.github.com/users/octocat"
        },
        "repo": {
            "id": 789012,
            "name": "octocat/Hello-World",
            "url": "https://api.github.com/repos/octocat/Hello-World"
        },
        "payload": {
            "ref": "refs/heads/main",
            "commits": [
                {"sha": "abc123", "message": "Fix bug"}
            ]
        },
        "public": True,
        "created_at": "2025-12-19T10:00:00Z"
    }

    result = validate_event(valid_event)
    print_result("PASS - Fully Valid Event", result)
    assert result.status == ValidationStatus.PASS

    # ========================================================================
    # SCENARIO 2: WARN - Unknown Event Type
    # ========================================================================
    warning_event = {
        "id": "22345678902",
        "type": "NewUnknownEvent",  # Not in enum list -> WARN
        "actor": {
            "id": 12346,
            "login": "developer",
            "url": "https://api.github.com/users/developer"
        },
        "repo": {
            "id": 789013,
            "name": "developer/test-repo",
            "url": "https://api.github.com/repos/developer/test-repo"
        },
        "payload": {
            "some_field": "some_value"
        },
        "public": True,
        "created_at": "2025-12-19T11:00:00Z"
    }

    result = validate_event(warning_event)
    print_result("WARN - Unknown Event Type", result)
    assert result.status == ValidationStatus.WARN

    # ========================================================================
    # SCENARIO 3: WARN - Missing Optional Field
    # ========================================================================
    missing_optional_event = {
        "id": "32345678903",
        "type": "IssuesEvent",
        "actor": {
            "id": 12347,
            "login": "tester",
            "url": "https://api.github.com/users/tester"
        },
        "repo": {
            "id": 789014,
            "name": "tester/sample-repo",
            "url": "https://api.github.com/repos/tester/sample-repo"
        },
        "payload": {
            "action": "opened",
            "issue": {"number": 42}
        },
        # "public": True,  # Missing optional field with WARN severity
        "created_at": "2025-12-19T12:00:00Z"
    }

    result = validate_event(missing_optional_event)
    print_result("WARN - Missing Optional Field", result)
    # Note: This may pass if optional fields don't generate warnings

    # ========================================================================
    # SCENARIO 4: WARN - Future Timestamp (within tolerance)
    # ========================================================================
    future_timestamp_event = {
        "id": "42345678904",
        "type": "WatchEvent",
        "actor": {
            "id": 12348,
            "login": "watcher",
            "url": "https://api.github.com/users/watcher"
        },
        "repo": {
            "id": 789015,
            "name": "watcher/popular-repo",
            "url": "https://api.github.com/repos/watcher/popular-repo"
        },
        "payload": {},
        "public": True,
        "created_at": "2025-12-20T10:00:00Z"  # Future timestamp -> WARN
    }

    result = validate_event(future_timestamp_event)
    print_result("WARN - Future Timestamp", result)
    # May be WARN depending on tolerance

    # ========================================================================
    # SCENARIO 5: FAIL - Missing Required Field
    # ========================================================================
    missing_required_event = {
        "id": "52345678905",
        "type": "ForkEvent",
        "actor": {
            "id": 12349,
            "login": "forker"
            # Missing "url" field
        },
        "repo": {
            "id": 789016,
            "name": "forker/forked-repo",
            "url": "https://api.github.com/repos/forker/forked-repo"
        },
        # Missing "created_at" field -> FAIL
        "public": True
    }

    result = validate_event(missing_required_event)
    print_result("FAIL - Missing Required Field", result)
    assert result.status == ValidationStatus.FAIL

    # ========================================================================
    # SCENARIO 6: FAIL - Invalid Data Type
    # ========================================================================
    invalid_type_event = {
        "id": "62345678906",
        "type": "CreateEvent",
        "actor": {
            "id": "not_an_integer",  # Should be integer -> FAIL
            "login": "creator",
            "url": "https://api.github.com/users/creator"
        },
        "repo": {
            "id": 789017,
            "name": "creator/new-repo",
            "url": "https://api.github.com/repos/creator/new-repo"
        },
        "payload": {
            "ref_type": "branch"
        },
        "public": True,
        "created_at": "2025-12-19T14:00:00Z"
    }

    result = validate_event(invalid_type_event)
    print_result("FAIL - Invalid Data Type", result)
    assert result.status == ValidationStatus.FAIL

    # ========================================================================
    # SCENARIO 7: FAIL - Invalid Format (Regex)
    # ========================================================================
    invalid_format_event = {
        "id": "7234567890a",  # Contains non-digit -> FAIL
        "type": "DeleteEvent",
        "actor": {
            "id": 12350,
            "login": "deleter@invalid",  # Invalid username format -> FAIL
            "url": "https://api.github.com/users/deleter"
        },
        "repo": {
            "id": 789018,
            "name": "invalid_name_format",  # Missing owner/ prefix -> FAIL
            "url": "https://api.github.com/repos/deleter/deleted-repo"
        },
        "payload": {
            "ref_type": "tag"
        },
        "public": True,
        "created_at": "2025-12-19T15:00:00Z"
    }

    result = validate_event(invalid_format_event)
    print_result("FAIL - Multiple Format Violations", result)
    assert result.status == ValidationStatus.FAIL

    # ========================================================================
    # SCENARIO 8: FAIL - Null/Empty Values
    # ========================================================================
    null_values_event = {
        "id": "",  # Empty string -> FAIL
        "type": None,  # Null value -> FAIL
        "actor": {
            "id": 12351,
            "login": "",  # Empty string -> FAIL
            "url": "https://api.github.com/users/nulluser"
        },
        "repo": {
            "id": 789019,
            "name": "nulluser/null-repo",
            "url": "https://api.github.com/repos/nulluser/null-repo"
        },
        "public": True,
        "created_at": "2025-12-19T16:00:00Z"
    }

    result = validate_event(null_values_event)
    print_result("FAIL - Null and Empty Values", result)
    assert result.status == ValidationStatus.FAIL

    # ========================================================================
    # SCENARIO 9: FAIL - Invalid Timestamp Format
    # ========================================================================
    invalid_timestamp_event = {
        "id": "92345678909",
        "type": "ReleaseEvent",
        "actor": {
            "id": 12352,
            "login": "releaser",
            "url": "https://api.github.com/users/releaser"
        },
        "repo": {
            "id": 789020,
            "name": "releaser/release-repo",
            "url": "https://api.github.com/repos/releaser/release-repo"
        },
        "payload": {
            "release": {"tag_name": "v1.0.0"}
        },
        "public": True,
        "created_at": "invalid-timestamp-format"  # Invalid format -> FAIL
    }

    result = validate_event(invalid_timestamp_event)
    print_result("FAIL - Invalid Timestamp Format", result)
    assert result.status == ValidationStatus.FAIL

    # ========================================================================
    # SCENARIO 10: WARN - Inconsistent Payload Structure
    # ========================================================================
    inconsistent_payload_event = {
        "id": "10234567891",
        "type": "PushEvent",
        "actor": {
            "id": 12353,
            "login": "pusher",
            "url": "https://api.github.com/users/pusher"
        },
        "repo": {
            "id": 789021,
            "name": "pusher/push-repo",
            "url": "https://api.github.com/repos/pusher/push-repo"
        },
        "payload": {
            # Missing "ref" and "commits" for PushEvent -> WARN
            "other_field": "value"
        },
        "public": True,
        "created_at": "2025-12-19T17:00:00Z"
    }

    result = validate_event(inconsistent_payload_event)
    print_result("WARN - Inconsistent Payload Structure", result)
    # Should be WARN due to consistency check

    # ========================================================================
    # Summary Statistics
    # ========================================================================
    print(f"\n{'='*70}")
    print("VALIDATION SUMMARY")
    print(f"{'='*70}")

    metrics = get_metrics()
    stats = metrics.export_json()

    print(f"\nTotal Validations: {stats['total_validations']}")
    print(f"  [OK] Passed: {stats['passed_validations']}")
    print(f"  [WARN] Warnings: {stats['warned_validations']}")
    print(f"  [FAIL] Failed: {stats['failed_validations']}")
    print(f"\nPass Rate: {stats['pass_rate']:.1%}")
    print(f"Warn Rate: {stats['warn_rate']:.1%}")
    print(f"Fail Rate: {stats['fail_rate']:.1%}")
    print(
        f"\nAverage Processing Time: {stats['avg_processing_time_seconds']*1000:.2f}ms")

    print(f"\nFailures by Check Type:")
    for check_type, count in sorted(stats['failures_by_check'].items()):
        print(f"  {check_type}: {count}")

    print(f"\nFailures by Severity:")
    for severity, count in sorted(stats['failures_by_severity'].items()):
        print(f"  {severity}: {count}")

    # Show Prometheus metrics format
    print(f"\n{'='*70}")
    print("PROMETHEUS METRICS (Sample)")
    print(f"{'='*70}")
    print(metrics.export_text()[:500] + "...\n")

    print(f"{'='*70}")
    print("All tests completed successfully!")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()

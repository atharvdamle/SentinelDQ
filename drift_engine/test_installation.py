"""
Validation test: Ensure drift engine imports work correctly.

Run this to verify the drift engine installation.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_imports():
    """Test that all drift engine modules can be imported."""
    print("Testing drift engine imports...")

    try:
        # Core models
        from drift_engine.models import (
            DriftType, Severity, TimeWindow, DriftResult, DriftSummary
        )
        print("✓ Models imported successfully")

        # Profilers
        from drift_engine.profiles import (
            SchemaProfile, StatisticalProfile, VolumeProfile
        )
        print("✓ Profilers imported successfully")

        # Detectors
        from drift_engine.detectors import (
            SchemaDriftDetector, DistributionDriftDetector, VolumeDriftDetector
        )
        print("✓ Detectors imported successfully")

        # Engine
        from drift_engine.engine import DriftRunner
        print("✓ Engine imported successfully")

        # Persistence
        from drift_engine.persistence import DriftPostgresWriter
        print("✓ Persistence imported successfully")

        # Reports
        from drift_engine.reports import ReportGenerator
        print("✓ Reports imported successfully")

        print("\n✅ All imports successful!")
        return True

    except ImportError as e:
        print(f"\n❌ Import failed: {e}")
        return False


def test_basic_functionality():
    """Test basic drift engine functionality without database."""
    print("\nTesting basic functionality...")

    try:
        from datetime import datetime, timedelta
        from drift_engine.models import TimeWindow, DriftResult, DriftType, Severity, DriftSummary
        from drift_engine.profiles import SchemaProfile, StatisticalProfile, VolumeProfile

        # Test TimeWindow
        now = datetime.utcnow()
        window = TimeWindow(start=now - timedelta(hours=24), end=now)
        assert window.duration_hours() == 24.0
        print("✓ TimeWindow works correctly")

        # Test SchemaProfile with sample data
        sample_data = [
            {"id": "1", "type": "PushEvent", "actor": {"login": "user1"}},
            {"id": "2", "type": "CreateEvent", "actor": {"login": "user2"}},
            {"id": "3", "type": "PushEvent", "actor": {"login": "user1"}}
        ]

        profile = SchemaProfile.from_records(sample_data)
        assert profile.row_count == 3
        assert "id" in profile.fields
        assert "type" in profile.fields
        assert "actor.login" in profile.fields
        print("✓ SchemaProfile works correctly")

        # Test StatisticalProfile
        stat_profile = StatisticalProfile.from_records(
            sample_data,
            categorical_fields=["type"],
            numerical_fields=[],
            max_categories=100
        )
        assert "type" in stat_profile.categorical
        print("✓ StatisticalProfile works correctly")

        # Test VolumeProfile
        vol_profile = VolumeProfile.from_records(
            sample_data,
            entity_fields=["type"],
            top_n=10
        )
        assert vol_profile.total_count == 3
        print("✓ VolumeProfile works correctly")

        # Test DriftResult
        drift = DriftResult(
            drift_type=DriftType.SCHEMA,
            entity="test",
            field_name="test_field",
            baseline_window=window,
            current_window=window,
            metric_name="test_metric",
            baseline_value=10,
            current_value=20,
            drift_score=0.5,
            severity=Severity.WARNING,
            metadata={"test": "value"}
        )
        assert drift.drift_type == DriftType.SCHEMA
        assert drift.severity == Severity.WARNING
        print("✓ DriftResult works correctly")

        # Test DriftSummary
        summary = DriftSummary(
            run_timestamp=now,
            baseline_window=window,
            current_window=window,
            total_checks=10,
            total_drifts=0
        )
        summary.add_result(drift)
        assert summary.total_drifts == 1
        assert summary.warning_count == 1
        print("✓ DriftSummary works correctly")

        print("\n✅ All basic functionality tests passed!")
        return True

    except Exception as e:
        print(f"\n❌ Functionality test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_configuration():
    """Test that configuration file can be loaded."""
    print("\nTesting configuration...")

    try:
        import yaml
        from pathlib import Path

        config_path = Path(__file__).parent / "config" / "drift_config.yaml"

        if not config_path.exists():
            print(f"❌ Config file not found: {config_path}")
            return False

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Verify expected sections
        assert "windowing" in config
        assert "thresholds" in config
        assert "targets" in config
        assert "profiling" in config

        print("✓ Configuration file loaded successfully")
        print(
            f"  - Baseline window: {config['windowing']['baseline']['days']} days")
        print(
            f"  - Current window: {config['windowing']['current']['hours']} hours")

        print("\n✅ Configuration test passed!")
        return True

    except Exception as e:
        print(f"\n❌ Configuration test failed: {e}")
        return False


def main():
    """Run all validation tests."""
    print("=" * 80)
    print("SENTINELDQ DRIFT ENGINE - VALIDATION TEST")
    print("=" * 80)
    print()

    results = []

    # Run tests
    results.append(("Imports", test_imports()))
    results.append(("Basic Functionality", test_basic_functionality()))
    results.append(("Configuration", test_configuration()))

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)

    for test_name, passed in results:
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{test_name:.<40} {status}")

    all_passed = all(result[1] for result in results)

    print("\n" + "=" * 80)
    if all_passed:
        print("✅ ALL TESTS PASSED - Drift engine is ready to use!")
        print("\nNext steps:")
        print("  1. Ensure PostgreSQL is running and accessible")
        print("  2. Run: python drift_engine/run_drift_detection.py")
        print("  3. Check drift_results table for results")
    else:
        print("❌ SOME TESTS FAILED - Please check the errors above")
    print("=" * 80)

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())

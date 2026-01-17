# SentinelDQ Drift Detection Engine

## Overview

The **Drift Detection Engine** is a production-grade, batch-oriented system that identifies meaningful changes in data over time. Unlike real-time validation (which catches malformed records), drift detection identifies:

- **Schema drift** - Structural changes in data contracts
- **Statistical drift** - Changes in data distributions and patterns  
- **Volume drift** - Traffic pattern anomalies

## Architecture

```
drift_engine/
├── config/
│   └── drift_config.yaml          # Configuration and thresholds
├── profiles/
│   ├── schema_profile.py          # Schema metadata extraction
│   ├── statistical_profile.py    # Distribution profiling
│   └── volume_profile.py          # Count/volume profiling
├── detectors/
│   ├── schema_drift.py            # Schema drift detection
│   ├── distribution_drift.py     # Statistical drift detection
│   └── volume_drift.py            # Volume drift detection
├── engine/
│   └── drift_runner.py            # Main orchestration engine
├── models/
│   └── drift_result.py            # Data models
├── persistence/
│   └── postgres_writer.py         # PostgreSQL persistence
├── reports/
│   └── report_generator.py        # Report generation
└── run_drift_detection.py         # CLI entry point
```

## How It Works

### 1. Time Windows

Drift detection compares two time windows:

- **Baseline Window**: Last 7 days (excluding most recent 24h) - represents "normal" behavior
- **Current Window**: Last 24 hours - represents "active" behavior

```
Timeline:
├─────────────────────────────┬────────┬───┤
│    Baseline (7 days)        │Current │Now│
│                              │(24h)   │   │
└─────────────────────────────┴────────┴───┘
```

### 2. Profiling

For each window, the engine builds three profiles:

**Schema Profile**
- Field names and paths (including nested)
- Data types
- Nullability
- Cardinality (unique value counts)
- Presence ratios

**Statistical Profile**
- Categorical field distributions (e.g., event type percentages)
- Numerical field statistics (mean, std, percentiles)
- Null ratios

**Volume Profile**
- Total record counts
- Per-entity counts (e.g., per event type, per repo)
- Normalized event rates

### 3. Drift Detection

Three specialized detectors compare profiles:

#### Schema Drift Detector

Detects:
- **Field additions** (INFO if nullable, WARNING if mandatory)
- **Field removals** (CRITICAL - data loss risk)
- **Type changes** (CRITICAL - breaking change)
- **Cardinality explosions** (WARNING/CRITICAL based on ratio)

#### Distribution Drift Detector

Uses statistical tests:
- **PSI (Population Stability Index)** for categorical distributions
  - PSI < 0.1: No drift
  - 0.1 ≤ PSI < 0.25: WARNING
  - PSI ≥ 0.25: CRITICAL
  
- **Mean shift analysis** for numerical fields
  - Normalized by standard deviations
  - 1-2 std: INFO
  - 2-3 std: WARNING
  - \>3 std: CRITICAL

- **Null ratio changes**
  - >10% change: WARNING
  - >25% change: CRITICAL

#### Volume Drift Detector

Uses anomaly detection:
- **Z-score analysis** for global volume
  - |z| < 2: Normal
  - 2 ≤ |z| < 3: WARNING
  - |z| ≥ 3: CRITICAL

- **Percentage change** for per-entity volumes
  - >20%: INFO
  - >50%: WARNING

### 4. Persistence

All drift results are stored in PostgreSQL:

```sql
drift_results (
    drift_id,
    drift_type,           -- 'schema', 'distribution', 'volume'
    entity,               -- 'global', 'event_type', 'repo'
    field_name,
    baseline_window,
    current_window,
    metric_name,          -- 'psi', 'ks_statistic', 'z_score', etc.
    baseline_value,       -- JSONB
    current_value,        -- JSONB
    drift_score,          -- 0-1 normalized score
    severity,             -- 'INFO', 'WARNING', 'CRITICAL'
    detected_at,
    metadata              -- JSONB
)
```

### 5. Reporting

Generate human-readable reports in multiple formats:
- **Text**: Console-friendly summary
- **JSON**: Machine-readable for integrations
- **Markdown**: Documentation-friendly

## Usage

### Basic Usage

```bash
# Run drift detection with default configuration
python drift_engine/run_drift_detection.py

# Save report as JSON
python drift_engine/run_drift_detection.py --output report.json --format json

# Custom configuration
python drift_engine/run_drift_detection.py --config custom_config.yaml --log-level DEBUG
```

### Programmatic Usage

```python
from drift_engine.engine import DriftRunner
from drift_engine.reports import ReportGenerator

# Initialize and run
runner = DriftRunner()
summary = runner.run()

# Generate report
report = ReportGenerator.generate_text_report(summary)
print(report)

# Check severity
if summary.critical_count > 0:
    print(f"ALERT: {summary.critical_count} critical drifts detected!")
```

### Scheduled Execution

Add to cron (Linux):
```bash
0 */6 * * * cd /path/to/SentinelDQ && python drift_engine/run_drift_detection.py >> /var/log/drift.log 2>&1
```

Windows Task Scheduler:
```powershell
# Every 6 hours
$action = New-ScheduledTaskAction -Execute 'python' -Argument 'drift_engine/run_drift_detection.py'
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Hours 6)
Register-ScheduledTask -Action $action -Trigger $trigger -TaskName "SentinelDQ-Drift"
```

## Configuration

Edit `drift_engine/config/drift_config.yaml` to customize:

```yaml
# Windowing strategy
windowing:
  baseline:
    days: 7
  current:
    hours: 24

# Detection thresholds
thresholds:
  schema:
    cardinality_warning_ratio: 2.0
    cardinality_critical_ratio: 5.0
  
  distribution:
    psi:
      info: 0.1
      warning: 0.25
  
  volume:
    z_score:
      info: 2.0
      warning: 3.0

# What to detect
targets:
  schema_drift:
    enabled: true
  distribution_drift:
    enabled: true
    categorical_fields:
      - "type"
      - "actor.login"
      - "repo.name"
    numerical_fields:
      - "payload.size"
  volume_drift:
    enabled: true
    top_n_entities: 50
```

## Example Drift Scenarios

### Scenario 1: New Event Type Introduced

**Detection**: Schema drift - field addition
- `type` field gains new value `SecurityAdvisoryEvent`
- Distribution drift - PSI increase in `type` distribution
- Volume drift - per-entity count change

**Severity**: WARNING (new data, not breaking)

### Scenario 2: Upstream API Removes Field

**Detection**: Schema drift - field removal
- `payload.ref` field disappears from all events

**Severity**: CRITICAL (data loss risk)

### Scenario 3: Traffic Spike

**Detection**: Volume drift - z-score > 3
- Event rate: 2000/hr → 8000/hr

**Severity**: CRITICAL (anomalous traffic)

### Scenario 4: A/B Test Rollout

**Detection**: Distribution drift - PSI change
- `PushEvent` ratio: 45% → 60%

**Severity**: WARNING (significant distribution shift)

## Integration with SentinelDQ

### Data Flow

```
GitHub API
    ↓
Kafka (raw events)
    ↓
Validation Engine ─→ github_events_processed
    ↓
MinIO (backup)
    ↓
[Periodic Batch Process]
    ↓
Drift Engine ─→ drift_results
    ↓
Reports/Alerts
```

### Relationship to Validation

| Aspect | Validation | Drift Detection |
|--------|-----------|----------------|
| **Timing** | Real-time (inline) | Batch (periodic) |
| **Scope** | Individual records | Aggregate patterns |
| **Purpose** | Reject malformed data | Detect behavior changes |
| **Blocking** | Blocks ingestion | Never blocks |
| **Example** | "Field `id` is null" | "`type` distribution shifted 30%" |

## Observability

### Metrics (Future Enhancement)

```python
# Prometheus-style metrics
drift_checks_total{drift_type="schema"} 150
drift_detected_total{severity="CRITICAL"} 3
drift_detected_total{severity="WARNING"} 7
drift_detection_duration_seconds 12.5
```

### Logs

```
2025-12-21 14:00:00 - INFO - Starting drift detection run
2025-12-21 14:00:05 - INFO - Fetched 50000 baseline records
2025-12-21 14:00:06 - INFO - Fetched 2000 current records
2025-12-21 14:00:10 - WARNING - [CRITICAL] Field removed: payload.ref
2025-12-21 14:00:10 - INFO - [WARNING] PSI drift in type: 0.18
2025-12-21 14:00:12 - INFO - Drift detection complete: 5 drifts detected
```

## Scaling Considerations

### Production Readiness

1. **Window Size Tuning**
   - Larger baseline → more stable, slower to adapt
   - Smaller current → faster detection, more false positives

2. **Data Volume**
   - Profile computation is O(n) on record count
   - For >10M records/day, consider sampling strategies

3. **Parallelization**
   - Profile building can be parallelized by field
   - Detection runs independently for each drift type

4. **Storage Growth**
   - `drift_results` table grows with detections
   - Implement retention policy (e.g., 90 days)

### Future Enhancements

- **Adaptive thresholds**: Learn normal variance from historical data
- **Seasonal baselines**: Compare "this Monday" vs "last 4 Mondays"
- **Correlation detection**: Multi-field drift patterns
- **Automated alerting**: Slack/email notifications for CRITICAL drifts
- **Drift explanation**: Root cause analysis and impact assessment
- **Web dashboard**: Visual drift timeline and trends

## Troubleshooting

### No drift detected (but expected)

- Check data is flowing to `github_events_processed`
- Verify time windows contain sufficient data (min_sample_size)
- Review thresholds - may be too lenient

### Too many false positives

- Increase thresholds in `drift_config.yaml`
- Lengthen baseline window for more stability
- Check for expected variance (weekday/weekend patterns)

### Database connection errors

- Verify PostgreSQL credentials in `.env`
- Ensure `drift_results` table exists (auto-created on first run)
- Check network connectivity

## Summary

The SentinelDQ Drift Detection Engine provides:

✅ **Production-grade**: Robust, scalable, and battle-tested approach  
✅ **Non-blocking**: Never impacts ingestion throughput  
✅ **Explainable**: Every drift has clear metrics and context  
✅ **Actionable**: Severity-based prioritization  
✅ **Observable**: Comprehensive logging and persistence  
✅ **Extensible**: Easy to add new drift types and detectors  

This is not alert spam - this is intelligent, meaningful data quality observability.

# SentinelDQ Data Validation System

## Production-Grade Data Quality Framework

**Version:** 1.0.0  
**Last Updated:** December 19, 2025

---

## Overview

SentinelDQ is a production-ready data validation system designed for event-driven architectures. It provides comprehensive validation, quality monitoring, and drift detection for software-generated events (initially designed for GitHub Events).

**Key Characteristics:**
- **Configuration-Driven**: Rules defined in YAML, not hardcoded
- **Three-Tier Severity**: PASS, WARN, FAIL with clear processing logic
- **Full Traceability**: All validation decisions persisted to PostgreSQL
- **Observable**: Prometheus-compatible metrics for monitoring
- **Modular**: Clean separation of concerns, easy to extend

---

## Architecture

```
data_validation/
├── rules/
│   └── github_events.yaml          # Validation rules (YAML)
├── checks/
│   ├── schema.py                   # Required field validation
│   ├── type_checks.py              # Data type validation
│   ├── value_checks.py             # Regex, enum, range, null checks
│   ├── consistency_checks.py       # Cross-field and timestamp validation
│   └── drift_checks.py             # Schema/distribution drift (Phase 2)
├── engine/
│   └── validator.py                # Orchestration engine
├── models/
│   └── validation_result.py        # Data structures (ValidationResult, etc.)
├── persistence/
│   └── postgres_writer.py          # PostgreSQL storage
├── metrics/
│   └── prometheus.py               # Prometheus metrics
└── data_validator.py               # High-level API
```

---

## Quick Start

### Installation

```bash
cd SentinelDQ
pip install -r requirements.txt
```

### Basic Usage

```python
from data_validation import validate_event

event = {
    "id": "12345",
    "type": "PushEvent",
    "actor": {"id": 123, "login": "user"},
    "repo": {"id": 456, "name": "user/repo"},
    "created_at": "2025-12-19T10:00:00Z",
    "public": True
}

result = validate_event(event)

if result.status == "PASS":
    # Insert into processed table
    pass
elif result.status == "WARN":
    # Insert but mark with warnings
    pass
else:  # FAIL
    # Do not process, log failures
    pass
```

---

## Validation Flow

### 1. Validation Order (Critical → Least Critical)

1. **Schema Checks**: Required fields present
2. **Type Checks**: Correct data types
3. **Null/Empty Checks**: No invalid null/empty values
4. **Value Checks**: Regex, enum, range validation
5. **Timestamp Checks**: Format, future, age validation
6. **Consistency Checks**: Cross-field relationships
7. **Duplicate Detection**: Event ID uniqueness

### 2. Status Determination Logic

- **FAIL**: Any check with `severity: CRITICAL` fails
- **WARN**: Any check with `severity: WARNING` fails (no critical failures)
- **PASS**: All checks pass

---

## Validation Rules (YAML)

### Rule Structure

```yaml
version: "1.0"
event_type: "github_events"

schema:
  required_fields:
    - path: "id"
      severity: "FAIL"
      description: "Event unique identifier"

type_checks:
  - field: "actor.id"
    expected_type: "integer"
    severity: "FAIL"

value_checks:
  - field: "id"
    check_type: "regex"
    pattern: "^[0-9]+$"
    severity: "FAIL"
    error_message: "Event ID must be numeric"

timestamp_checks:
  - field: "created_at"
    check_type: "not_future"
    tolerance_seconds: 300
    severity: "WARN"

consistency_checks:
  - name: "payload_structure"
    check_type: "conditional"
    rules:
      - if_field: "type"
        equals: "PushEvent"
        then_required: ["payload.ref", "payload.commits"]
```

### Check Types

| Check Type | Purpose | Example |
|------------|---------|---------|
| `schema` | Required field presence | `actor.id` must exist |
| `type` | Data type validation | `actor.id` must be integer |
| `null` | Null/empty validation | `id` cannot be empty string |
| `regex` | Pattern matching | ID must match `^[0-9]+$` |
| `enum` | Allowed values | type must be in list |
| `range` | Numeric bounds | ID between 1-999999999 |
| `timestamp` | Timestamp validation | ISO8601 format, not future |
| `consistency` | Cross-field logic | If type=PushEvent, payload.ref required |

---

## Processing Behavior

### FAIL Status
```python
if result.status == ValidationStatus.FAIL:
    # DO NOT insert into github_events_processed
    # Log to validation_results table
    # Alert on critical failures
    logger.error(f"Event {event_id} failed validation: {result.error_messages}")
```

### WARN Status
```python
if result.status == ValidationStatus.WARN:
    # INSERT into github_events_processed
    # Add 'has_warnings' flag
    # Log to validation_results table
    # Monitor warning trends
    processed_event['validation_warnings'] = result.warning_failures
```

### PASS Status
```python
if result.status == ValidationStatus.PASS:
    # INSERT into github_events_processed
    # No special handling
    # Log to validation_results table (optional)
```

---

## Database Persistence

### Validation Results Table

```sql
CREATE TABLE validation_results (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    failed_checks JSONB,
    error_messages JSONB,
    severity VARCHAR(50),
    validation_ts TIMESTAMP NOT NULL,
    processing_time_ms FLOAT,
    metadata JSONB,
    failure_details JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_validation_event_id ON validation_results(event_id);
CREATE INDEX idx_validation_status ON validation_results(status);
CREATE INDEX idx_validation_ts ON validation_results(validation_ts DESC);
```

### Usage with Persistence

```python
from data_validation import DataValidator

db_config = {
    'host': 'localhost',
    'port': 5432,
    'database': 'sentinel_dq',
    'user': 'postgres',
    'password': 'postgres'
}

validator = DataValidator(
    rules_path='data_validation/rules/github_events.yaml',
    db_config=db_config,
    enable_persistence=True,
    enable_metrics=True
)

result = validator.validate_event(event)

# Result automatically persisted to database
```

---

## Prometheus Metrics

### Exposed Metrics

```prometheus
# Total validations by status
sentineldq_validation_total
sentineldq_validation_passed
sentineldq_validation_warned
sentineldq_validation_failed

# Failures by check type
sentineldq_validation_check_failures{check_type="schema"}
sentineldq_validation_check_failures{check_type="type"}
sentineldq_validation_check_failures{check_type="regex"}

# Processing time histogram
sentineldq_validation_duration_seconds_bucket{le="0.01"}
sentineldq_validation_duration_seconds_sum
sentineldq_validation_duration_seconds_count

# Pass rate gauge
sentineldq_validation_pass_rate
```

### Export Metrics

```python
from data_validation.metrics import get_metrics

metrics = get_metrics()

# Prometheus text format
print(metrics.export_text())

# JSON format (for logging)
print(metrics.export_json())
```

### Alert Conditions

```python
alerts = metrics.get_alert_conditions()

if alerts['high_failure_rate']:
    # More than 5% failures
    send_alert("High validation failure rate detected")

if alerts['slow_validation']:
    # Average > 100ms
    send_alert("Validation performance degraded")
```

---

## Integration with Ingestion Pipeline

### Kafka Consumer Integration

```python
from confluent_kafka import Consumer
from data_validation import DataValidator

validator = DataValidator(enable_persistence=True)

consumer = Consumer(kafka_config)
consumer.subscribe(['github-events'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    
    event = json.loads(msg.value())
    
    # Validate before processing
    result = validator.validate_event(event)
    
    if result.status == ValidationStatus.FAIL:
        # Skip processing
        logger.error(f"Skipping event {event['id']}: {result.error_messages}")
        continue
    
    # Process valid/warned events
    insert_to_processed_table(event, result)
```

### Batch Processing

```python
# For batch jobs
validator = DataValidator()

events = read_events_from_source()
results = validator.validate_batch(events)

# Separate by status
passed = [e for e, r in zip(events, results) if r.status == "PASS"]
warned = [e for e, r in zip(events, results) if r.status == "WARN"]
failed = [e for e, r in zip(events, results) if r.status == "FAIL"]

# Process accordingly
process_clean_events(passed)
process_warned_events(warned)
log_failed_events(failed)
```

---

## Example Outputs

### PASS Example

```
Status: PASS
Event ID: 12345678901
Processing Time: 0.16ms
Failure Count: 0
[OK] All validation checks passed!
```

### WARN Example

```
Status: WARN
Event ID: 22345678902
Processing Time: 0.10ms
Failure Count: 1

Failures:
  1. value_check.enum.type
     Field: type
     Type: enum
     Severity: WARN
     Message: Unknown or unsupported event type
     Expected: one of [PushEvent, IssuesEvent, ...]
     Actual: NewUnknownEvent
```

### FAIL Example

```
Status: FAIL
Event ID: 52345678905
Processing Time: 0.09ms
Failure Count: 5

Failures:
  1. schema.required_field.created_at
     Field: created_at
     Type: schema
     Severity: FAIL
     Message: Required field 'created_at' is missing
     
  2. type_check.actor.id
     Field: actor.id
     Type: type
     Severity: FAIL
     Message: Actor ID must be an integer
     Expected: integer
     Actual: str
     
  [... additional failures ...]
```

---

## Edge Cases and Failure Modes

### 1. Malformed JSON
- **Handled By**: Ingestion layer (before validation)
- **Action**: Log parse error, skip event

### 2. Schema Evolution (New Fields)
- **Handled By**: Schema drift detection (Phase 2)
- **Action**: WARN on new fields, update baseline

### 3. Missing Nested Fields
- **Handled By**: Schema checker with dot notation
- **Example**: `actor.id` checks `event['actor']['id']`

### 4. Null vs. Missing
- **Distinction**: 
  - Missing: Field doesn't exist
  - Null: Field exists but value is `null`
- **Both handled by**: Schema + Null checkers

### 5. Type Coercion
- **Strategy**: No automatic coercion
- **Reason**: Explicit is better than implicit
- **Example**: `"123"` as integer fails type check

### 6. Validation Engine Errors
- **Handled By**: Try-catch in engine
- **Result**: Creates FAIL result with system error
- **Logged**: Full exception details

### 7. Database Connection Loss
- **Handled By**: Warning log, validation continues
- **Reason**: Validation should not block processing
- **Retry**: Batch persistence on reconnect

### 8. Duplicate Detection at Scale
- **Current**: In-memory cache (development)
- **Production**: Query PostgreSQL with time window
- **Optimization**: Redis cache for O(1) lookups

---

## Production Scaling Considerations

### 1. Performance Optimization

**Current Performance**: ~0.1-0.2ms per event

**Bottlenecks**:
- Regex compilation (cached)
- Database writes (use batching)
- Duplicate checks (use Redis)

**Optimizations**:
```python
# Batch validation for throughput
results = validator.validate_batch(events, persist=True)

# Async database writes
validator = DataValidator(enable_async_persistence=True)  # Future

# Sampling for high volume
validator = DataValidator(sample_rate=0.1)  # Validate 10%
```

### 2. Distributed Validation

For multi-node processing:
```
[Kafka Topic] 
    ↓
[Consumer Group: Validators] (3 nodes)
    ↓
[Shared PostgreSQL + Redis]
    ↓
[Processed Events Topic]
```

### 3. Rule Hot-Reloading

```python
# Watch rules file for changes
validator = ValidationEngine(
    rules_path='rules/github_events.yaml',
    auto_reload=True,
    reload_interval=60  # Check every 60s
)
```

### 4. Monitoring Dashboard

**Grafana Queries**:
```promql
# Pass rate over time
rate(sentineldq_validation_passed[5m]) / 
rate(sentineldq_validation_total[5m])

# P99 latency
histogram_quantile(0.99, 
    sentineldq_validation_duration_seconds_bucket)

# Failures by check type
sum by (check_type) (
    sentineldq_validation_check_failures
)
```

---

## Future Enhancements (Phase 2)

### 1. Batch-Level Drift Detection

```python
from data_validation.checks.drift_checks import DriftDetector

detector = DriftDetector(rules['drift_detection'])

# Schema drift
schema_drift = detector.detect_schema_drift(
    current_batch=recent_events,
    baseline_schema=load_baseline()
)

# Distribution drift
dist_drift = detector.detect_distribution_drift(
    current_batch=recent_events,
    baseline_window_days=7
)
```

### 2. Machine Learning Integration

- Anomaly detection for numeric fields
- Pattern learning for new event types
- Automatic threshold tuning

### 3. Web UI

- Visual rule editor
- Real-time validation dashboard
- Failure investigation tools

### 4. Multi-Tenancy

- Per-tenant validation rules
- Isolated validation metrics
- Custom alert configurations

---

## Testing

### Run Examples

```bash
# Basic test
python data_validation/data_validator.py

# Comprehensive examples (PASS/WARN/FAIL)
python data_validation/examples/validation_examples.py

# Unit tests
pytest tests/ -v
```

### Create Custom Test

```python
from data_validation import validate_event, ValidationStatus

def test_custom_event():
    event = {...}  # Your test event
    result = validate_event(event)
    
    assert result.status == ValidationStatus.PASS
    assert len(result.failures) == 0
```

---

## Conclusion

SentinelDQ provides a **production-grade**, **scalable**, and **observable** data validation framework that ensures data quality at the ingestion layer. By validating events before they enter the processed pipeline, we prevent bad data from polluting downstream analytics and ML models.

**Key Takeaways**:
- ✅ Configuration-driven rules (no code changes needed)
- ✅ Three-tier severity with clear processing logic
- ✅ Full traceability and observability
- ✅ Performance-optimized (~0.1-0.2ms per event)
- ✅ Production-ready architecture

For questions or contributions, see the main [README.md](../README.md).

---

**Built with ❤️ for Data Quality**

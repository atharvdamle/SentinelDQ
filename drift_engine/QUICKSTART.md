# SentinelDQ Drift Detection - Quick Start Guide

## 🚀 Get Started in 5 Minutes

### Prerequisites

1. **SentinelDQ base system running**
   - PostgreSQL with `github_events_processed` table
   - Data flowing through ingestion pipeline

2. **Environment variables set** (in `.env`)
   ```bash
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   POSTGRES_DB=SentinelDQ_DB
   POSTGRES_USER=postgres
   POSTGRES_PASSWORD=your_password
   ```

3. **Python dependencies** (already installed if using SentinelDQ)
   ```bash
   pip install psycopg2-binary pyyaml
   ```

---

## Step 1: Run Your First Drift Detection

```bash
cd drift_engine
python run_drift_detection.py
```

**Expected output:**
```
================================================================================
SENTINELDQ DRIFT DETECTION REPORT
================================================================================

Run Timestamp: 2025-12-21 14:30:00 UTC

Baseline Window: 2025-12-13 00:00 to 2025-12-20 00:00
Current Window:  2025-12-20 14:30 to 2025-12-21 14:30

--------------------------------------------------------------------------------
SUMMARY
--------------------------------------------------------------------------------
Total Checks Performed: 45
Total Drifts Detected:  3

  CRITICAL: 0
  WARNING:  1
  INFO:     2

Drifts by Type:
  distribution: 2
  volume: 1

... (detailed drift information)
```

---

## Step 2: Check Database Results

```sql
-- View all detected drifts
SELECT 
    drift_type,
    entity,
    field_name,
    metric_name,
    severity,
    drift_score,
    detected_at
FROM drift_results
ORDER BY detected_at DESC
LIMIT 10;

-- Count drifts by severity
SELECT severity, COUNT(*) 
FROM drift_results 
GROUP BY severity;

-- Get critical drifts from last 24 hours
SELECT * 
FROM drift_results 
WHERE severity = 'CRITICAL' 
  AND detected_at >= NOW() - INTERVAL '24 hours';
```

---

## Step 3: Customize Configuration

Edit `drift_engine/config/drift_config.yaml`:

```yaml
# Make detection more sensitive (lower thresholds)
thresholds:
  distribution:
    psi:
      info: 0.05      # Was 0.1
      warning: 0.15   # Was 0.25

# Or make it less sensitive (higher thresholds)
thresholds:
  volume:
    z_score:
      info: 2.5       # Was 2.0
      warning: 4.0    # Was 3.0
```

---

## Step 4: Save Reports

```bash
# Save as JSON
python run_drift_detection.py --output report.json --format json

# Save as Markdown
python run_drift_detection.py --output report.md --format markdown

# Both console and file
python run_drift_detection.py --output report.txt
```

---

## Step 5: Schedule Periodic Runs

### Linux/Mac (cron)

```bash
# Edit crontab
crontab -e

# Add this line (runs every 6 hours)
0 */6 * * * cd /path/to/SentinelDQ && python drift_engine/run_drift_detection.py >> /var/log/drift.log 2>&1
```

### Windows (Task Scheduler)

```powershell
# Create scheduled task (PowerShell as Admin)
$action = New-ScheduledTaskAction `
    -Execute 'python' `
    -Argument 'C:\path\to\SentinelDQ\drift_engine\run_drift_detection.py'

$trigger = New-ScheduledTaskTrigger `
    -Once -At (Get-Date) `
    -RepetitionInterval (New-TimeSpan -Hours 6)

Register-ScheduledTask `
    -Action $action `
    -Trigger $trigger `
    -TaskName "SentinelDQ-Drift-Detection" `
    -Description "Runs drift detection every 6 hours"
```

---

## Common Use Cases

### Use Case 1: Continuous Monitoring

```bash
# Run in a loop with delays
while true; do
    python drift_engine/run_drift_detection.py
    sleep 21600  # 6 hours
done
```

### Use Case 2: CI/CD Integration

```bash
# In your deployment pipeline
python drift_engine/run_drift_detection.py

# Check exit code
if [ $? -eq 1 ]; then
    echo "CRITICAL drift detected! Aborting deployment."
    exit 1
fi
```

### Use Case 3: Alerting Pipeline

```python
# drift_alerting.py
from drift_engine.engine import DriftRunner
import requests  # for Slack

runner = DriftRunner()
summary = runner.run()

if summary.critical_count > 0:
    # Send Slack alert
    webhook_url = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
    message = {
        "text": f"🚨 CRITICAL: {summary.critical_count} drifts detected!",
        "attachments": [
            {
                "color": "danger",
                "text": str(drift)
            }
            for drift in summary.get_critical_drifts()
        ]
    }
    requests.post(webhook_url, json=message)
```

---

## Troubleshooting

### Problem: "No data found in baseline window"

**Solution**: Ensure you have at least 100 records in the last 7 days
```sql
SELECT COUNT(*) 
FROM github_events_processed 
WHERE processed_at >= NOW() - INTERVAL '7 days';
```

### Problem: "Connection to PostgreSQL failed"

**Solution**: Check `.env` file and database connectivity
```bash
# Test connection
psql -h localhost -p 5432 -U postgres -d SentinelDQ_DB
```

### Problem: "Too many false positives"

**Solution**: Increase thresholds in `drift_config.yaml`
- Start conservative: PSI warning at 0.4, z_score at 4.0
- Gradually tighten based on data characteristics

### Problem: "Missing expected drift"

**Solution**: 
1. Check if data actually changed significantly
2. Lower thresholds (more sensitive)
3. Verify field names match configuration

---

## Next Steps

1. **Read the full documentation**: `drift_engine/README.md`
2. **Review implementation details**: `drift_engine/IMPLEMENTATION_SUMMARY.md`
3. **Try example integrations**: `drift_engine/examples/pipeline_integration.py`
4. **Customize for your data**: Adjust categorical/numerical fields in config
5. **Set up alerting**: Integrate with Slack/PagerDuty/email

---

## Getting Help

- **Check logs**: Drift detection emits detailed INFO/WARNING/ERROR logs
- **Query database**: All results in `drift_results` table
- **Review reports**: Generated reports contain full context
- **Example code**: See `examples/` directory

---

## Key Metrics to Monitor

After running for a few days, check:

```sql
-- Drift frequency over time
SELECT 
    DATE_TRUNC('day', detected_at) as day,
    severity,
    COUNT(*) 
FROM drift_results 
GROUP BY day, severity 
ORDER BY day DESC;

-- Most frequently drifting fields
SELECT 
    entity,
    field_name,
    COUNT(*) as drift_count
FROM drift_results
WHERE detected_at >= NOW() - INTERVAL '30 days'
GROUP BY entity, field_name
ORDER BY drift_count DESC
LIMIT 10;

-- Average drift scores by type
SELECT 
    drift_type,
    AVG(drift_score) as avg_score,
    MAX(drift_score) as max_score
FROM drift_results
GROUP BY drift_type;
```

---

## Summary

You've now:
- ✅ Run your first drift detection
- ✅ Understood the output format
- ✅ Learned how to customize thresholds
- ✅ Scheduled periodic runs
- ✅ Know how to troubleshoot common issues

**The drift detection engine is now monitoring your data quality! 🎉**

Continue to the full documentation for advanced features and production deployment guidance.

# SentinelDQ

**Real-time data quality validation and drift monitoring.**

SentinelDQ ingests a stream of events, validates each one against YAML-defined rules before it reaches
the database, and periodically runs batch statistical drift detection over what has accumulated. It
ships with a GitHub Events API source; the ingestion layer is swappable.

Two things happen to your data, and they are not the same thing:

- **Validation** is real-time and per-record. It answers *"is this event malformed?"* and blocks bad
  events from being stored.
- **Drift detection** is batch and aggregate. It answers *"has the shape of the data changed?"* — new
  fields, shifted distributions, volume anomalies — and never blocks anything.

| | Validation | Drift detection |
|---|---|---|
| Timing | Inline, synchronous | Every 6 hours, batch |
| Scope | One event | A 24h window vs. a 7d baseline |
| On trigger | Drop the event | Record a finding |
| Output | `PASS` / `WARN` / `FAIL` | `drift_results` rows |

For how the pieces fit together, see **[ARCHITECTURE.md](ARCHITECTURE.md)**.
For known defects and the refactoring roadmap, see **[IMPROVEMENTS.md](IMPROVEMENTS.md)**.

> **Project status:** this is a working system with a documented backlog, not a hardened release. Read
> the [known issues](#known-issues) before running it against anything you care about.

---

## Quick start

**Requirements:** Docker with Compose v2. Nothing else — the five application services build from
the root `Dockerfile`; Kafka, PostgreSQL and MinIO are upstream images.

### 1. Create `.env`

Compose reads this file for variable substitution and it has **no defaults** — several services fail to
start without it.

Copy `.env.example` and fill it in, or start from:

```env
# PostgreSQL
# localhost so host-run processes work; compose overrides it to "postgres"
# for the containers, so one file serves both.
POSTGRES_HOST=localhost
# In-network clients always reach Postgres on 5432. Set POSTGRES_PORT_HOST
# instead if 5432 is taken on your machine.
POSTGRES_PORT=5432
POSTGRES_PORT_HOST=5432
POSTGRES_DB=sentineldq
POSTGRES_USER=sentineldq
POSTGRES_PASSWORD=change_me

# MinIO
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=github-events-backup

# Kafka
KAFKA_TOPIC=github_events

# GitHub
GITHUB_EVENTS_URL=https://api.github.com/events
GITHUB_POLL_INTERVAL_SECONDS=60
```

> **`db/config.py` is the only place `POSTGRES_*` is read.** Every component gets its connection
> settings from there, so there is one set of defaults rather than one per module. `POSTGRES_PASSWORD`
> has no default: leave it unset and you get a configuration error rather than an authentication
> failure. Set it to the empty string if your database uses trust authentication.

### 2. Start everything

```bash
docker-compose up -d
docker-compose ps
```

Eight services come up: Kafka (KRaft), PostgreSQL, MinIO, the validator API, the producer, both
consumers, and the drift detector. Application containers block on a TCP readiness gate
(`scripts/wait_and_run.py`) until their dependencies accept connections.

### 3. Verify

```bash
curl http://localhost:8000/health
# {"status":"ok"}

# Events landing?
# (compose reads .env for its own substitution but does not export into
#  your shell, so source it first)
set -a; . ./.env; set +a
docker-compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT count(*) FROM github_events;"

# Raw backups landing?
docker-compose logs minio-consumer | tail
```

Give it a minute — the producer polls on a 60-second cycle, and drift detection needs
`profiling.min_sample_size` (default 100) records in *both* windows before it reports anything.

### Ports

| Service | Port | |
|---|---|---|
| Validator API | 8000 | `/validate`, `/health` |
| PostgreSQL | `POSTGRES_PORT_HOST` (5432) | Containers always use 5432 in-network |
| MinIO API | 9000 | |
| MinIO Console | 9001 | Browse raw event backups |
| Kafka | 9092 | External listener |

---

## Using the validator

### `POST /validate`

```bash
curl -X POST http://localhost:8000/validate \
  -H 'Content-Type: application/json' \
  -d '{
    "event": {
      "id": "12345",
      "type": "PushEvent",
      "actor": {"id": 1, "login": "octocat", "url": "https://api.github.com/users/octocat"},
      "repo":  {"id": 123, "name": "octo/repo", "url": "https://api.github.com/repos/octo/repo"},
      "public": true,
      "created_at": "2026-08-30T12:00:00Z",
      "payload": {}
    }
  }'
```

Response:

```json
{
  "status": "WARN",
  "event_id": "12345",
  "processing_time_ms": 0.42,
  "failures": [
    {
      "check_name": "consistency.payload_structure_matches_event_type",
      "field_path": "payload.ref",
      "check_type": "consistency",
      "severity": "WARN",
      "error_message": "PushEvent must have 'ref' and 'commits' in payload",
      "expected_value": "present",
      "actual_value": null,
      "rule_definition": null
    }
  ]
}
```

`status` is one of:

| Status | Meaning | Consumer behaviour |
|---|---|---|
| `PASS` | No failures, or informational only | Stored |
| `WARN` | At least one warning, no critical | Stored |
| `FAIL` | At least one critical failure | **Dropped** |

Optional `event_id` in the request overrides the id extracted from the event body.

`GET /health` returns `{"status": "ok"}` and backs the compose healthcheck.

---

## Configuration

### Environment variables

Read by the application code (compose sets the Kafka and service-address ones per service):

| Variable | Default | Used by |
|---|---|---|
| `POSTGRES_HOST` / `PORT` / `DB` / `USER` | `localhost` / `5432` / `sentineldq` / `sentineldq` | `db/config.py`, the only reader |
| `POSTGRES_PASSWORD` | **none — required** | `db/config.py`; unset is a config error |
| `POSTGRES_PORT_HOST` | `5432` | compose only — the published host port |
| `POSTGRES_CONNECT_TIMEOUT` / `POSTGRES_STATEMENT_TIMEOUT_MS` | `10` / `30000` | `db/config.py` |
| `INGEST_BATCH_SIZE` / `INGEST_FLUSH_INTERVAL` | `100` / `5` | Postgres consumer — rows per write, seconds |
| `VALIDATOR_PERSIST` | `true` | validator — set false to skip the `validation_results` write |
| `KAFKA_BOOTSTRAP_SERVERS` | — | producer, both consumers |
| `KAFKA_TOPIC` | — | producer, both consumers |
| `GITHUB_EVENTS_URL` | — | producer |
| `GITHUB_TOKEN` | unset | producer — optional; raises the API rate limit |
| `MINIO_HOST` / `MINIO_API_PORT` | `localhost` / `9000` | MinIO consumer |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` / `MINIO_BUCKET` | — | MinIO consumer |
| `MINIO_SECURE` | `False` | MinIO consumer — selects `https` |
| `VALIDATOR_URL` | `http://validator:8000/validate` | Postgres consumer |
| `VALIDATOR_TIMEOUT` | `0.5` | Postgres consumer — seconds per attempt |
| `VALIDATOR_FALLBACKS` | unset | Postgres consumer — extra comma-separated URLs |

`GITHUB_POLL_INTERVAL_SECONDS` is set by compose but currently has no effect; the producer's interval is
hardcoded to its 60-second default (see IMPROVEMENTS.md F23).

### Validation rules — `data_validation/rules/github_events.yaml`

Each top-level section maps to one checker. Rules are data; adding one requires no code change.

```yaml
schema:
  required_fields:
    - path: "actor.login"        # dot notation traverses nested objects
      severity: "FAIL"
      description: "Actor username"

type_checks:
  - field: "repo.id"
    expected_type: "integer"     # string|integer|float|number|boolean|list|dict|null
    severity: "FAIL"
    error_message: "Repository ID must be an integer"

value_checks:
  - field: "type"
    check_type: "enum"           # enum|regex|range|length
    allowed_values: ["PushEvent", "WatchEvent", "ForkEvent"]
    severity: "WARN"
    error_message: "Unknown or unsupported event type"

timestamp_checks:
  - field: "created_at"
    check_type: "not_future"     # format|not_future|not_too_old|parseable
    tolerance_seconds: 300
    severity: "WARN"
    error_message: "Event timestamp is in the future"
```

`severity: FAIL` maps to a critical failure and drops the event; `WARN` stores it with the warning
recorded; `INFO` is advisory only. Sections not shown: `null_checks`, `consistency_checks`,
`duplicate_check`.

### Drift thresholds — `drift_engine/config/drift_config.yaml`

```yaml
windowing:
  baseline:  {days: 7}       # historical normal
  current:   {hours: 24}     # what we compare against it
  gap_hours: 0

thresholds:
  distribution:
    psi:                     # population stability index, categorical fields
      info: 0.1
      warning: 0.25
  volume:
    percent_change:
      info: 0.20
      warning: 0.50

targets:
  distribution_drift:
    enabled: true
    categorical_fields: ["type", "actor.login", "repo.name"]
    numerical_fields:   ["payload.size"]

profiling:
  min_sample_size: 100              # skip the run below this, per window
  categorical_max_categories: 100   # skip distributions above this cardinality

execution:
  schedule_cron: "0 */6 * * *"      # parsed for */N hours only, not full cron
```

Two caveats worth knowing before you tune these: `categorical_max_categories: 100` means high-cardinality
fields like `actor.login` and `repo.name` are skipped in practice, and the `psi` threshold *names* do not
match the severities they emit. Both are IMPROVEMENTS.md F24.

---

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### Tests

```bash
pytest                              # unit tests
pytest tests/test_db_pool.py        # one file
pytest --cov=. --cov-report=html    # coverage
```

`pytest.ini` sets `testpaths = tests`, so a bare `pytest` no longer picks up the root-level
`test_e2e.py` (which starts Docker). Run that one explicitly: `python test_e2e.py`.

There is no linter, formatter, or CI configured.

### Running components individually

```bash
docker-compose up -d kafka postgres minio    # infrastructure only

python -m ingestion.producers.github_producer
python -m ingestion.consumers.postgres_consumer
python -m ingestion.consumers.minio_consumer
python -m drift_engine.drift_service
uvicorn data_validation.api:app --reload --port 8000
```

Or run the whole application layer as supervised subprocesses with multiplexed logs:

```bash
python run_pipeline.py
```

Full-stack integration check — starts compose, exercises the pipeline, tears down:

```bash
python test_e2e.py
```

### Extending

Both extension points follow the same duck-typed pattern — write a class with the expected method and
wire it in one place. No registry, no plugin discovery. ARCHITECTURE.md has the details.

- **A new validation check:** a class with `validate(event) -> List[ValidationFailure]`, exported from
  `data_validation/checks/__init__.py`, read in `ValidationEngine._initialize_checkers`, called in
  `validate_event`'s fixed order.
- **A new drift type:** a profile with `from_records(...)` and a detector with `detect(...)`, both wired
  into `DriftRunner`, plus a `targets.<name>_drift` section in the config.

### Database schema

No ORM and no migration framework. All DDL lives in `db/schema.py`, and `init_schema()` creates
whatever is missing at startup. Because that is `CREATE TABLE IF NOT EXISTS`, **it will not alter a
table that already exists** — changes to an existing database need a migration applied by hand.
`scripts/migrate_001.sql` is the one that accompanies the persistence-layer consolidation
(`TIMESTAMPTZ`, the missing indexes, and the `validation_results` rework):

```bash
set -a; . ./.env; set +a
docker-compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f - < scripts/migrate_001.sql
```

Timestamps are `TIMESTAMPTZ` throughout. As naive `TIMESTAMP` they mixed the server's local time
(`DEFAULT now()`) with the application's UTC, which silently shifted every drift window unless the
database happened to run in UTC.

| Table | Contents | Indexes |
|---|---|---|
| `github_events` | Validated events, flat columns. Written but not read by any component — `github_events_processed` is what the pipeline uses | `event_id` unique, `created_at` |
| `github_events_processed` | Validated events as JSONB — the drift engine's source | `event_id` unique, `processed_at` |
| `drift_results` | One row per detected drift | `detected_at`, `severity`, `(drift_type, entity)` |
| `validation_results` | One row per validated event, upserted on `event_id` | `event_id` unique, `validation_ts`, `status` |

---

## Known issues

This project has a full audit at **[IMPROVEMENTS.md](IMPROVEMENTS.md)** — 29 findings across six phases,
each with acceptance criteria. The ones most likely to affect you:

- **Kafka offsets auto-commit**, so events dropped by a validator outage are not retried (F9).
- **A new TCP connection to the validator per message** — no `requests.Session`, and a validator outage
  costs four failed URL attempts per event (F8).
- **Prometheus metrics are collected but not exposed** — there is no `/metrics` route (F16).
- **Per-entity volume drift compares mismatched window lengths**, producing false positives (F4).
- **`scipy` and `numpy` are pinned but never imported**, and the KS test the docstrings describe does
  not exist (F22).

Fixed since the audit: the missing import that had taken the drift engine offline (F1); the hanging
test files, so `pytest tests/` now terminates (F2); and the persistence layer — connection handling,
the missing indexes, the divergent DDL, and the four conflicting `POSTGRES_DB` defaults (F7, F10, F11,
F17, F21).

---

## License

No license file is present in this repository. Until one is added, no usage rights are granted — add a
`LICENSE` before publishing or accepting contributions.

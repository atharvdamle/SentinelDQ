# SentinelDQ — Architecture

The single source of truth for how this system is put together. It describes the code as it actually
runs at `production-refinements`. Where a component is inert or a config key is unread, that is stated
as fact — the judgment about whether it should be fixed, and in what order, lives in
[IMPROVEMENTS.md](IMPROVEMENTS.md).

> **Keeping this accurate:** every path and table name below was verified against the code. If you change
> a module boundary, a table schema, or a config key, update this file in the same commit. The document
> it replaced drifted into describing seven files that no longer existed.

---

## The shape of the system

Three layers that never call each other directly. They communicate through **Kafka** (ingestion → the
rest) and **PostgreSQL** (validation → drift detection). The only synchronous coupling in the system is
the Postgres consumer's HTTP call to the Validator API.

```
                    ┌──────────────────┐
                    │  GitHub Events   │
                    │       API        │
                    └────────┬─────────┘
                             │ HTTP poll
                             ▼
┌────────────────────────────────────────────────────────────────┐
│ INGESTION                                                       │
│                                                                 │
│   github_producer ──────► Kafka topic: $KAFKA_TOPIC             │
│                                    │                            │
│                    ┌───────────────┴───────────────┐            │
│                    ▼                               ▼            │
│           postgres_consumer                 minio_consumer      │
│                    │                               │            │
└────────────────────┼───────────────────────────────┼────────────┘
                     │ POST /validate (sync, blocking)│
                     ▼                               │
┌────────────────────────────────────┐               │
│ VALIDATION  (FastAPI, real-time)   │               │
│                                    │               │
│   DataValidator                    │               │
│     └─ ValidationEngine            │               │
│          └─ 7 checks, fixed order  │               │
│                                    │               │
│   returns PASS / WARN / FAIL ──────┼───────┐       │
└────────────────────────────────────┘       │       │
                                             │       │
                     ┌───────────────────────┘       │
                     │ FAIL → drop, else INSERT      │
                     ▼                               ▼
        ┌─────────────────────────┐        ┌──────────────────┐
        │ PostgreSQL              │        │ MinIO            │
        │  github_events          │        │  raw/YYYY-MM-DD/ │
        │  github_events_processed│        │   HH-MM-SS-uuid  │
        │  drift_results          │        │   .json          │
        │  validation_results ⁽¹⁾ │        │                  │
        └───────────┬─────────────┘        └──────────────────┘
                    │ SELECT event_data BY processed_at
                    ▼
┌────────────────────────────────────────────────────────────────┐
│ DRIFT DETECTION  (batch, every 6h, fully decoupled)             │
│                                                                 │
│   DriftRunner                                                   │
│     ├─ baseline window: 7 days     ┐                            │
│     ├─ current window:  24 hours   ┘ → 3 profiles each          │
│     │                                                           │
│     ├─ SchemaProfile ──────► SchemaDriftDetector                │
│     ├─ StatisticalProfile ─► DistributionDriftDetector          │
│     └─ VolumeProfile ──────► VolumeDriftDetector                │
│                                       │                         │
│                                       ▼                         │
│                            DriftRepository → drift_results      │
└────────────────────────────────────────────────────────────────┘
```

⁽¹⁾ `validation_results` is defined by `ValidationRepository` but is not written on the running path —
the API constructs its validator with `enable_persistence=False`, so the table is never created. See
IMPROVEMENTS.md F17.

---

## Layer 1 — Ingestion (`ingestion/`)

Always-on streaming processes. Each is an independently runnable module with its own `main()`.

### `producers/github_producer.py`

Polls the GitHub Events API and publishes each event to Kafka keyed by `str(event["id"])`. Scheduling
uses the `schedule` library on a fixed interval, with one immediate run at startup.

Authentication is optional: if `GITHUB_TOKEN` is set it becomes a `Bearer` header, otherwise the poll is
unauthenticated and subject to the lower anonymous rate limit.

Interval configuration is currently split across two variables — `GITHUB_POLL_INTERVAL_SECONDS` is read
into `self.poll_interval` and unused, while `main()` reads `GITHUB_EVENTS_FETCH_INTERVAL`, which nothing
sets. The effective interval is therefore always the 60-second default (IMPROVEMENTS.md F23).

### `consumers/postgres_consumer.py`

Consumer group `github_events_postgres_consumer`, `auto.offset.reset=earliest`.

For each message, `store_event`:

1. **POSTs the event to the Validator API** and waits. This is the one synchronous cross-layer call in
   the system. It is *fail-closed*: if no validator responds, the event is dropped rather than stored.
2. Tries a chain of validator URLs — `VALIDATOR_URL`, then any in `VALIDATOR_FALLBACKS`, then hardcoded
   `localhost:8000` and `host.docker.internal:8000` — and remembers the first that answers.
3. On `FAIL`, drops the event. On `PASS` or `WARN`, inserts into **both** `github_events` (flat columns)
   and `github_events_processed` (whole event as JSONB), each `ON CONFLICT (event_id) DO NOTHING`.

`init_db()` creates both tables idempotently at startup.

### `consumers/minio_consumer.py`

Consumer group `github_events_minio_consumer`. Completely independent of the Postgres path — it does
**not** validate. Every raw event is written immutably to
`raw/YYYY-MM-DD/HH-MM-SS-{uuid}.json`, so MinIO holds events the Postgres path rejected.

MinIO is write-only backup. Nothing in the system reads from it.

---

## Layer 2 — Validation (`data_validation/`)

Real-time, synchronous, invoked over HTTP by the Postgres consumer.

### Entry point: `api.py`

A FastAPI app with two routes:

| Route | Method | Purpose |
|---|---|---|
| `/validate` | POST | Validate one event; returns status, event id, timing, failure list |
| `/health` | GET | Liveness probe (used by the compose healthcheck) |

A single module-level `DataValidator` is constructed at import with `enable_persistence=False` and
`enable_metrics=True`. Metrics are therefore collected on every request; there is no `/metrics` route to
read them (IMPROVEMENTS.md F16).

### `data_validator.py` — the facade

`DataValidator` wraps three things: a `ValidationEngine`, an optional `ValidationRepository`, and an
optional `PrometheusMetrics`. It is the intended public entry point; module-level `validate_event()` /
`validate_batch()` helpers wrap a lazily-created singleton.

### `engine/validator.py` — the orchestrator

`ValidationEngine` loads `rules/github_events.yaml`, instantiates one checker per rule section, and runs
them in a **fixed order, most critical first**:

```
1. Schema       required fields present
2. Type         int / str / bool / list / dict
3. Null         null and empty-value rules
4. Value        regex, enum, numeric range, length
5. Timestamp    ISO8601 parse, not-future, not-too-old
6. Consistency  cross-field: conditional requirements, all-or-none groups
7. Duplicate    in-memory set of seen ids
```

Each checker returns `List[ValidationFailure]`; the engine accumulates them into a `ValidationResult`
whose status is derived from the highest severity present:

| Any failure at severity | Resulting status |
|---|---|
| `CRITICAL` | `FAIL` — consumer drops the event |
| `WARNING` (no critical) | `WARN` — event is stored |
| `INFO` only, or none | `PASS` — event is stored |

The duplicate cache is an in-process `set` on the engine instance. It is not shared across processes and
not bounded — the YAML's `lookback_window_seconds` and `check_table` keys are not read
(IMPROVEMENTS.md F15).

### `checks/` — the checkers

Checkers are **duck-typed, not an ABC**. The contract is only:

```python
class MyChecker:
    def __init__(self, rules): ...                          # its own YAML section
    def validate(self, event) -> List[ValidationFailure]: ...
```

| Module | Exports |
|---|---|
| `schema.py` | `SchemaChecker`, plus the shared `get_nested_value` / `field_exists` helpers |
| `type_checks.py` | `TypeChecker` |
| `value_checks.py` | `ValueChecker`, `NullChecker` |
| `consistency_checks.py` | `ConsistencyChecker`, `TimestampChecker` |
| `drift_checks.py` | `DriftDetector` — superseded by `drift_engine/`, no callers |

> **Adding a check:** write the class, export it from `checks/__init__.py`, read its YAML section in
> `ValidationEngine._initialize_checkers`, and call it at the right point in `validate_event`'s fixed
> order. Four edits, no registration magic.

### `models/validation_result.py`

`ValidationFailure` (frozen dataclass) and `ValidationResult` (mutable, `add_failure` recomputes status).
Note `Severity` is an enum whose *values* are the status strings it maps to — `Severity.CRITICAL` has
value `"FAIL"`, `Severity.WARNING` has `"WARN"` — which is why severity and status read alike in logs
and JSON.

---

## Layer 3 — Drift detection (`drift_engine/`)

Periodic batch. Reads `github_events_processed` directly; shares no code path with the real-time layer.

### `drift_service.py` — the scheduler

Long-running process. Parses `execution.schedule_cron` from the config, runs `DriftRunner.run()` once
immediately at startup, then on the derived interval. Despite the key name this is not cron — the string
is parsed for a `*/N` hours pattern and handed to `schedule.every(N).hours`, anchored to process start
(IMPROVEMENTS.md F26). Failures are caught and logged; the service stays up.

### `engine/drift_runner.py` — the orchestrator

One `run()` performs:

1. **Window definition** — current = last `windowing.current.hours`; baseline = the preceding
   `windowing.baseline.days`, optionally separated by `gap_hours`.
2. **Fetch** — one `SELECT event_data FROM github_events_processed WHERE processed_at >= %s AND < %s`
   per window, fully materialized.
3. **Guard** — if either window has fewer than `profiling.min_sample_size` records, return an empty
   summary without running detectors.
4. **Profile** — three profiles per window, six total.
5. **Detect** — each enabled detector under `targets.*.enabled`.
6. **Persist** — only when at least one drift was found, via `DriftRepository.save_many` (one
   statement, one commit). Failures here are logged and **not** re-raised, so a run can report drifts
   it did not store.

### Profiles and detectors

Same duck-typed extension pattern as the checkers, paired one-to-one:

| Profile (`from_records`) | Detector (`detect`) | Signal |
|---|---|---|
| `SchemaProfile` | `SchemaDriftDetector` | field added / removed, type change, cardinality ratio |
| `StatisticalProfile` | `DistributionDriftDetector` | PSI on categoricals, mean shift on numericals, null-ratio change |
| `VolumeProfile` | `VolumeDriftDetector` | global event rate, per-entity counts |

Profiles expose `from_records(records, ...) -> Profile`; detectors expose
`detect(baseline, current, baseline_window, current_window) -> List[DriftResult]`.

> **Adding a drift type:** write a profile + detector pair, wire both into `DriftRunner` (construct the
> detector in `__init__`, build the profile and call the detector in `run`), and add a
> `targets.<name>_drift` section to `drift_config.yaml`.

The `distribution` thresholds include `ks_test.info_pvalue` / `warning_pvalue`, which are read into
attributes but never used — the numerical comparison is a mean-shift heuristic, not a KS test
(IMPROVEMENTS.md F22).

### `reports/report_generator.py`

`generate_text_report` is called by `drift_service` and printed to stdout after each run. The JSON,
Markdown, and `save_report` variants exist and have no callers.

---

## Persistence (`db/`)

The one shared abstraction in the codebase. Every component reaches PostgreSQL through it — there is no
second path, and keeping it that way is the point of the package.

```
db/config.py        DatabaseConfig — the only reader of POSTGRES_*
db/pool.py          ThreadedConnectionPool
                    ├── get_connection()   borrow / return
                    ├── transaction()      commit on exit, rollback on exception
                    ├── fetch_all()        materialized read
                    └── fetch_iter()       server-side cursor, streamed
db/schema.py        all DDL and indexes; init_schema()
db/errors.py        DatabaseError ├ ConfigurationError ├ ConnectionFailed ├ QueryFailed
db/repositories.py  EventRepository       → github_events, github_events_processed
                    DriftRepository       → drift_results
                    ValidationRepository  → validation_results
```

`transaction()` is the primitive underneath every write. It commits on clean exit and rolls back on any
exception, so a failed statement cannot leave a connection aborted for the next caller — which is what
used to happen, silently, for the life of the process.

Repositories hold **no connection state**: they borrow a cursor, emit SQL, and map rows. They take plain
dictionaries rather than domain objects, so `db/` imports nothing from `data_validation` or
`drift_engine`. Adapting values for JSONB (`psycopg2.extras.Json`) happens at that boundary rather than
in the models' `to_dict()`.

**Import from the `db` package root** — `from db import transaction, DriftRepository`. `__init__.py` is
the public surface; the submodules are an implementation detail.

Connections are pooled per process. A connection the server has terminated is discarded rather than
handed back, and every connection carries a `connect_timeout`, a `statement_timeout`, and an
`application_name` so it can be traced in `pg_stat_activity`.

All SQL is hand-written. There is no ORM and **no migration framework** — `init_schema()` creates what is
missing at startup, but `CREATE TABLE IF NOT EXISTS` will not alter an existing table, so changes to a
live database need a hand-applied migration. `scripts/migrate_001.sql` is the one that accompanied this
consolidation.

### Tables

All DDL lives in `db/schema.py`. Timestamps are `TIMESTAMPTZ` throughout: as naive `TIMESTAMP` they mixed
the server's local time (`DEFAULT now()`) with the application's UTC, shifting every drift window unless
the database ran in UTC.

| Table | Holds | Indexes |
|---|---|---|
| `github_events` | Flat columns: event/repo/actor/payload fields. Written by the consumer, **read by nothing** — it duplicates what `github_events_processed` already stores | `event_id` unique, `created_at` |
| `github_events_processed` | Whole event as JSONB + `validation_status`; **the drift engine's only source** | `event_id` unique, `processed_at` |
| `drift_results` | One row per detected drift: type, entity, field, both windows, metric, score, severity | `detected_at`, `severity`, `(drift_type, entity)` |
| `validation_results` | Per-event validation outcome, upserted on `event_id` so re-validation replaces rather than duplicates | `event_id` unique, `validation_ts`, `status` |

The only column the drift query filters on is `github_events_processed.processed_at`, which is indexed.
It was not, until this consolidation: `drift_results` had two rival `CREATE TABLE` definitions, and the
one the running path used created no indexes at all.

---

## Configuration

Three mechanisms, no unifying framework. Each is authoritative for a different kind of setting.

| Mechanism | Owns | Loaded by |
|---|---|---|
| **Environment variables** | Connection info, service URLs, credentials | `os.getenv`, `python-dotenv`; `.env` at repo root |
| **YAML files** | Validation rules; drift thresholds, windows, schedule | Read explicitly by app code at construction |
| **Constructor kwargs** | Programmatic overrides | `DataValidator(...)`, `DriftRunner(config_path=...)` |

The two YAML files:

- `data_validation/rules/github_events.yaml` — sections map one-to-one onto checkers: `schema`,
  `type_checks`, `null_checks`, `value_checks`, `timestamp_checks`, `consistency_checks`,
  `duplicate_check`. A trailing `drift_detection:` block is vestigial and unread.
- `drift_engine/config/drift_config.yaml` — `windowing`, `thresholds`, `targets`, `profiling`,
  `execution`, `database`, `observability`. Of these, `execution.parallel_workers`,
  `execution.timeout_seconds`, `database.table_name`, and all of `observability` are unread.
`database.batch_insert_size` is now the page size for the batched drift insert; it used to be read into
a local and then never referenced, while the code inserted one row at a time.

See the README for the environment variables themselves.

---

## Runtime topology

Every component is its own runnable module. There is **no unified CLI** — `docker-compose.yml` is what
wires them together, and all five application services build from the single root `Dockerfile`.

| Service | Command | Depends on |
|---|---|---|
| `kafka` | KRaft mode, single node | — |
| `postgres` | — | — |
| `minio` | `server /data` | — |
| `validator` | `uvicorn data_validation.api:app` | postgres, minio, kafka |
| `github-producer` | `python -m ingestion.producers.github_producer` | kafka |
| `postgres-consumer` | `python -m ingestion.consumers.postgres_consumer` | kafka, postgres |
| `minio-consumer` | `python -m ingestion.consumers.minio_consumer` | kafka, minio |
| `drift-detector` | `python -m drift_engine.drift_service` | postgres |

Each application `command:` is wrapped by `scripts/wait_and_run.py host:port [...] -- <command>`, a TCP
readiness gate that blocks until its dependencies accept connections, then execs the real command. This
is the only startup ordering mechanism — `depends_on` here means start-order, not readiness, and no
service uses `condition: service_healthy`.

Note that `postgres-consumer` calls the validator synchronously on every message but does not declare it
as a dependency, so on a cold start it exhausts the fallback URL chain until the validator is up.

Two entry points exist outside compose:

- `run_pipeline.py` — dev convenience. Runs producer, both consumers, and the drift service as
  asyncio subprocesses with colour-multiplexed logs. Expects infrastructure to already be running.
- `test_e2e.py` — an integration *script*, not a pytest test, despite the name. It drives
  `docker-compose up -d`, exercises the full pipeline, and tears down.

---

## Where things live

```
SentinelDQ/
├── ingestion/
│   ├── producers/github_producer.py       GitHub API → Kafka
│   └── consumers/
│       ├── postgres_consumer.py           Kafka → validate → PostgreSQL
│       └── minio_consumer.py              Kafka → MinIO (raw backup)
│
├── data_validation/
│   ├── api.py                             FastAPI: /validate, /health
│   ├── data_validator.py                  Facade: engine + persistence + metrics
│   ├── engine/validator.py                ValidationEngine, fixed check order
│   ├── checks/                            One module per check family
│   ├── models/validation_result.py        ValidationResult, ValidationFailure, Severity
│   ├── metrics/prometheus.py              Hand-rolled exposition (not routed)
│   └── rules/github_events.yaml           Validation rules
│
├── drift_engine/
│   ├── drift_service.py                   Scheduler, service entry point
│   ├── engine/drift_runner.py             Windowing, fetch, profile, detect, persist
│   ├── profiles/                          schema / statistical / volume
│   ├── detectors/                         schema / distribution / volume
│   ├── models/drift_result.py             DriftResult, DriftSummary, TimeWindow
│   ├── reports/report_generator.py        Text report (JSON/MD unused)
│   └── config/drift_config.yaml           Thresholds, windows, schedule
│
├── db/
│   ├── __init__.py                        Public surface — import from here
│   ├── config.py                          DatabaseConfig; the only POSTGRES_* reader
│   ├── pool.py                            Pool, transaction(), fetch_all/fetch_iter
│   ├── schema.py                          All DDL and indexes; init_schema()
│   ├── repositories.py                    Event, Drift, Validation repositories
│   └── errors.py                          DatabaseError hierarchy
│
├── scripts/wait_and_run.py                TCP readiness gate for containers
├── scripts/migrate_001.sql                One-shot migration for pre-consolidation DBs
├── tests/                                 Unit tests (ingestion + db only)
├── run_pipeline.py                        Local multi-process dev runner
├── test_e2e.py                            Integration script (not a pytest test)
├── docker-compose.yml                     Service topology
├── Dockerfile                             Single image, all services
├── ARCHITECTURE.md                        This file
├── IMPROVEMENTS.md                        Audit findings and refactoring roadmap
└── README.md                              Setup and usage
```

---

## Design decisions worth knowing

**Layers are decoupled through infrastructure, not interfaces.** Ingestion does not import validation;
validation does not import drift detection. They meet at a Kafka topic and a Postgres table. The one
exception — the consumer's HTTP call to the validator — is deliberate, because validation must be
inline to keep bad events out of the table the drift engine trusts.

This held everywhere except `db/`, which imported both `drift_engine.models` and
`data_validation.models` to type its `save()` signatures — so `import data_validation` transitively
pulled in `drift_engine`, and the persistence layer could not be used without the layers above it. The
repositories now take plain dictionaries, and the dependency runs one way.

**Fail-closed on validation.** If the validator cannot be reached, events are dropped rather than
stored unvalidated. This protects the drift engine's input at the cost of availability.

**Duck typing over base classes.** Neither checkers nor profiles/detectors inherit from an ABC. Adding
one means writing a class with the right method and wiring it in one place. There is no registry,
plugin loader, or entry-point scanning.

**Two write paths, deliberately asymmetric.** MinIO gets everything, immutably, unvalidated. PostgreSQL
gets only what passes. MinIO is the replay source of record; Postgres is the queryable one.

**Runtime DDL instead of migrations.** Every component creates its own tables idempotently on startup.
Adding a column means editing the `CREATE TABLE IF NOT EXISTS` — which will not alter an existing table.
Schema changes to a live database must be applied by hand.

---

## Known issues

This document describes the system as it runs. It does not evaluate it. For defects, performance
bottlenecks, dead code, and the phased plan to address them, see **[IMPROVEMENTS.md](IMPROVEMENTS.md)** —
29 findings, ordered by consequence, each with acceptance criteria.

The cross-references above (F10, F11, F15, F16, F17, F22, F23, F26) point into that document.

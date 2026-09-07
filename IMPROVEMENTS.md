# SentinelDQ — Improvement Roadmap

An architectural and performance audit of the codebase at `production-refinements` (`bf52455`), written
as a phased refactoring plan. Every finding below cites `file:line` and was verified against the code;
where a finding was confirmed by running something rather than reading it, that is stated.

**Scope note:** this is no longer diagnosis-only. Findings marked ✅ *fixed* have been applied — F1
(the missing import), F2 (the hanging suite), and the persistence layer: F7, F10 (partly), F11, F17, and
the `db/` rows of F21. **F30–F39**, in the addendum before *Suggested sequencing*, were found during that
work and were not in the original 29; F30 in particular is more consequential than anything F7–F11
described. Everything unmarked is still diagnosis.

---

## Executive summary

Three things matter more than everything else here.

**1. The drift engine has been dead since the `db/` consolidation.** `drift_runner.py` called
`PersistenceService(...)` without importing it, so every run raised `NameError` — and `drift_service.py`
catches and logs the exception, so the service stayed up, on its 6-hour schedule, doing nothing. No
drift has been detected or persisted on this branch. *(F1 — fixed, with a regression test.)*

**2. Nothing caught it, because the test suite cannot finish.** Two of four test files hang forever on a
live `confluent_kafka.Consumer` — the patches miss because both modules bind the name at import — a third
assertion is stale and fails, and `test_e2e.py` sits at the repo root inside pytest's default collection
glob, so a bare `pytest` tries to run `docker-compose up`. The suite has never been a gate.
*(F2 — fixed; F3.)*

**3. The persistence layer had no error handling at all.** The repositories ran `execute` then `commit`
with no `try`/`except` and no rollback, so one failed insert left the shared connection aborted and every
later write failed identically — while the callers swallowed it with `print()`. On top of that the
ingestion hot path opened a fresh connection *and* a fresh TCP connection to the validator per message.
*(F30, F7, F8, F10 — the connection work is fixed.)*

Underneath those: the drift statistics that *would* have run compare a 7-day window against a 24-hour
window without normalizing for duration, so they would have produced a false-positive flood on the first
successful run (**F4**). And the validator's Prometheus metrics are collected on every request and
exposed nowhere (**F16**).

---

## Findings index

| # | Finding | Phase | Severity |
|---|---|---|---|
| F1 | Drift engine `NameError` on every run | 0 | **Critical** — *fixed* |
| F2 | Two test files hang forever; one assertion stale | 0 | **Critical** — *fixed* |
| F3 | `test_e2e.py` collected by bare `pytest` | 0 | High |
| F4 | Per-entity volume drift ignores window duration | 1 | **Critical** |
| F5 | Prometheus histogram double-counts, invalid exposition | 1 | High |
| F6 | Naive-UTC vs DB-local timestamp comparison | 1 | High |
| F7 | New PostgreSQL connection per event | 2 | **Critical** |
| F8 | New TCP connection to validator per event | 2 | **Critical** |
| F9 | Kafka offsets auto-commit → silent data loss | 2 | **Critical** |
| F10 | No batching anywhere (Postgres, MinIO, drift) | 2 | High |
| F11 | Missing indexes on the only filtered columns | 2 | High |
| F12 | Drift profiling unbounded in memory, O(records × fields) | 2 | High |
| F13 | Validator serves one request at a time | 2 | High |
| F14 | Producer republishes the same events every cycle | 2 | Medium |
| F15 | Unbounded duplicate cache | 3 | High |
| F16 | Metrics collected and never exposed | 3 | Medium |
| F17 | `validation_results` never written | 3 | Medium |
| F18 | Shared mutable state with no lock | 3 | Medium |
| F19 | `_parse_severity` duplicated seven times | 4 | Low |
| F20 | Four separate nested-field extractors | 4 | Low |
| F21 | Dead code (~450 LOC) | 4 | Low |
| F22 | Unused dependencies; docstrings describe absent statistics | 4 | Medium |
| F23 | Dead configuration keys | 4 | Low |
| F24 | Config contradicts runtime behaviour | 1 | High |
| F25 | Regex rules collide on field name | 4 | Medium |
| F26 | `schedule_cron` is not cron | 4 | Low |
| F27 | Environment and packaging drift | 4 | Medium |
| F28 | Zero coverage of validation and drift packages | 5 | High |
| F29 | Diagnostics: bare excepts, `print()`, broken log counter | 4 | Low |

---

# Phase 0 — Restore the pipeline

**Goal:** the drift engine runs, and `pytest` becomes a gate that can actually fail.

Nothing else on this roadmap is verifiable until the suite terminates. Do this first.

### F1 — Drift engine raises `NameError` on every run ✅ *fixed*

`drift_engine/engine/drift_runner.py:241` calls `PersistenceService(...)`, but the imports at `:11-17`
never brought it in — it was left behind when persistence moved into `db/` (commits `9bbba03`,
`bf52455`). `drift_service.py:59` catches the exception and logs it, so the service stayed up and
silently produced nothing every 6 hours.

**Applied:** added `from db.persistence import PersistenceService` to `drift_runner.py:18`, plus
`tests/test_drift_runner.py`, which drove `_fetch_data` against a fake connection and asserted it
returned the rows and issued the expected query. (That file was later folded into
`tests/test_db_pool.py` and `tests/test_db_repositories.py` when `PersistenceService` was replaced by
the pool; `drift_runner` now calls `db.fetch_iter`.) Confirmed by execution: the call now fails with
`ConnectionError` against a local Postgres rather than `NameError` — i.e. it reaches the DB layer.

### F2 — The test suite cannot run to completion ✅ *fixed*

`tests/test_postgres_consumer.py` and `tests/test_minio_consumer.py` construct a **real**
`confluent_kafka.Consumer` against `localhost:9092` in `PostgresConsumer.__init__` /
`MinIOConsumer.__init__`. Only one test in each file patches `confluent_kafka.Consumer`
(`test_postgres_consumer.py:95`); the rest let librdkafka spawn a background thread that keeps the
process alive after pytest has finished reporting.

*Confirmed by execution:* run per file with a 22-second budget — `test_postgres_writer.py` and
`test_github_producer.py` complete in under 0.1 s; the other two were still running at 22 s and had to
be killed.

Separately, `test_init_db` **fails on its assertions**, and has since the processed-events table was
added:

- `mock_cursor.execute.assert_called_once()` — but `init_db` now issues *two* DDL statements
  (`postgres_consumer.py:83-84`).
- `mock_connection.commit.assert_called_once()` — psycopg2's connection context manager commits on
  `__exit__`, which a `MagicMock` never does.

Note *why* the existing patches miss: both consumer modules do `from confluent_kafka import Consumer`,
which binds the name at import time, so patching `confluent_kafka.Consumer` has no effect on the
already-bound reference.

**Applied:** patched at the module-bound name (`ingestion.consumers.<module>.Consumer`) — in `setUp` for
the MinIO tests, per-test for the Postgres ones. `pytest tests/` now completes in well under a second.
The stale `test_init_db` assertions are gone: DDL moved to `db/schema.py`, so the test asserts the
consumer delegates to `init_schema()` rather than counting `execute` calls.

### F3 — `test_e2e.py` is collected by a bare `pytest`

It sits at the repo root and matches pytest's default `test_*.py` glob. There is no `pytest.ini`,
`setup.cfg`, or `pyproject.toml` to scope collection, so `pytest` at the root collects it and it shells
out to `docker-compose up -d` (`test_e2e.py:114`) and `docker-compose down` (`:111`, `:430`).

**Fix:** add a `pyproject.toml` with `[tool.pytest.ini_options]` and `testpaths = ["tests"]`. Keep
`test_e2e.py` as the manually-invoked integration script it is — or rename it `e2e_check.py` so the glob
can never reach it.

### Acceptance criteria

- [ ] `pytest` at the repo root exits **0**, in under 30 seconds, with no Docker daemon running.
- [ ] `pytest` collects zero tests from `test_e2e.py` (`pytest --collect-only -q` confirms).
- [ ] No test constructs a live `confluent_kafka.Consumer` — `grep -L "patch.*Consumer" tests/test_*consumer*.py` returns nothing.
- [x] `pytest tests/` passes, and reverting the `drift_runner.py` import made it fail with `NameError`. (The dedicated file has since been folded into the `db/` test modules.)

**Effort:** ~half a day. **Risk:** very low — test-only, plus one import.

---

# Phase 1 — Correctness of the drift signal

**Goal:** when the drift engine runs (Phase 0), what it reports is true.

These are live bugs that were masked by F1. Fixing Phase 0 without Phase 1 turns a silent engine into a
noisy one.

### F4 — Per-entity volume drift ignores window duration

`volume_drift.py:191-211` compares a raw baseline count against a raw current count. But the baseline
window is **7 days** and the current window is **24 hours** (`drift_config.yaml`, `windowing`). The
global path gets this right — it divides both by `duration_hours()` (`volume_drift.py:101-102`) before
comparing. The per-entity path does not.

Consequence: an entity with a perfectly steady rate produces
`percent_change = (24h_count - 7d_count) / 7d_count ≈ -0.857`, which clears the 0.50 `warning`
threshold. **Every entity present in both windows flags as a ~86% decrease, every run**, up to
`top_n_entities: 50` per dimension across 3 dimensions.

The same class of bug is at `schema_drift.py:143`: `card_ratio = current_card / baseline_card` compares
cardinality observed over 24 hours against cardinality over 7 days. That one fails in the opposite
direction — the ratio is almost always well below 1.0, so `cardinality_explosion` never fires and the
check is effectively dead.

**Fix:** normalize both to per-hour rates, exactly as `_detect_global_volume_drift` already does. For
cardinality, a raw ratio across unequal windows is not meaningful at all — either compare equal-length
windows or drop the check rather than leave it permanently silent.

### F5 — Prometheus histogram double-counts

`prometheus.py:97-99` increments *every* bucket whose bound is ≥ the observation:

```python
for bucket in self.duration_buckets:
    if duration_seconds <= bucket:
        self.duration_counts[bucket] += 1
```

That already makes `duration_counts[b]` the cumulative count of observations ≤ `b`, which is exactly
what Prometheus wants. But `:170-173` then accumulates those cumulative values *again*:

```python
cumulative += self.duration_counts[bucket]
lines.append(f'..._bucket{{le="{bucket}"}} {cumulative}')
```

Consequence: `_bucket` values grow super-linearly and exceed the `+Inf` bucket
(`:174`, correctly `duration_count`). A histogram whose buckets exceed `+Inf` is invalid exposition and
scrapers reject it.

**Fix:** emit `duration_counts[bucket]` directly and delete the `cumulative` accumulator.

### F6 — Naive-UTC compared against DB-local time

`github_events_processed.processed_at` is `TIMESTAMP DEFAULT NOW()` (`postgres_consumer.py:76`) —
`NOW()` returns the value in the *server session's* timezone, stored without offset. The drift query
filters that column using Python's `datetime.utcnow()` (`drift_runner.py:83`, via `_define_windows`),
which is naive UTC.

Consequence: whenever the database's timezone is not UTC, both windows silently shift by the offset —
selecting the wrong rows with no error. The same naive `utcnow()` appears at `validation_result.py:149`,
`drift_result.py:74`, and `minio_consumer.py:94`.

**Fix:** make the column `TIMESTAMPTZ` and use `datetime.now(timezone.utc)` throughout. This is also a
forward-compatibility fix — `datetime.utcnow()` is deprecated and emits a `DeprecationWarning` on the
Python 3.14 in the local `.venv` (see F27).

### F24 — Configuration contradicts runtime behaviour

Two mismatches where the YAML promises something the code cannot deliver:

**Distribution drift covers only one field.** `drift_config.yaml` lists
`categorical_fields: [type, actor.login, repo.name]`, but `categorical_max_categories: 100` combined
with `statistical_profile.py:87-94` means any field exceeding 100 distinct values is skipped with a
warning. On real GitHub event volume, `actor.login` and `repo.name` will *always* exceed it. Two of the
three configured fields are permanently inert.

**PSI threshold names are inverted against the severities they emit.** At
`distribution_drift.py:94-99`, crossing `psi.warning` (0.25) emits `Severity.CRITICAL`, and crossing
`psi.info` (0.1) emits `Severity.WARNING`. Someone tuning `psi.warning` is actually tuning the critical
alert.

**Fix:** for high-cardinality fields, profile the top-N categories plus an explicit `__other__` bucket
rather than skipping the field. Rename the threshold keys to match what they emit (`psi.critical`,
`psi.warning`), or fix the mapping — either way, make them agree.

### Acceptance criteria

- [ ] A test where baseline and current windows carry **identical per-hour rates** over 7 days and 24 hours produces **zero** volume drift results.
- [ ] A test where the current window's rate is genuinely double the baseline's still produces a drift result — the normalization did not simply mute the detector.
- [ ] Histogram test: for observations spread across buckets, `_bucket` values are monotonically non-decreasing and every one is ≤ the `+Inf` value.
- [ ] `promtool check metrics < metrics.txt` accepts the exposition output.
- [ ] Drift window selection returns identical rows with the database session set to `UTC` and to `America/New_York`.
- [ ] `actor.login` produces a distribution profile (top-N + `__other__`) rather than a skip warning.

**Effort:** ~2 days. **Risk:** medium — changes what the system reports. Land Phase 5 tests for these
detectors alongside, or you are trading one unverified signal for another.

---

# Phase 2 — The hot path

**Goal:** ingestion throughput bounded by work done, not by connection setup.

### F7 — A new PostgreSQL connection per event ✅ *fixed*

`postgres_consumer.py:143` opens `psycopg2.connect(**self.db_config)` **inside** `store_event`, which is
called once per Kafka message (`:198`). Connection establishment — TCP, TLS, auth, backend fork — is
paid per event, and dwarfs the single-row INSERT it wraps. `init_db` (`:81`) opens yet another.

Note the connection is never explicitly closed either: psycopg2's context manager commits or rolls
back the *transaction* on exit, but leaves the connection open. Whether that accumulates or is reclaimed
promptly by refcounting was **not** established — measure `pg_stat_activity` under load before treating
it as a leak rather than as churn.

**Applied:** a module-level `ThreadedConnectionPool` in `db/pool.py`. The consumer buffers events and
writes them in batches through `EventRepository`, so connection setup is no longer per message.

### F8 — A new TCP connection to the validator per event

`postgres_consumer.py:101` uses bare `requests.post` — no `Session`, so no connection pooling and no
HTTP keep-alive. One TCP handshake per event, on top of F7's database handshake.

Worse, the fallback URL list is rebuilt from `os.getenv` on every call (`:91-95`), and when the validator
is unreachable, each message tries **four** URLs — the configured one, any `VALIDATOR_FALLBACKS`, then
hardcoded `localhost:8000` and `host.docker.internal:8000` — at `VALIDATOR_TIMEOUT` (default 0.5 s)
apiece before being dropped. A validator outage costs ~2 s per message while dropping every one.

**Fix:** a `requests.Session` created once in `__init__`; hoist the fallback list into `__init__`; add a
circuit breaker so a sustained outage fails fast instead of re-probing four endpoints per message.

### F9 — Kafka offsets auto-commit

Neither consumer sets `enable.auto.commit=False`, so librdkafka's default (`true`, every 5 s) applies.
Offsets therefore advance on a timer, independent of whether the message was successfully stored.

`store_event` returns early when the validator is unreachable (`:116`) or when validation fails
(`:120`), and re-raises on a database error (`:172`) which `start_consuming` catches and continues past
(`:199-201`). In every one of those paths, the offset still advances. **Events are silently lost with no
dead-letter path and no counter.**

**Fix:** `enable.auto.commit=False` and an explicit `consumer.commit(msg)` after a confirmed write.
Route validation failures and unreachable-validator drops to a dead-letter topic rather than `return`.

### F10 — Nothing is batched ✅ *partly fixed*

- **Postgres:** one INSERT + one implicit commit per event (`postgres_consumer.py:143-168`).
- **MinIO:** one `put_object` per event (`minio_consumer.py:112`) — one HTTP round trip per message.
- **Drift results:** `drift_runner.py:313-314` loops `repository.save(result)`, and each `save`
  commits (`db/repositories.py:64`). It reads `batch_insert_size` at `:299` and then never uses it.

That last one is pure waste: `DriftPostgresWriter.write_results` (`db/postgres_writer.py:64-93`)
**already implements** exactly this with `execute_batch` and a single commit — and is called by nothing
but a test.

**Applied:** Postgres writes are batched with `execute_values` over a poll window
(`INGEST_BATCH_SIZE`, `INGEST_FLUSH_INTERVAL`), and `_persist_results` calls
`DriftRepository.save_many` — one statement and one commit — which finally gives
`database.batch_insert_size` an effect.
**Remaining:** MinIO still issues one `put_object` per event.

### F11 — Missing indexes on the only columns that are filtered ✅ *fixed*

`github_events_processed` is created with `event_id TEXT UNIQUE` and nothing else
(`postgres_consumer.py:71-79`). The drift query filters exclusively on `processed_at`
(`drift_runner.py:236`) — an unindexed column. **Every 6-hour drift run sequentially scans the entire
table**, and the table grows without bound.

`DriftRepository.ensure_table_exists` (`db/repositories.py:74-97`) creates `drift_results` with **no
indexes** — while its near-duplicate `DriftPostgresWriter.ensure_table_exists`
(`db/postgres_writer.py:48-50`) creates three (`detected_at`, `severity`, `drift_type, entity`).
`DriftRunner` uses the repository, so production gets the un-indexed table. `validation_results` has no
indexes either (`db/repositories.py:20-34`).

**Applied:** all DDL moved to `db/schema.py`, so there is one definition per table rather than two.
`github_events_processed(processed_at)` is indexed, the three drift indexes are in the definition the
running path uses, and `validation_results` has `event_id` unique plus `validation_ts` and `status`.
Because `CREATE TABLE IF NOT EXISTS` cannot alter an existing table, databases created before this need
`scripts/migrate_001.sql`.

### F12 — Drift profiling is unbounded in memory and O(records × fields)

`_fetch_data` (`drift_runner.py:223-257`) issues `SELECT event_data FROM github_events_processed WHERE
processed_at >= %s AND processed_at < %s` with **no `LIMIT`**, `fetchall()`s the entire result, and
materializes every row as a Python dict. For the baseline that is 7 days of every event ever ingested.
Both windows are held simultaneously (`:94-95`).

Then `run()` walks each dataset three times, once per profile (`:130-137`). And within them:

- `StatisticalProfile.from_records` (`statistical_profile.py:67-113`) loops **all records once per
  configured field**, twice over — once for categorical, once for numerical — building a full
  intermediate `values` list each time, then `sorted()` on it.
- `VolumeProfile.from_records` (`volume_profile.py:51-63`) does the same, once per entity field.
- `SchemaProfile.from_records` (`schema_profile.py:60-78`) calls `_flatten_dict` per record, which
  recursively rebuilds a dict for every nested object, and retains a `set` of up to
  `max_cardinality_track: 1000` distinct string values **per discovered field path** — and GitHub
  payloads flatten to hundreds of paths.

**Fix:** a server-side (named) cursor and a single streaming pass that updates all three profiles
incrementally, so memory is bounded by profile state rather than by window size. Better still: push the
aggregation into SQL — counts and distributions over JSONB are what Postgres is for.

### F13 — The validator serves one request at a time

`api.py:18` declares `async def validate(...)` around `_validator.validate_event(...)`, which is
entirely synchronous CPU-bound work (regex, dict traversal). An `async def` endpoint runs **on the event
loop**, so each request blocks the loop for its full duration — FastAPI's threadpool offload only
applies to plain `def` handlers. Uvicorn runs a single worker (`docker-compose.yml`, validator
`command:`), so the service has no concurrency at all.

**Fix:** change `async def` to `def` and FastAPI will run it in a threadpool. Then add workers. Note the
ordering dependency: doing this **exposes F18** — the shared singleton's mutable state becomes a real
race the moment requests genuinely overlap. Fix F18 in the same change.

### F14 — The producer republishes the same events every cycle

`github_producer.py:34` calls `requests.get(self.api_url, headers=self.headers)` with:

- **No `ETag` / `If-None-Match`.** The GitHub Events API returns an `ETag`, and a conditional request
  that 304s **does not count against the rate limit**. Every poll currently spends quota re-fetching
  data it already has.
- **No `since` / high-water mark.** The endpoint returns the most recent ~30 events; at a 60-second
  poll, consecutive responses overlap heavily. Those duplicates are republished to Kafka, consumed,
  validated, and dropped — and they are what feeds the unbounded cache in F15.
- **No `timeout=`.** A hung connection stalls the producer indefinitely with no recovery.

**Fix:** store the `ETag` and send `If-None-Match`; track the last-seen event id and publish only newer
events; pass an explicit `timeout`.

### Acceptance criteria

- [ ] A repeatable local load script (fixed Kafka backlog, timed drain) is committed, with a **before** number recorded in this document.
- [ ] Sustained ingestion throughput improves against that baseline by a stated target, measured with the same script.
- [ ] `EXPLAIN (ANALYZE)` on the drift window query shows an **Index Scan** on `processed_at`, not a Seq Scan.
- [ ] Connection count is flat under load: `SELECT count(*) FROM pg_stat_activity` does not grow with events processed.
- [ ] Killing the consumer mid-batch and restarting it reprocesses the uncommitted messages — no gap in `github_events`.
- [ ] A drift run over a synthetic 7-day window holds resident memory under a stated ceiling (measured with `tracemalloc` or RSS sampling).
- [ ] The validator handles N concurrent `/validate` requests with throughput scaling roughly linearly to the worker count, not flat.
- [ ] A second producer poll with an unchanged upstream `ETag` publishes **zero** Kafka messages.

**Effort:** ~1 week. **Risk:** medium-high — this is the live data path. F9 in particular changes
delivery semantics; land it with the restart test above.

---

# Phase 3 — Memory and unreachable functionality

**Goal:** the service survives a long uptime, and what it measures is visible.

### F15 — Unbounded duplicate cache

`validator.py:92` defaults `duplicate_cache` to a plain `set()`, and `_check_duplicate` only ever
**adds** to it (`:300`). There is no eviction — `:302` is an explicit `TODO` about exactly this.

The cache lives in the long-running validator API process (a module-level singleton, `api.py:9`), and it
is fed by a producer that re-emits the same events every 60 seconds (F14). It grows for as long as the
process lives.

The rules file already specifies the correct behaviour and is ignored: `github_events.yaml`'s
`duplicate_check` block defines `lookback_window_seconds: 3600` and `check_table:
"github_events_raw"` — neither key is ever read. `validator.py:277-287` reads only `key_field`,
`severity`, and `error_message`.

**Fix:** a TTL cache keyed on `lookback_window_seconds` — an `OrderedDict` of `id → timestamp` pruned on
insert is sufficient and needs no dependency. If cross-restart correctness matters, the `ON CONFLICT
(event_id) DO NOTHING` already in `postgres_consumer.py:130` is the durable dedupe; the in-memory cache
is only an optimization and should be honest about that.

### F16 — Metrics are collected and never exposed

`PrometheusMetrics` records every validation (`data_validator.py:122-123`), and
`metrics_endpoint()` exists and is exported (`prometheus.py:291`, `metrics/__init__.py:9`). `api.py`
declares exactly two routes: `/validate` and `/health`. There is **no `/metrics` route.**

Roughly 300 lines of metrics collection run on the hot path of every request and are unreachable. The
module docstring at `:298-301` even shows the Flask wiring that was never done for FastAPI.

**Fix:** add `@app.get("/metrics")` returning `Response(metrics_endpoint(), media_type="text/plain")`.
Fix F5 first, or the endpoint will serve invalid exposition. Consider adopting the real
`prometheus_client` library rather than maintaining a hand-rolled exposition formatter — it is not
currently a dependency, but it would delete this whole module.

### F17 — `validation_results` is never written ✅ *fixed*

`api.py:9` constructs `DataValidator(enable_persistence=False)`, and `:20` additionally passes
`persist=False`. `ValidationRepository` (`db/repositories.py:12-64`) is therefore never instantiated on
the production path, `ensure_table_exists` never runs, and **the `validation_results` table is never
even created.** Only the status string is returned to the caller.

Per-event failure detail — which check failed, with what expected/actual values — is computed on every
request (`validator.py:186-218`) and discarded.

**Applied:** wired up. The write is a single pooled upsert keyed on `event_id`, so re-validation
replaces rather than duplicates, and it does not recreate F7 because connections are pooled. It is still
synchronous on the request path — the consumer calls this endpoint with a 0.5s timeout and drops events
fail-closed when it expires, so `VALIDATOR_PERSIST=false` takes the write off that path if latency ever
becomes the binding constraint. Batching it asynchronously is the next step if so.

### F18 — Shared mutable state with no lock

The module-level `_validator` singleton mutates state on every request:
`engine.stats` (`validator.py:307-318`), `engine.duplicate_cache` (`:300`), and every counter in
`PrometheusMetrics.record_validation` (`prometheus.py:76-114`, including a list slice at `:113-114`).

None of it is guarded. This is safe **today only by accident** — F13 forces every request onto the event
loop serially. Fix F13 and these become live races: lost counter increments, and a `recent_failures`
list mutated concurrently.

**Fix:** guard the mutable state with a `threading.Lock`, or make the counters atomic by moving to
`prometheus_client` (which handles this). Must land in the same change as F13, not after it.

### Acceptance criteria

- [ ] Feeding 100k events of which 99% are duplicates leaves the duplicate cache bounded at a size derived from `lookback_window_seconds`, not from event count. Assert on `len(cache)`.
- [ ] Process RSS is flat across a sustained duplicate-heavy run (sampled, not eyeballed).
- [ ] `curl localhost:8000/metrics` returns `200` and `promtool check metrics` accepts it.
- [ ] `lookback_window_seconds` and `check_table` are either read by the code or removed from the YAML — no key remains that only looks configured.
- [ ] A concurrency test — N threads × M validations — ends with `total_validated == N*M` exactly.
- [ ] A written decision on `validation_results`: wired up, or removed along with `ValidationRepository`.

**Effort:** ~3 days. **Risk:** low, except F18, which is coupled to F13.

---

# Phase 4 — Consolidation

**Goal:** delete what does not run; unify what is written six times.

Nothing here changes behaviour. Do it after Phase 5 gives you the tests to prove that.

### F19 — `_parse_severity` duplicated seven times

The identical method, wrapping the identical `severity_map` dict, appears at:
`value_checks.py:233` (`ValueChecker`) and `:322` (`NullChecker`), `type_checks.py:141`,
`schema.py:156`, `consistency_checks.py:138` (`ConsistencyChecker`) and `:315` (`TimestampChecker`),
and `validator.py:342`.

Seven copies of the same six-line mapping. A new severity level means seven edits, and any drift
between them is silent.

**Fix:** one `parse_severity(str) -> Severity` in `data_validation/models/validation_result.py`, beside
the `Severity` enum it returns.

### F20 — Four separate nested-field extractors

Dot-notation traversal of nested dicts is implemented independently at:

- `data_validation/checks/schema.py:11` — `get_nested_value`, the most complete version
- `drift_engine/profiles/statistical_profile.py:142` — `_extract_field`
- `drift_engine/profiles/volume_profile.py:72` — `_extract_field`, byte-identical to the above
- `drift_engine/profiles/schema_profile.py:107` — `_flatten_dict`, the inverse operation

The two `_extract_field` copies are the same function. Note they are not *quite* equivalent to
`get_nested_value` — both bail on the first `None`, conflating "absent" with "present and null", which
is precisely the distinction `field_exists` (`schema.py:40`) was written to preserve.

**Fix:** one shared utility. Choose the semantics deliberately, since the profiles' null-ratio
statistics depend on which one they get.

### F21 — Dead code (~630 LOC)

| Module | LOC | Status |
|---|---|---|
| `data_validation/checks/drift_checks.py` | 291 | `DriftDetector` is exported from `checks/__init__.py:16`; `SchemaDriftDetector` (`:153`) and `DistributionDriftDetector` (`:201`) are defined but not exported — and both names **collide** with the live classes in `drift_engine/detectors/`. **Zero callers** for all three. Superseded by `drift_engine/`; `CLAUDE.md` already warns not to confuse the two. |
| ~~`db/base.py`~~ | 5 | ✅ *deleted.* The shim aliased `PersistenceService` as `PostgresWriter` — a name promising a writer, for a class with no `save`. |
| ~~`db/database.py`~~ | 9 | ✅ *deleted.* `DatabaseService = PersistenceService`, zero importers. |
| ~~`db/postgres_writer.py`~~ | 144 | ✅ *deleted*, after moving `write_results` into `DriftRepository.save_many` and its read methods onto the same class. |
| `report_generator.py:131,164,234` | ~120 | `generate_json_report`, `generate_markdown_report`, `save_report` — only `generate_text_report` is called (`drift_service.py:43`). |
| `value_checks.py:337,375` | ~60 | `validate_regex`, `validate_enum` — module-level helpers, no callers. |

**Applied (db/ rows only):** the three `db/` modules are gone, with the batch insert preserved.
**Remaining:** `drift_checks.py`, the unused report generators, and the `value_checks.py` helpers.

### F22 — Unused dependencies; docstrings describe statistics that do not exist

`requirements.txt` pins `scipy>=1.11.0` and `numpy>=1.24.0`. **Neither is imported anywhere** —
confirmed by grep across all 50 Python files. Together they add roughly 120 MB to every image built from
the single root `Dockerfile`, for all five application services.

They were presumably added for the statistics the docstrings promise but the code does not implement:

- `distribution_drift.py:22` documents "KS test for numerical distributions". The implementation
  (`:125-165`) is a mean-shift heuristic — `abs(current_mean - baseline_mean) / baseline_std` against
  hardcoded thresholds of 1.0/2.0/3.0. The configured `ks_test.info_pvalue` / `warning_pvalue` are read
  into `self.ks_info_pvalue` (`:35-36`) and **never used**. There is no KS test.
- `volume_drift.py:120` fabricates its own standard deviation when no history is available:
  `std_rate = baseline_rate * 0.1`. The resulting "z-score" is therefore exactly `10 ×
  percent_change` — a relabeled percentage, not a statistic. It is then compared against a z-threshold
  of 2.0, which is identical to the 0.20 percent threshold on the next line.

**Fix:** either implement the real tests with `scipy.stats.ks_2samp` and keep the dependencies, or drop
`scipy`/`numpy` and correct the docstrings and config keys to describe the heuristics actually in use.
Do not leave the current gap, where an operator tuning `ks_test.warning_pvalue` changes nothing.

### F23 — Dead configuration keys

Read by nothing:

- `drift_config.yaml` — `execution.parallel_workers` (profiling is single-threaded),
  `execution.timeout_seconds` (no timeout anywhere), `database.table_name` (repositories hardcode it),
  the entire `observability:` block (`emit_metrics`, `log_level`, `log_all_comparisons`).
- `github_events.yaml` — the entire `drift_detection:` block, correctly marked "To be implemented in
  Phase 2"; that work happened in `drift_engine/` and this block was never removed.
- `duplicate_check.lookback_window_seconds`, `duplicate_check.check_table` — see F15.
- `distribution.ks_test.*` — see F22.

And one that is worse than dead: `github_producer.py:21` reads `GITHUB_POLL_INTERVAL_SECONDS` into
`self.poll_interval`, which is **never used**. `main()` instead reads `GITHUB_EVENTS_FETCH_INTERVAL`
(`:69`) — a variable that `docker-compose.yml` does not set, while it *does* set
`GITHUB_POLL_INTERVAL_SECONDS`. **The documented, configured, plumbed-through interval has no effect;
the producer always runs at the 60-second default.**

**Fix:** delete the dead keys. Settle on one interval variable and make docker-compose set the one the
code reads.

### F25 — Regex rules collide on field name

`value_checks.py:47` compiles patterns into `self._compiled_patterns[field]`, keyed by **field name
alone**. `_check_regex` then looks up by the same key (`:98`).

Two regex rules on the same field silently overwrite each other — only the last survives, and the first
is never evaluated. It does not fire in the current `github_events.yaml` (each field has at most one
regex rule), which is exactly why it will not be noticed when someone adds a second.

Related: `_parse_type` (`type_checks.py:98-100`) silently falls back to `str` for any unrecognized type
string, so a typo like `expected_type: "intger"` becomes a string check that quietly passes or fails for
the wrong reason.

**Fix:** key compiled patterns by rule index or by `(field, pattern)`. Raise on unknown type names at
load time rather than defaulting.

### F26 — `schedule_cron` is not cron

`drift_service.py:76-79` string-parses `"0 */6 * * *"` by splitting on whitespace and checking whether
`parts[1]` starts with `*/`, then calls `schedule.every(N).hours`. Consequences:

- Runs are anchored to **process start**, not the wall clock. A restart at 02:47 means runs at 08:47,
  14:47 — not the 00:00/06:00 the cron expression states.
- Any expression that is not exactly `"M */N * * *"` silently falls back to 6 hours.
- `DriftRunner()` is constructed twice — `:68` in `main()` purely to read `runner.config`, then again
  at `:37` on every single run — reparsing the YAML and reconstructing all three detectors each time.
- Nothing prevents overlapping runs if one exceeds the interval.

**Fix:** either use a real cron library (`croniter`, `APScheduler`) and honour the expression, or rename
the key to `schedule_interval_hours` and stop implying cron semantics. Construct `DriftRunner` once.

### F27 — Environment and packaging drift

- **Python version mismatch.** The local `.venv` is **3.14.6**; the `Dockerfile` is `python:3.11-slim`.
  Three minor versions of divergence between where tests run and where code ships. The
  `datetime.utcnow()` deprecation (F6) is visible locally and invisible in the image.
- **Dockerfile.** Single stage, so `build-essential` and `git` ship in the runtime image. `COPY . /app`
  with **no `.dockerignore`** copies `.env`, `.git/`, `.venv/`, and `.pytest_cache/` into every image.
  Runs as **root**. No `HEALTHCHECK`.
- **docker-compose.yml.** Declares a `minio_data` volume that nothing mounts — MinIO uses the bind mount
  `./data/minio` instead. `postgres-consumer` does not `depends_on: validator` despite calling it
  synchronously on every message, so on a cold start it burns through the F8 fallback path until the
  validator is up. The `validator` service `depends_on: kafka`, which it never contacts. Only `minio`
  and `validator` define healthchecks, and no `depends_on` uses `condition: service_healthy`.

**Fix:** pin one Python version across `.venv`, Dockerfile, and (once it exists) CI. Multi-stage build,
non-root `USER`, and a `.dockerignore` covering `.env`, `.git`, `.venv`, `__pycache__`, `.pytest_cache`.
Prune the unused volume and correct the `depends_on` graph.

### F29 — Diagnostics

- **32 bare `except Exception` handlers** across the codebase. Several are actively harmful:
  `drift_runner.py:316-318` swallows every persistence failure and returns the summary anyway, so a
  drift run reports success having stored nothing (this is what concealed F1); `validator.py:223-234`
  converts any engine bug into a `CRITICAL` validation failure attributed to the *event*, so a code
  defect looks like bad data.
- **`print()` for warnings in library code** — `data_validator.py:95`, `:130`, `:159`;
  `value_checks.py:49`. These bypass the logging configuration entirely and are invisible to any log
  aggregator. `value_checks.py:49` in particular means an invalid regex in the rules file prints once at
  startup and is then silently skipped, so the check never runs and nothing reports it.
- **`verify=False` hardcoded** on the boto3 client (`minio_consumer.py:55`), with the comment "for local
  development" — but there is no flag; it applies in every environment, including when `MINIO_SECURE` is
  `true`.
- **Broken log counter.** `minio_consumer.py:139` initializes `total_size_kb = 0` and `:163` prints it
  every 100 messages — but **nothing ever increments it**. The size is computed at `:110` into a local
  `size_kb` that is discarded. The throughput log has always printed `Total Size: 0.00 KB`.

**Fix:** narrow the excepts to the exceptions actually expected; let the rest crash loudly. Replace
`print()` with `logger`. Gate `verify` on `MINIO_SECURE`. Either wire up `total_size_kb` or delete it.

### Acceptance criteria

- [ ] `parse_severity` and the nested-field accessor each have exactly **one** definition — `grep -c` proves it.
- [ ] Every module listed in F21 is deleted, and `pytest` still passes.
- [ ] `pip install -r requirements.txt` in a clean venv installs neither `scipy` nor `numpy`, **or** `ks_2samp` is genuinely called by `distribution_drift.py`.
- [ ] Image size recorded before and after (`docker images --format '{{.Size}}'`).
- [ ] `docker run --rm <image> whoami` does not print `root`.
- [ ] `docker run --rm <image> ls -a /app` shows no `.env`, `.git`, or `.venv`.
- [ ] Every remaining key in both YAML files is read by some code path — verified by grep, one key at a time.
- [ ] Setting the producer interval env var to 10 demonstrably changes the poll interval.
- [ ] Two regex rules on the same field both evaluate.
- [ ] `grep -rn "print(" --include="*.py"` returns nothing outside `run_pipeline.py` and `test_e2e.py`, which are CLI tools.

**Effort:** ~3 days. **Risk:** low individually, but it is a wide diff. Sequence it after Phase 5.

---

# Phase 5 — Test coverage

**Goal:** the two packages holding all the domain logic stop being untested.

### F28 — Zero coverage of the validation and drift packages

Current state, four files, all ingestion:

| File | Covers | Status |
|---|---|---|
| `test_github_producer.py` | producer | 5 tests, pass in 0.04 s |
| `test_postgres_writer.py` | `db/` | 1 test — see below |
| `test_postgres_consumer.py` | consumer | **hangs**; `test_init_db` fails (F2) |
| `test_minio_consumer.py` | consumer | **hangs** (F2) |

Untested entirely: **all of `data_validation/`** (~1,800 LOC — the engine, six checkers, the result
model, the metrics module) and **all of `drift_engine/`** (~1,400 LOC — three profiles, three detectors,
the runner, the report generator). That is roughly 3,200 lines, containing every bug in Phases 1 and 3
of this document.

`tests/test_postgres_writer.py` is the only `db/` test and asserts nothing behavioural — four
`isinstance` checks, one `hasattr`, and one identity check. It also patches
`db.postgres_writer.psycopg2.connect` while the code under test resolves `psycopg2.connect` through
`db.persistence`; it passes only because that path mutates the shared `psycopg2` module object, patching
it *globally*. A reader would reasonably conclude the patch target is correct. It is not.

**This is the highest-leverage phase and the cheapest.** The validation checkers and the drift detectors
are pure functions over dictionaries — no Kafka, no Postgres, no MinIO, no fixtures beyond a sample
event dict. Every Phase 1 bug (F4, F5, F24) is a five-line unit test.

**Suggested order, by value per hour:**

1. `ValidationEngine.validate_event` — the PASS/WARN/FAIL aggregation and the fixed check ordering.
2. The six checkers — one table-driven test each, valid and invalid cases per rule type.
3. The three detectors — this is where F4 and F24 live. Assert on *rates*, not counts.
4. The three profiles — cardinality caps, null-ratio arithmetic, `_flatten_dict` on nested payloads.
5. `PrometheusMetrics.export_text` — F5.

Add a `conftest.py` with a shared valid GitHub event fixture; the mock event already inlined in
`test_postgres_consumer.py:18-32` is a reasonable starting point.

### Acceptance criteria

- [ ] `pytest --cov=data_validation --cov=drift_engine` reports a coverage floor of **at least 70%** on both packages.
- [ ] Every bug in Phases 1 and 3 has a regression test that **fails against the current code** — verified by writing the test first.
- [x] The `db/` tests patch the path the code actually resolves, and assert on behaviour rather than `isinstance` — `tests/test_db_pool.py` and `tests/test_db_repositories.py` replaced `test_postgres_writer.py`.
- [ ] A `conftest.py` provides the shared event fixture; no test inlines its own copy.
- [ ] CI runs `pytest` on every push and fails the build below the coverage floor. **There is currently no CI at all** — this is the first thing to add.
- [ ] The full suite completes in under 30 seconds without any external service.

**Effort:** ~1 week for the floor, ongoing after. **Risk:** none. **Do this before Phases 1 and 4.**

---

## Addendum — persistence-layer findings (F30–F39)

Found during the consolidation of `db/`, after the original 29 were written. All are **fixed**; they are
recorded because each was a real defect and several were invisible in normal operation.

### F30 — No rollback anywhere in the repositories ✅ *fixed*

`db/repositories.py` ran `execute` then `commit` with no `try`/`except`. On any failure the exception
propagated with the transaction left aborted, and nothing rolled it back — so **every subsequent write on
that connection failed the same way for the life of the process**. The callers then hid it:
`data_validator.py` caught `Exception` and `print()`ed a warning; `drift_runner._persist_results` caught
it and deliberately did not re-raise. `DriftRunner` looped `save()` over one shared connection, so a
single bad row poisoned the rest of the batch.

This is the most consequential finding in the persistence layer and the original audit missed it: F10
noted the per-row commit as a *performance* problem without noticing that a failure part-way left the
connection unusable.

**Fixed by** `db.pool.transaction()`, which commits on clean exit and rolls back on any exception.

### F31 — Dead connections were handed out forever ✅ *fixed*

`get_connection()` tested `if not self.connection`. A closed psycopg2 connection object is still truthy,
so after any server-side termination — a Postgres restart, an idle timeout — the same dead connection was
returned on every call, with no ping, no reconnect and no retry. `connect()` also never closed an
existing connection before replacing it, and `disconnect()` had no `try/finally`, so a failing `close()`
left the object permanently holding a dead handle.

**Fixed by** an explicit `conn.closed` check that discards and re-borrows, plus pooling.

### F32 — `__exit__` neither committed nor rolled back ✅ *fixed*

`PersistenceService.__exit__` ignored `exc_type` and only called `disconnect()`. With
`autocommit = False`, work done inside `with persistence:` and not explicitly committed was discarded
**silently — no error, no log line**.

### F33 — `dbname` vs `database` raised TypeError ✅ *fixed*

`_build_connection_params` unconditionally set `database`, psycopg2's deprecated alias. Passing it a dict
using psycopg2's canonical `dbname` — which is exactly what `postgres_consumer.py:33` built — makes
psycopg2 raise `TypeError: you can't specify both 'database' and 'dbname' arguments`. Routing the
consumer through `db/` would have crashed on the first call until the key names were unified.

### F34 — A parameter interpolated inside a quoted SQL literal ✅ *fixed*

`db/postgres_writer.py:128`: `AND detected_at >= NOW() - INTERVAL '%s hours'`. psycopg2 does whole-string
`%` substitution and does not parse quoting, so the placeholder sat *inside* the literal. It rendered
correctly for an int by luck; a string produced `INTERVAL ''24' hours'` — a syntax error, and the same
mechanism is a quote-breakout vector. Now `now() - (%s * INTERVAL '1 hour')`.

### F35 — The JSONB round-trip was asymmetric ✅ *fixed*

Both models `json.dumps`'d before insert, handing psycopg2 a `str` for `JSONB` columns — which worked
only via Postgres' untyped-parameter inference — while reads returned Python dicts. Feeding
`get_recent_drifts()` output back into a write would have double-encoded. `validation_ts` was likewise
passed as an `.isoformat()` string into a `TIMESTAMP` column, while the drift model passed real
`datetime` objects: two contracts for the same concept. Values are now adapted with
`psycopg2.extras.Json` at the repository boundary, and `to_dict()` returns plain Python objects.

### F36 — `db/` imported the application ✅ *fixed*

`db/repositories.py` imported `drift_engine.models` and `data_validation.models`, and `db/__init__.py`
imported all of it eagerly — so `import db` pulled in the entire application, and `import data_validation`
transitively pulled in `drift_engine`. This contradicts the layering property ARCHITECTURE.md claims.
Repositories now take plain dictionaries.

### F37 — Phantom and write-only tables ✅ *fixed / documented*

`github_events_raw` was referenced by `rules/github_events.yaml:317` and as the default `table_name` in
`models/validation_result.py` — the value that would have been written into
`validation_results.table_name`. **No such table exists anywhere;** the real one is `github_events`. Both
now say `github_events`. That column was also renamed `source_table`, since `table_name` read as if it
named the table it sits in.

Separately, `github_events` is **write-only**: fifteen flat columns per event, read by no component
(only `test_e2e.py`), duplicating what `github_events_processed.event_data` already holds. Documented
rather than removed — dropping it is a product decision.

### F38 — Environment and readiness ✅ *fixed*

- `drift_engine/` never called `load_dotenv()`, so host runs silently used defaults while `.env` said
  otherwise. `db/config.py` now loads it for every component.
- `.env` set `POSTGRES_HOST=postgres`, a Docker-network hostname, in a file loaded by host-run
  processes — so `test_e2e.py` and `run_pipeline.py` could not resolve it. Now `localhost`, with compose
  setting `postgres` per service.
- `POSTGRES_PORT` was both the published host port and the in-network port, but in-network Postgres
  always listens on 5432 — setting it to anything else broke every service. Split into
  `POSTGRES_PORT_HOST`.
- No `postgres` healthcheck existed, and `postgres-consumer` did not depend on the validator it calls
  synchronously on every message, so on a cold start it dropped events fail-closed until uvicorn was up.
  Both fixed; `pg_isready` plus `condition: service_healthy`.
- Four conflicting `POSTGRES_DB` defaults, the most common of which (`SentinelDQ_DB`) matched no database
  that exists. One source now: `db/config.py`.

### F39 — `created_at` parsing dropped events ✅ *fixed*

`store_event` used `datetime.strptime(..., "%Y-%m-%dT%H:%M:%SZ")`, which raises `ValueError` on
fractional seconds. `test_e2e.py` produces `datetime.now(timezone.utc).isoformat()` timestamps, which
have microseconds — so **every event that harness produced failed to parse** and was dropped by the
`except Exception` in the consume loop. Now `datetime.fromisoformat`.

---

## Suggested sequencing

Phase numbers are grouped by theme, not by execution order. By dependency:

```
Phase 0  ─────────────────────────────►  nothing is verifiable until the suite runs
   │
   └──► Phase 5 (tests) ──┬──► Phase 1 (drift correctness)   needs tests to prove the fix
                          ├──► Phase 4 (consolidation)       needs tests to prove no change
                          └──► Phase 2 (hot path) ──► Phase 3 (memory + observability)
                                                              F13 and F18 must land together
```

Phase 5 before Phase 1 is the important edge: fixing the drift statistics without tests means replacing
one unverified signal with another.

---

## Appendix — What was changed

**First pass — F1 only, per explicit agreement.** Two files: one added import line in
`drift_engine/engine/drift_runner.py`, and a new `tests/test_drift_runner.py` driving `_fetch_data`
against a fake connection. Verified: the ad-hoc repro that previously raised `NameError` reached the DB
layer and raised `ConnectionError` instead.

**Second pass — the persistence layer.** `db/` was rebuilt as `config` / `pool` / `schema` /
`repositories` / `errors`; `base.py`, `database.py` and `postgres_writer.py` were deleted after their
batch insert and read methods moved onto `DriftRepository`. Every caller now goes through the package,
including `postgres_consumer`, which previously used raw `psycopg2`. Closes F7, F11, F17, the `db/` rows
of F21, and part of F10; F30–F39 were found during the work. `tests/test_drift_runner.py` and
`tests/test_postgres_writer.py` were replaced by `tests/test_db_pool.py` and
`tests/test_db_repositories.py`, and the `Consumer` patch targets were fixed so `pytest tests/`
terminates (F2).

**Not verified against a live database.** The unit suite passes and every module imports, but no Docker
or local PostgreSQL was available in the environment where this work was done, so the `pg_stat_activity`
measurement, `EXPLAIN` plans, the timezone equivalence check, `scripts/migrate_001.sql`, and
`test_e2e.py` have **not** been run. Do those before trusting the F7 severity question either way.

Every other finding in this document is diagnosis. Nothing else was reorganized, renamed, or deleted.

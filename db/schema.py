"""Every table and index in the system, in one place.

There is no ORM and no migration framework: ``init_schema()`` is run at
startup by each component and creates what is missing. That means **editing a
CREATE TABLE here does not alter an existing database** -- see
scripts/migrate_001.sql for the change that accompanied this module.

Consolidating the DDL fixes a specific failure: ``drift_results`` used to be
created in two places with different index sets, and because both used
CREATE TABLE IF NOT EXISTS, whichever component started first won and the
other's indexes were never created. Production got the version with none.

Timestamps are TIMESTAMPTZ throughout. As TIMESTAMP, a column filled by
DEFAULT now() stored the server's local time while the application wrote naive
UTC, so the drift engine's window filter silently selected the wrong rows
whenever the database timezone was not UTC.
"""

from __future__ import annotations

import logging

from db.pool import transaction

logger = logging.getLogger(__name__)

# Raw ingestion: one row per event, flat columns.
GITHUB_EVENTS = """
CREATE TABLE IF NOT EXISTS github_events (
    id SERIAL PRIMARY KEY,
    event_id TEXT UNIQUE,
    event_type TEXT,
    repo_id BIGINT,
    repo_name TEXT,
    repo_url TEXT,
    actor_id BIGINT,
    actor_login TEXT,
    actor_url TEXT,
    actor_avatar TEXT,
    payload_ref TEXT,
    payload_head TEXT,
    payload_before TEXT,
    push_id BIGINT,
    public BOOLEAN,
    created_at TIMESTAMPTZ,
    ingestion_ts TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON github_events(created_at);
"""

# The drift engine's only source. processed_at is the only column it filters
# on, and it carried no index -- so every scheduled run sequentially scanned an
# unbounded table.
GITHUB_EVENTS_PROCESSED = """
CREATE TABLE IF NOT EXISTS github_events_processed (
    id SERIAL PRIMARY KEY,
    event_id TEXT UNIQUE,
    event_data JSONB,
    processed_at TIMESTAMPTZ DEFAULT now(),
    validation_status TEXT
);
CREATE INDEX IF NOT EXISTS idx_processed_at ON github_events_processed(processed_at);
"""

DRIFT_RESULTS = """
CREATE TABLE IF NOT EXISTS drift_results (
    drift_id SERIAL PRIMARY KEY,
    drift_type VARCHAR(50) NOT NULL,
    entity VARCHAR(100),
    field_name VARCHAR(200),

    baseline_start TIMESTAMPTZ NOT NULL,
    baseline_end TIMESTAMPTZ NOT NULL,
    current_start TIMESTAMPTZ NOT NULL,
    current_end TIMESTAMPTZ NOT NULL,

    metric_name VARCHAR(100) NOT NULL,
    baseline_value JSONB,
    current_value JSONB,
    drift_score FLOAT NOT NULL,

    severity VARCHAR(20) NOT NULL,
    detected_at TIMESTAMPTZ DEFAULT now(),

    metadata JSONB
);
CREATE INDEX IF NOT EXISTS idx_drift_detected_at ON drift_results(detected_at);
CREATE INDEX IF NOT EXISTS idx_drift_severity ON drift_results(severity);
CREATE INDEX IF NOT EXISTS idx_drift_type_entity ON drift_results(drift_type, entity);
"""

# event_id is UNIQUE so re-validating an event updates its row instead of
# accumulating duplicates.
VALIDATION_RESULTS = """
CREATE TABLE IF NOT EXISTS validation_results (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL UNIQUE,
    source_table VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    failed_checks JSONB,
    error_messages JSONB,
    severity VARCHAR(50),
    validation_ts TIMESTAMPTZ NOT NULL,
    processing_time_ms FLOAT,
    metadata JSONB,
    failure_details JSONB,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_validation_ts ON validation_results(validation_ts);
CREATE INDEX IF NOT EXISTS idx_validation_status ON validation_results(status);
"""

ALL_TABLES = (
    GITHUB_EVENTS,
    GITHUB_EVENTS_PROCESSED,
    DRIFT_RESULTS,
    VALIDATION_RESULTS,
)


def init_schema() -> None:
    """Create every missing table and index. Safe to call on every startup."""
    with transaction() as cursor:
        for statement in ALL_TABLES:
            cursor.execute(statement)
    logger.info("Schema ready")

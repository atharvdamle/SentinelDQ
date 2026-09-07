-- migrate_001: timestamptz, missing indexes, validation_results rework.
--
-- init_schema() creates missing tables, but CREATE TABLE IF NOT EXISTS does
-- nothing to a table that already exists. Run this once against any database
-- created before the persistence-layer consolidation:
--
--   docker-compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
--     -f - < scripts/migrate_001.sql
--
-- Idempotent, and safe to run inside a transaction.

BEGIN;

-- 1. Naive TIMESTAMP columns held server-local time where DEFAULT now() filled
--    them and naive UTC where the application did. Existing rows are
--    reinterpreted as UTC, which is what the application intended.
ALTER TABLE github_events
    ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
    ALTER COLUMN ingestion_ts TYPE TIMESTAMPTZ USING ingestion_ts AT TIME ZONE 'UTC',
    ALTER COLUMN ingestion_ts SET DEFAULT now();

ALTER TABLE github_events_processed
    ALTER COLUMN processed_at TYPE TIMESTAMPTZ USING processed_at AT TIME ZONE 'UTC',
    ALTER COLUMN processed_at SET DEFAULT now();

ALTER TABLE drift_results
    ALTER COLUMN baseline_start TYPE TIMESTAMPTZ USING baseline_start AT TIME ZONE 'UTC',
    ALTER COLUMN baseline_end TYPE TIMESTAMPTZ USING baseline_end AT TIME ZONE 'UTC',
    ALTER COLUMN current_start TYPE TIMESTAMPTZ USING current_start AT TIME ZONE 'UTC',
    ALTER COLUMN current_end TYPE TIMESTAMPTZ USING current_end AT TIME ZONE 'UTC',
    ALTER COLUMN detected_at TYPE TIMESTAMPTZ USING detected_at AT TIME ZONE 'UTC',
    ALTER COLUMN detected_at SET DEFAULT now();

-- 2. The index the drift engine's only query needs. Without it every
--    scheduled run sequentially scanned github_events_processed.
CREATE INDEX IF NOT EXISTS idx_processed_at ON github_events_processed(processed_at);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON github_events(created_at);

-- 3. drift_results indexes. These were defined only in the module the running
--    path did not use, so a live database has none of them.
CREATE INDEX IF NOT EXISTS idx_drift_detected_at ON drift_results(detected_at);
CREATE INDEX IF NOT EXISTS idx_drift_severity ON drift_results(severity);
CREATE INDEX IF NOT EXISTS idx_drift_type_entity ON drift_results(drift_type, entity);

COMMIT;

-- 4. validation_results was never created on the running path, so in most
--    databases there is nothing to migrate and init_schema() builds it fresh.
--    If it does exist, it needs the column rename, the uniqueness that makes
--    the upsert work, and its indexes.
DO $$
BEGIN
    IF to_regclass('public.validation_results') IS NULL THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'validation_results' AND column_name = 'table_name'
    ) THEN
        -- 'table_name' collided with the repository's own attribute name and
        -- read as if it named this table rather than the event's source.
        ALTER TABLE validation_results RENAME COLUMN table_name TO source_table;
    END IF;

    ALTER TABLE validation_results
        ALTER COLUMN validation_ts TYPE TIMESTAMPTZ USING validation_ts AT TIME ZONE 'UTC',
        ALTER COLUMN created_at TYPE TIMESTAMPTZ USING created_at AT TIME ZONE 'UTC',
        ALTER COLUMN created_at SET DEFAULT now();

    -- Required by ON CONFLICT (event_id). Deduplicate first, keeping the most
    -- recent verdict per event.
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'validation_results_event_id_key'
    ) THEN
        DELETE FROM validation_results a
        USING validation_results b
        WHERE a.event_id = b.event_id AND a.id < b.id;

        ALTER TABLE validation_results
            ADD CONSTRAINT validation_results_event_id_key UNIQUE (event_id);
    END IF;

    CREATE INDEX IF NOT EXISTS idx_validation_ts ON validation_results(validation_ts);
    CREATE INDEX IF NOT EXISTS idx_validation_status ON validation_results(status);
END
$$;

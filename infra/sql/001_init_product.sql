-- =============================================================================
-- TALOS :: talos_product :: 001_init_product.sql
-- =============================================================================
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DO $$ BEGIN
    CREATE TYPE source_system AS ENUM (
        'dialpad','gmail','sms','google_voice','manual','other'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE event_channel AS ENUM (
        'email','sms','voicemail','call','meeting','chat','other'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE event_direction AS ENUM ('inbound','outbound','internal','unknown');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE job_state AS ENUM (
        'QUEUED','DISPATCHED','PROCESSING','DEFERRED','SUCCEEDED','FAILED','CANCELLED'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE baseline_state AS ENUM (
        'NONE','QUEUED','PROCESSING','PARTIAL','READY','STALE'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE model_provenance AS ENUM (
        'default_model','partial_baseline','contact_baseline','override'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE detail_level AS ENUM ('quiet','normal','verbose');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS communication_events (
    event_uuid          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system       source_system NOT NULL,
    channel             event_channel NOT NULL,
    direction           event_direction NOT NULL DEFAULT 'unknown',
    external_id         TEXT,
    remote_identifier   TEXT,
    local_identifier    TEXT,
    occurred_at         TIMESTAMPTZ NOT NULL,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    contact_uuid        UUID,
    subject             TEXT,
    source_metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,
    verified            BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_system, external_id)
);
CREATE INDEX IF NOT EXISTS idx_events_contact ON communication_events (contact_uuid);
CREATE INDEX IF NOT EXISTS idx_events_occurred_at ON communication_events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_remote_identifier ON communication_events (remote_identifier);
CREATE INDEX IF NOT EXISTS idx_events_unresolved
    ON communication_events (received_at DESC) WHERE contact_uuid IS NULL;

CREATE TABLE IF NOT EXISTS event_artifacts (
    artifact_uuid   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_uuid      UUID NOT NULL REFERENCES communication_events(event_uuid) ON DELETE CASCADE,
    kind            TEXT NOT NULL,
    storage_scheme  TEXT NOT NULL,
    storage_uri     TEXT NOT NULL,
    sha256          TEXT,
    byte_size       BIGINT,
    content_type    TEXT,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_artifacts_event ON event_artifacts (event_uuid);
CREATE INDEX IF NOT EXISTS idx_artifacts_sha ON event_artifacts (sha256);

CREATE TABLE IF NOT EXISTS event_participants (
    participant_uuid   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_uuid         UUID NOT NULL REFERENCES communication_events(event_uuid) ON DELETE CASCADE,
    participant_index  INTEGER NOT NULL,
    role               TEXT NOT NULL,
    display_name       TEXT,
    email_raw          TEXT,
    email_normalized   TEXT,
    phone_raw          TEXT,
    phone_normalized   TEXT,
    source_person_id   TEXT,
    source_system      source_system,
    is_internal        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_uuid, participant_index)
);
CREATE INDEX IF NOT EXISTS idx_event_participants_event
    ON event_participants (event_uuid, participant_index);
CREATE INDEX IF NOT EXISTS idx_event_participants_email
    ON event_participants (email_normalized) WHERE email_normalized IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_event_participants_phone
    ON event_participants (phone_normalized) WHERE phone_normalized IS NOT NULL;

-- event_contact_links: surrogate PK + partial unique indexes.
-- Postgres cannot use COALESCE-keyed uniqueness in a PRIMARY KEY.
CREATE TABLE IF NOT EXISTS event_contact_links (
    link_uuid       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_uuid      UUID NOT NULL REFERENCES communication_events(event_uuid) ON DELETE CASCADE,
    contact_uuid    UUID,
    candidate_uuid  UUID,
    role            TEXT NOT NULL,
    confidence      NUMERIC(4,3),
    resolver        TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT event_contact_links_target_present CHECK (
        contact_uuid IS NOT NULL OR candidate_uuid IS NOT NULL
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_event_contact_links_contact
    ON event_contact_links (event_uuid, role, contact_uuid)
    WHERE contact_uuid IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_event_contact_links_candidate
    ON event_contact_links (event_uuid, role, candidate_uuid)
    WHERE candidate_uuid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_event_contact_links_event
    ON event_contact_links (event_uuid);
CREATE INDEX IF NOT EXISTS idx_event_contact_links_contact
    ON event_contact_links (contact_uuid) WHERE contact_uuid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_event_contact_links_candidate
    ON event_contact_links (candidate_uuid) WHERE candidate_uuid IS NOT NULL;

CREATE TABLE IF NOT EXISTS processing_jobs (
    job_uuid        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type        TEXT NOT NULL,
    state           job_state NOT NULL DEFAULT 'QUEUED',
    priority        SMALLINT NOT NULL DEFAULT 100,
    event_uuid      UUID REFERENCES communication_events(event_uuid) ON DELETE SET NULL,
    contact_uuid    UUID,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    result          JSONB,
    idempotency_key TEXT UNIQUE,
    dispatched_at   TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_jobs_state_priority
    ON processing_jobs (state, priority DESC, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_jobs_contact
    ON processing_jobs (contact_uuid) WHERE contact_uuid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_event
    ON processing_jobs (event_uuid) WHERE event_uuid IS NOT NULL;

CREATE TABLE IF NOT EXISTS processing_runs (
    run_uuid        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_uuid        UUID NOT NULL REFERENCES processing_jobs(job_uuid) ON DELETE CASCADE,
    worker_id       TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    outcome         TEXT,
    logs_uri        TEXT,
    stats           JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_runs_job ON processing_runs (job_uuid);

CREATE TABLE IF NOT EXISTS dialpad_recovery_state (
    recovery_key             TEXT PRIMARY KEY,
    last_recovered_through   TIMESTAMPTZ,
    last_run_started_at      TIMESTAMPTZ,
    last_run_finished_at     TIMESTAMPTZ,
    last_run_status          TEXT NOT NULL DEFAULT 'idle',
    last_error               TEXT,
    last_job_uuid            UUID REFERENCES processing_jobs(job_uuid) ON DELETE SET NULL,
    last_payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transcripts (
    transcript_uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_uuid      UUID NOT NULL REFERENCES communication_events(event_uuid) ON DELETE CASCADE,
    engine          TEXT NOT NULL,
    language        TEXT,
    diarized        BOOLEAN NOT NULL DEFAULT FALSE,
    text            TEXT,
    confidence      NUMERIC(4,3),
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_transcripts_event ON transcripts (event_uuid);

CREATE TABLE IF NOT EXISTS utterances (
    utterance_uuid  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transcript_uuid UUID NOT NULL REFERENCES transcripts(transcript_uuid) ON DELETE CASCADE,
    sequence        INTEGER NOT NULL,
    start_ms        INTEGER,
    end_ms          INTEGER,
    speaker_label   TEXT,
    contact_uuid    UUID,
    text            TEXT NOT NULL,
    confidence      NUMERIC(4,3),
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_utterances_transcript ON utterances (transcript_uuid, sequence);
CREATE INDEX IF NOT EXISTS idx_utterances_contact ON utterances (contact_uuid);

CREATE TABLE IF NOT EXISTS behavior_features (
    feature_uuid    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contact_uuid    UUID,
    event_uuid      UUID REFERENCES communication_events(event_uuid) ON DELETE CASCADE,
    utterance_uuid  UUID REFERENCES utterances(utterance_uuid) ON DELETE CASCADE,
    feature_set     TEXT NOT NULL,
    features        JSONB NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_features_contact ON behavior_features (contact_uuid);
CREATE INDEX IF NOT EXISTS idx_features_event ON behavior_features (event_uuid);

CREATE TABLE IF NOT EXISTS contact_baselines (
    baseline_uuid       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contact_uuid        UUID NOT NULL,
    state               baseline_state NOT NULL DEFAULT 'NONE',
    version             INTEGER NOT NULL DEFAULT 1,
    events_observed     INTEGER NOT NULL DEFAULT 0,
    utterances_observed INTEGER NOT NULL DEFAULT 0,
    coverage_score      NUMERIC(4,3),
    payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
    stale_after         TIMESTAMPTZ,
    last_refresh_job    UUID REFERENCES processing_jobs(job_uuid) ON DELETE SET NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (contact_uuid, version)
);
CREATE INDEX IF NOT EXISTS idx_baselines_contact_state
    ON contact_baselines (contact_uuid, state);

CREATE TABLE IF NOT EXISTS advisory_outputs (
    advisory_uuid       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_uuid          UUID REFERENCES communication_events(event_uuid) ON DELETE SET NULL,
    contact_uuid        UUID,
    baseline_uuid       UUID REFERENCES contact_baselines(baseline_uuid) ON DELETE SET NULL,
    provenance          model_provenance NOT NULL,
    provisional         BOOLEAN NOT NULL DEFAULT FALSE,
    summary             TEXT NOT NULL,
    detail              TEXT,
    confidence          NUMERIC(4,3),
    risk_level          TEXT,
    signals             JSONB NOT NULL DEFAULT '{}'::jsonb,
    surfaced_at_level   detail_level NOT NULL DEFAULT 'normal',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_advisory_event ON advisory_outputs (event_uuid);
CREATE INDEX IF NOT EXISTS idx_advisory_contact ON advisory_outputs (contact_uuid);

CREATE TABLE IF NOT EXISTS delete_index (
    delete_uuid     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scope           TEXT NOT NULL,
    target_uuid     UUID,
    source_system   source_system,
    requested_by    TEXT NOT NULL,
    reason          TEXT,
    requested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'pending',
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_delete_status ON delete_index (status);

CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_events_touch ON communication_events;
CREATE TRIGGER trg_events_touch BEFORE UPDATE ON communication_events
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

DROP TRIGGER IF EXISTS trg_jobs_touch ON processing_jobs;
CREATE TRIGGER trg_jobs_touch BEFORE UPDATE ON processing_jobs
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

DROP TRIGGER IF EXISTS trg_baselines_touch ON contact_baselines;
CREATE TRIGGER trg_baselines_touch BEFORE UPDATE ON contact_baselines
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

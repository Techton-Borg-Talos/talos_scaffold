-- TALOS :: talos_product :: 003_contact_registry.sql
CREATE TABLE IF NOT EXISTS contacts (
    contact_uuid    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    display_name    TEXT NOT NULL,
    given_name      TEXT,
    family_name     TEXT,
    organization    TEXT,
    notes           TEXT,
    tags            TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    deleted_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_contacts_display_name ON contacts (display_name);
CREATE INDEX IF NOT EXISTS idx_contacts_tags ON contacts USING GIN (tags);

CREATE TABLE IF NOT EXISTS contact_identities (
    identity_uuid   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contact_uuid    UUID NOT NULL REFERENCES contacts(contact_uuid) ON DELETE CASCADE,
    identity_kind   TEXT NOT NULL,
    identity_value  TEXT NOT NULL,
    source_system   source_system,
    is_primary      BOOLEAN NOT NULL DEFAULT FALSE,
    verified        BOOLEAN NOT NULL DEFAULT FALSE,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (identity_kind, identity_value)
);
CREATE INDEX IF NOT EXISTS idx_identities_contact ON contact_identities (contact_uuid);
CREATE INDEX IF NOT EXISTS idx_identities_value ON contact_identities (identity_kind, identity_value);

CREATE TABLE IF NOT EXISTS contact_candidates (
    candidate_uuid      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    working_label       TEXT,
    primary_identity_kind   TEXT,
    primary_identity_value  TEXT,
    sightings_count     INTEGER NOT NULL DEFAULT 0,
    score               NUMERIC(5,3) NOT NULL DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'open',
    promoted_contact_uuid   UUID REFERENCES contacts(contact_uuid) ON DELETE SET NULL,
    proposed_at         TIMESTAMPTZ,
    resolved_at         TIMESTAMPTZ,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (primary_identity_kind, primary_identity_value)
);
CREATE INDEX IF NOT EXISTS idx_candidates_status
    ON contact_candidates (status, last_seen_at DESC);

CREATE TABLE IF NOT EXISTS candidate_observations (
    observation_uuid    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_uuid      UUID NOT NULL REFERENCES contact_candidates(candidate_uuid) ON DELETE CASCADE,
    event_uuid          UUID REFERENCES communication_events(event_uuid) ON DELETE SET NULL,
    identity_kind       TEXT NOT NULL,
    identity_value      TEXT NOT NULL,
    source_system       source_system,
    observed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_cand_obs_candidate
    ON candidate_observations (candidate_uuid, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_cand_obs_identity
    ON candidate_observations (identity_kind, identity_value);

CREATE TABLE IF NOT EXISTS contact_merges (
    merge_uuid          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kept_contact_uuid   UUID NOT NULL REFERENCES contacts(contact_uuid) ON DELETE CASCADE,
    absorbed_contact_uuid UUID NOT NULL,
    decided_by          TEXT NOT NULL,
    rationale           TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_contacts_touch ON contacts;
CREATE TRIGGER trg_contacts_touch BEFORE UPDATE ON contacts
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

DROP TRIGGER IF EXISTS trg_candidates_touch ON contact_candidates;
CREATE TRIGGER trg_candidates_touch BEFORE UPDATE ON contact_candidates
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

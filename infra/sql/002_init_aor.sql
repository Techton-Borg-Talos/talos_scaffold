-- TALOS :: talos_aor :: 002_init_aor.sql
\connect talos_aor

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS aor_policies (
    policy_uuid     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_key      TEXT NOT NULL,
    policy_value    JSONB NOT NULL,
    scope           TEXT NOT NULL DEFAULT 'global',
    author          TEXT NOT NULL,
    effective_from  TIMESTAMPTZ NOT NULL DEFAULT now(),
    effective_to    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_policies_key_scope
    ON aor_policies (policy_key, scope, effective_from DESC);

CREATE TABLE IF NOT EXISTS aor_escalations (
    escalation_uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subject_kind    TEXT NOT NULL,
    subject_uuid    UUID NOT NULL,
    reason_code     TEXT NOT NULL,
    severity        TEXT NOT NULL DEFAULT 'normal',
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at       TIMESTAMPTZ,
    decision        TEXT,
    decided_by      TEXT,
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_escalations_open
    ON aor_escalations (opened_at DESC) WHERE closed_at IS NULL;

CREATE TABLE IF NOT EXISTS aor_contact_reviews (
    review_uuid     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_uuid  UUID,
    contact_uuid    UUID,
    action          TEXT NOT NULL,
    rationale       TEXT,
    decided_by      TEXT NOT NULL,
    decided_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS aor_bridge_audit (
    audit_uuid      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    direction       TEXT NOT NULL,
    event_code      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    delivered       BOOLEAN NOT NULL DEFAULT FALSE,
    delivered_at    TIMESTAMPTZ,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_bridge_created ON aor_bridge_audit (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_bridge_code ON aor_bridge_audit (event_code);

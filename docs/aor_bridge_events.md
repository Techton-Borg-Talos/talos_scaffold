# AoR Bridge Events

Per-event contracts. Allow-list lives in `aor_translator_boundary.json`.
Routing lives in `config/aor_bridge_rules.example.json`. Required payload
fields live in `config/aor_bridge_events.example.json`.

The bridge is a thin relay: it validates against these contracts and
routes. It does not infer meaning.

## Envelope

Every row in `bridge_outbox`:
- outbox_uuid, event_code, payload (jsonb), created_at,
  delivered_at (nullable), delivery_error (nullable).

Audit rows in `talos_aor.aor_bridge_audit` mirror these plus `direction`.

## Product → AoR events

- PROCESSING_FAILURE — job failed. {job_uuid, job_type, error}.
  Route: #system-alerts, #system-verbose.
- BASELINE_MISSING — baseline state NONE for contact. {contact_uuid}.
  Route: #system-verbose.
- BASELINE_BACKFILL_QUEUED — backfill job enqueued.
  {contact_uuid, idempotency_key}. Route: #system-verbose.
- CONTACT_MATCH_AMBIGUOUS — candidate hit promotion threshold.
  {candidate_uuid, identity_kind, identity_value, sightings_count}.
  Route: #aor-deliberation.
- ADVISORY_HIGH_UNCERTAINTY — low confidence advisory.
  {advisory_uuid, confidence, provenance}. Route: #aor-deliberation.
- ADVISORY_HIGH_RISK — high risk advisory.
  {advisory_uuid, risk_level, summary}. Route: #aor-front-desk, #aor-deliberation.
- WORKER_OFFLINE — scheduler lost worker.
  {local_worker_url, error}. Route: #system-alerts, #infrastructure.
- LIVE_TO_DEFERRED_FALLBACK — job deferred because worker offline.
  {job_uuid, job_type}. Route: #system-verbose.
- DEPLOY_FAILURE — deploy hook failure.
  {trigger, failed_step}. Route: #system-alerts, #infrastructure.
- SYSTEM_HEALTH_WARNING — generic health warning.
  {service, severity, detail}. Route: #system-alerts.

## AoR → Product events (Phase 1: allow-listed, consumer not yet built)

- POLICY_UPDATE, THRESHOLD_UPDATE, DETAIL_LEVEL_UPDATE,
  APPROVED_RESPONSE_TEMPLATE, ESCALATION_DECISION,
  CONTACT_REVIEW_DECISION, FEATURE_FLAG_UPDATE.

## Validation behavior

- event_code not in allow-list → delivery_error = "not_in_allow_list".
- required field missing → delivery_error = "missing_field:<name>".
- allowed but no route → delivery_error = "no_route".
- Matrix delivery failure → row left undelivered for retry.
- Partial delivery (one room fails) → row marked delivered with partial error.

## Known Phase 1 limitation

`bridge_outbox` currently has no attempt_count / last_attempt_at /
terminal_error columns. A poison row (e.g. missing_field that is never
fixed) will sit there forever and the bridge will re-audit it on every
drain. Future pass will add retry/poison-row controls; see
`services/matrix_bridge/app.py` TODO block.

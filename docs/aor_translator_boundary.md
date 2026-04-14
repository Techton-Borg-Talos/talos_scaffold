# AoR / Universal Translator Boundary

Governing artifact. Source of truth. Companion JSON:
`aor_translator_boundary.json`. If the two disagree, this markdown wins
and the JSON must be updated.

## 1. Definitions

**AoR (Architecture of Responsibility)** — internal governance + orchestration.
Surfaced through Matrix/Synapse. Not user-facing. Not marketed.

**Universal Translator** — the commercial, user-facing product. Ingests
historical + live comms, resolves contacts, builds contact-specific
behavioral baselines, falls back to default/partial/provisional when
history is unavailable, emits explicit assistive guidance.

## 2. Separation principles

1. Different identities. Translator public; AoR internal.
2. Different data stores. `talos_product` vs `talos_aor`. No cross-DB FKs.
3. Different compose projects. `talos-product` vs `talos-aor`.
4. Different channels. Translator → user; AoR → operators in Matrix.
5. Mediated bridge only (§4).
6. Matrix is not the system of record — Postgres + artifact storage are.

## 3. Baseline fallback rule (MANDATORY)

| State       | Behavior                                                      |
|-------------|---------------------------------------------------------------|
| NONE        | use default model NOW + queue CONTACT_HISTORY_BACKFILL        |
| QUEUED      | use default model NOW; do NOT duplicate backfill              |
| PROCESSING  | use default model NOW; do NOT duplicate backfill              |
| PARTIAL     | blended mode; output flagged provisional                      |
| READY       | use contact baseline; not provisional                         |
| STALE       | use existing baseline; queue one BASELINE_REFRESH             |

User is never blocked. Provisional/default output is clearly marked.

## 4. Allowed bridge events

Product → AoR:
PROCESSING_FAILURE, BASELINE_MISSING, BASELINE_BACKFILL_QUEUED,
CONTACT_MATCH_AMBIGUOUS, ADVISORY_HIGH_UNCERTAINTY, ADVISORY_HIGH_RISK,
WORKER_OFFLINE, LIVE_TO_DEFERRED_FALLBACK, DEPLOY_FAILURE,
SYSTEM_HEALTH_WARNING.

AoR → Product (consumer not yet built in Phase 1):
POLICY_UPDATE, THRESHOLD_UPDATE, DETAIL_LEVEL_UPDATE,
APPROVED_RESPONSE_TEMPLATE, ESCALATION_DECISION, CONTACT_REVIEW_DECISION,
FEATURE_FLAG_UPDATE.

## 5. Forbidden flows

- Raw deliberation → product.
- Raw chain-of-thought anywhere.
- Unrestricted consultant notes → product.
- Private/family Matrix content → product.
- Wholesale Matrix mirror → product.
- Raw artifacts (audio/email bodies/transcripts) → Matrix rooms.
- Product user messages routed to AoR as AoR input.
- matrix_bridge writing anywhere in talos_product other than
  bridge_outbox (delivered_at / delivery_error only).

## 6. Escalation

Escalation writes one row to bridge_outbox. Fire-and-forget. AoR
operators handle inline, open `aor_escalations`, or push an allow-listed
decision back.

## 7. Rooms on day one

- #aor-front-desk:taloscognitive.com
- #aor-deliberation:taloscognitive.com
- #consultant-council:taloscognitive.com
- #system-alerts:taloscognitive.com
- #system-verbose:taloscognitive.com
- #infrastructure:taloscognitive.com
- #consultant-rac:taloscognitive.com
- #consultant-mks:taloscognitive.com
- #consultant-hfc:taloscognitive.com

Detail levels: `quiet`, `normal`, `verbose`. Routing is config-only. No
code branching on event semantics in the bridge. Deliberation uses the
structured summary template (Task / Relevant facts / Assumptions /
Analysis / Risks / Recommendation / Confidence / Escalate). No raw
chain-of-thought logging.

## 8. Bridge-user provisioning (manual, Phase 1)

1. Bring up AoR.
2. Generate Synapse config.
3. `register_new_matrix_user -u talos-bridge ...`.
4. Log in once to get access token.
5. Invite to all §7 rooms.
6. Paste token into product `.env` `MATRIX_BRIDGE_USER_TOKEN`.
7. Restart `matrix_bridge`.

Until step 6, run with `BRIDGE_DRY_RUN=1`.

## 9. Future, out of scope for Phase 1

- AoR → product inbound consumer.
- Popup/live-assist channel.
- Wearable/glasses/multimodal delivery.
- Automated bridge-user lifecycle.
- Argo CD / GitOps / Kubernetes.
- Advanced contact engine on the worker.
- bridge_outbox retry/poison-row controls (see matrix_bridge TODO).

## 10. Change control

PR must update markdown AND JSON. Example configs must still validate.
Summary note in `#aor-deliberation`.

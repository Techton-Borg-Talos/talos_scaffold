# TALOS Worker Modules

Each file in this directory registers one or more job handlers via
`@worker_main.register("<JOB_TYPE>", "<module_name>")`. Disabled modules
(via `ENABLED_MODULES`) no-op cleanly.

## Contract

    async def handle(job: dict) -> dict:
        # job = {job_uuid, job_type, contact_uuid?, event_uuid?, payload}
        return {...}  # JSON-serializable

Exceptions become `{"status":"error", "module":..., "error":...}`.

## Job type catalog (Phase 1)

- TRANSCRIBE                → transcription
- FEATURE_EXTRACT           → behavior_analysis
- ADVISORY_GENERATE         → behavior_analysis
- CONTACT_MATCH_FUZZY       → contact_engine_local
- CONTACT_MERGE_RECOMMEND   → contact_engine_local
- CONTACT_HISTORY_BACKFILL  → baseline_updates
- BASELINE_REFRESH          → baseline_updates
- DEFERRED_DRAIN            → deferred_backfill
- (none yet)                → popup_live_assist

## Must not

- Cross the AoR/Translator boundary.
- Mutate talos_aor.
- Do synchronous blocking I/O inside a handler.
- Do heavy work at import time.

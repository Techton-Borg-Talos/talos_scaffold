"""modules/contact_engine_local — fuzzy/probabilistic matching (deferred)."""
from __future__ import annotations
from worker_main import register

@register("CONTACT_MATCH_FUZZY", "contact_engine_local")
async def handle_fuzzy(job: dict) -> dict:
    return {"todo":"fuzzy contact matching deliberately deferred past Phase 1",
            "job_uuid":job.get("job_uuid")}

@register("CONTACT_MERGE_RECOMMEND", "contact_engine_local")
async def handle_merge(job: dict) -> dict:
    return {"todo":"merge recommendation deliberately deferred past Phase 1",
            "job_uuid":job.get("job_uuid")}

"""modules/behavior_analysis — feature extraction + advisory generation."""
from __future__ import annotations
from worker_main import register

@register("FEATURE_EXTRACT", "behavior_analysis")
async def handle_fe(job: dict) -> dict:
    return {"todo":"feature extraction not yet implemented",
            "job_uuid":job.get("job_uuid"),
            "event_uuid":job.get("event_uuid")}

@register("ADVISORY_GENERATE", "behavior_analysis")
async def handle_adv(job: dict) -> dict:
    return {"todo":"advisory generation not yet implemented",
            "job_uuid":job.get("job_uuid"),
            "contact_uuid":job.get("contact_uuid")}

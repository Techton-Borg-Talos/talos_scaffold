"""modules/baseline_updates — per-contact baseline backfill + refresh."""
from __future__ import annotations
from worker_main import register

@register("CONTACT_HISTORY_BACKFILL", "baseline_updates")
async def handle_backfill(job: dict) -> dict:
    return {"todo":"baseline backfill not yet implemented",
            "job_uuid":job.get("job_uuid"),
            "contact_uuid":job.get("contact_uuid")}

@register("BASELINE_REFRESH", "baseline_updates")
async def handle_refresh(job: dict) -> dict:
    return {"todo":"baseline refresh not yet implemented",
            "job_uuid":job.get("job_uuid"),
            "contact_uuid":job.get("contact_uuid")}

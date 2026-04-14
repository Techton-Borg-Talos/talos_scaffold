"""modules/deferred_backfill — drains DEFERRED jobs when worker is online."""
from __future__ import annotations
from worker_main import register

@register("DEFERRED_DRAIN", "deferred_backfill")
async def handle_drain(job: dict) -> dict:
    return {"todo":"deferred drain not yet implemented",
            "job_uuid":job.get("job_uuid")}

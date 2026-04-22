"""modules/transcription — Whisper/pyannote transcription + speaker ID."""
from __future__ import annotations
from worker_main import register

@register("TRANSCRIBE", "transcription")
async def handle_transcribe(job: dict) -> dict:
    archive = job.get("payload", {}).get("archive") or {}
    # TODO(phase1): wire into existing comm_analysis pipeline.
    return {"todo":"transcription not yet implemented",
            "job_uuid":job.get("job_uuid"),
            "event_uuid":job.get("event_uuid"),
            "channel": archive.get("channel") or job.get("payload", {}).get("channel")}

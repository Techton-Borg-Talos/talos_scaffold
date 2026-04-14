"""modules/transcription — Whisper/pyannote transcription + speaker ID."""
from __future__ import annotations
from worker_main import register

@register("TRANSCRIBE", "transcription")
async def handle_transcribe(job: dict) -> dict:
    # TODO(phase1): wire into existing comm_analysis pipeline.
    return {"todo":"transcription not yet implemented",
            "job_uuid":job.get("job_uuid"),
            "event_uuid":job.get("event_uuid")}

"""modules/archive_export - preserve Dialpad event files to the shared archive."""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from worker_main import register

ARCHIVE_ROOT = Path(os.environ.get("DIALPAD_ARCHIVE_DIR", "/mnt/dialpad_archive"))
DEFAULT_OFFICE_MAP = {
    "4943523498434560": "INC",
}


def _load_company_map() -> Dict[str, str]:
    raw = os.environ.get("DIALPAD_COMPANY_MAP_JSON", "").strip()
    if not raw:
        return dict(DEFAULT_OFFICE_MAP)

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return dict(DEFAULT_OFFICE_MAP)

    merged = dict(DEFAULT_OFFICE_MAP)
    if isinstance(parsed, dict):
        for key, value in parsed.items():
            if key and value:
                merged[str(key)] = str(value)
    return merged


COMPANY_MAP = _load_company_map()
COMPANY_FALLBACK = os.environ.get("DIALPAD_COMPANY_FALLBACK", "UNMAPPED").strip() or "UNMAPPED"
CATEGORY_MAP = {
    "call": "Calls",
    "meeting": "Meetings",
    "sms": "SMS",
    "voicemail": "Voicemail",
}


def _safe_component(value: Optional[str], fallback: str) -> str:
    text = (value or "").strip()
    if not text:
        return fallback
    text = re.sub(r"[^A-Za-z0-9@+._-]+", "_", text)
    text = text.strip("._")
    return text or fallback


def _parse_occurred_at(value: Optional[str]) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)


def _company_slug(source_provenance: Dict[str, Any]) -> str:
    target = source_provenance.get("target") or {}
    office_id = str(target.get("office_id") or "").strip()
    if office_id and office_id in COMPANY_MAP:
        return COMPANY_MAP[office_id]
    return COMPANY_FALLBACK


def _internal_bucket(source_provenance: Dict[str, Any]) -> str:
    target = source_provenance.get("target") or {}
    return (
        source_provenance.get("internal_number")
        or target.get("email")
        or target.get("name")
        or target.get("office_id")
        or "unknown_target"
    )


def _channel_dir(channel: str) -> str:
    return CATEGORY_MAP.get(channel, _safe_component(channel, "Other"))


def _archive_channel(payload: Dict[str, Any]) -> str:
    channel = str(payload.get("channel") or "other")
    if payload.get("provider_transcript_text") or any(
        str((ref.get("metadata") or {}).get("reference_type") or "").startswith("voicemail")
        for ref in (payload.get("recording_references") or [])
        if isinstance(ref, dict)
    ):
        return "voicemail"
    return channel


def _write_text(path: Path, text: str) -> int:
    normalized = text.rstrip() + "\n"
    path.write_text(normalized, encoding="utf-8")
    return path.stat().st_size


def _write_json(path: Path, payload: Dict[str, Any]) -> int:
    rendered = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    path.write_text(rendered, encoding="utf-8")
    return path.stat().st_size


def _manifest_entry(path: Path, content_type: str, metadata: Dict[str, Any], byte_size: int) -> Dict[str, Any]:
    return {
        "storage_scheme": "local_file",
        "storage_uri": str(path),
        "content_type": content_type,
        "byte_size": byte_size,
        "metadata": metadata,
    }


@register("ARCHIVE_EXPORT", "archive_export")
async def handle_archive_export(job: Dict[str, Any]) -> Dict[str, Any]:
    payload = job.get("payload") or {}
    event_uuid = str(job.get("event_uuid") or payload.get("event_uuid") or "")
    channel = _archive_channel(payload)
    occurred_at = _parse_occurred_at(payload.get("occurred_at"))
    source_provenance = payload.get("source_provenance") or {}
    company_slug = _company_slug(source_provenance)
    internal_bucket = _safe_component(_internal_bucket(source_provenance), "unknown_target")

    archive_dir = (
        ARCHIVE_ROOT
        / _channel_dir(channel)
        / company_slug
        / f"{occurred_at:%Y}"
        / f"{occurred_at:%m}"
        / f"{occurred_at:%d}"
        / internal_bucket
        / _safe_component(event_uuid, "unknown_event")
    )
    archive_dir.mkdir(parents=True, exist_ok=True)

    manifest: List[Dict[str, Any]] = []
    common_meta = {
        "event_uuid": event_uuid,
        "channel": channel,
        "company_slug": company_slug,
    }

    metadata_payload = {
        "event_uuid": event_uuid,
        "external_id": payload.get("external_id"),
        "channel": channel,
        "subject": payload.get("subject"),
        "occurred_at": payload.get("occurred_at"),
        "source_provenance": source_provenance,
        "recording_references": payload.get("recording_references") or [],
    }
    metadata_path = archive_dir / "metadata.json"
    metadata_size = _write_json(metadata_path, metadata_payload)
    manifest.append(
        _manifest_entry(
            metadata_path,
            "application/json",
            {
                **common_meta,
                "file_role": "metadata",
            },
            metadata_size,
        )
    )

    event_payload = payload.get("source_metadata")
    if isinstance(event_payload, dict):
        event_path = archive_dir / "event.json"
        event_size = _write_json(event_path, event_payload)
        manifest.append(
            _manifest_entry(
                event_path,
                "application/json",
                {
                    **common_meta,
                    "file_role": "event_source_metadata",
                },
                event_size,
            )
        )

    provider_transcript_text = payload.get("provider_transcript_text")
    if isinstance(provider_transcript_text, str) and provider_transcript_text.strip():
        transcript_path = archive_dir / "provider_transcript.txt"
        transcript_size = _write_text(transcript_path, provider_transcript_text)
        manifest.append(
            _manifest_entry(
                transcript_path,
                "text/plain",
                {
                    **common_meta,
                    "file_role": "provider_transcript",
                    "provider": "dialpad",
                },
                transcript_size,
            )
        )

    sms_text = payload.get("sms_text")
    if isinstance(sms_text, str) and sms_text.strip():
        sms_path = archive_dir / "sms.txt"
        sms_size = _write_text(sms_path, sms_text)
        manifest.append(
            _manifest_entry(
                sms_path,
                "text/plain",
                {
                    **common_meta,
                    "file_role": "sms_text",
                    "provider": "dialpad",
                },
                sms_size,
            )
        )

    recording_references = payload.get("recording_references")
    if isinstance(recording_references, list) and recording_references:
        refs_path = archive_dir / "recording_references.json"
        refs_size = _write_json(refs_path, {"recording_references": recording_references})
        manifest.append(
            _manifest_entry(
                refs_path,
                "application/json",
                {
                    **common_meta,
                    "file_role": "recording_references",
                },
                refs_size,
            )
        )

    return {
        "job_uuid": job.get("job_uuid"),
        "event_uuid": event_uuid,
        "archive_dir": str(archive_dir),
        "archive_files": manifest,
        "provider_transcript_preserved": bool(provider_transcript_text),
        "sms_text_preserved": bool(sms_text),
        "recording_reference_count": len(recording_references or []),
    }

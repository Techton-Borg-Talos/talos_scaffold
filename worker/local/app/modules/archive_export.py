"""modules/archive_export - preserve Dialpad event files to the shared archive."""
from __future__ import annotations

import asyncio
import json
import mimetypes
import os
import re
import shlex
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

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
    "call": "calls",
    "meeting": "meetings",
    "sms": "sms",
    "voicemail": "voicemail",
}
ARCHIVE_TIMEZONE_NAME = os.environ.get("DIALPAD_ARCHIVE_TIMEZONE", "America/New_York").strip() or "America/New_York"
ARCHIVE_SYNC_MODE = os.environ.get("ARCHIVE_SYNC_MODE", "none").strip().lower()
ARCHIVE_SYNC_RSYNC_DEST = os.environ.get("ARCHIVE_SYNC_RSYNC_DEST", "").strip()
ARCHIVE_SYNC_SSH_KEY_PATH = os.environ.get("ARCHIVE_SYNC_SSH_KEY_PATH", "").strip()
ARCHIVE_SYNC_SSH_OPTS = os.environ.get("ARCHIVE_SYNC_SSH_OPTS", "").strip()
DIALPAD_API_TOKEN = os.environ.get("DIALPAD_API_TOKEN", "").strip()
DIALPAD_RECORDING_SHARE_PRIVACY = os.environ.get("DIALPAD_RECORDING_SHARE_PRIVACY", "company").strip() or "company"


def _safe_component(value: Optional[str], fallback: str) -> str:
    text = (value or "").strip()
    if not text:
        return fallback
    text = re.sub(r"[^A-Za-z0-9@+._-]+", "_", text)
    text = text.strip("._")
    return text or fallback


def _archive_timezone() -> timezone | ZoneInfo:
    try:
        return ZoneInfo(ARCHIVE_TIMEZONE_NAME)
    except ZoneInfoNotFoundError:
        return timezone.utc


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


def _conversation_number(source_provenance: Dict[str, Any]) -> str:
    target = source_provenance.get("target") or {}
    contact = source_provenance.get("contact") or {}
    return (
        source_provenance.get("external_number")
        or source_provenance.get("from_number")
        or contact.get("phone")
        or source_provenance.get("internal_number")
        or target.get("phone")
        or "unknown_number"
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


def _write_bytes(path: Path, content: bytes) -> int:
    path.write_bytes(content)
    return path.stat().st_size


def _manifest_entry(path: Path, content_type: str, metadata: Dict[str, Any], byte_size: int) -> Dict[str, Any]:
    return {
        "storage_scheme": "local_file",
        "storage_uri": str(path),
        "content_type": content_type,
        "byte_size": byte_size,
        "metadata": metadata,
    }


def _stem_suffix(channel: str, source_provenance: Dict[str, Any]) -> str:
    direction = str(source_provenance.get("direction") or "").lower()
    if channel == "sms":
        return "text"
    if channel == "voicemail":
        if direction == "outbound":
            return "sent_voicemail"
        if direction == "internal":
            return "internal_voicemail"
        return "received_voicemail"
    if channel == "call":
        if direction == "outbound":
            return "sent"
        if direction == "internal":
            return "internal"
        return "received"
    return _safe_component(channel, "event")


def _event_stem(phone_number: str, channel: str, occurred_at: datetime, source_provenance: Dict[str, Any]) -> str:
    local_dt = occurred_at.astimezone(_archive_timezone())
    timestamp = local_dt.strftime("%Y-%m-%dT%H-%M-%S")
    suffix = _stem_suffix(channel, source_provenance)
    return f"{phone_number}_{timestamp}_{suffix}"


def _duration_suffix(duration_ms: Optional[Any]) -> str:
    try:
        total_seconds = max(0, int(float(duration_ms) / 1000.0))
    except Exception:
        total_seconds = 0
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    return f"{hours:02d}-{minutes:02d}-{seconds:02d}"


def _guess_audio_extension(content_type: str, url: str) -> str:
    clean_content_type = (content_type or "").split(";")[0].strip().lower()
    mapping = {
        "audio/mpeg": ".mp3",
        "audio/mp3": ".mp3",
        "audio/wav": ".wav",
        "audio/x-wav": ".wav",
        "audio/wave": ".wav",
        "audio/mp4": ".m4a",
        "audio/x-m4a": ".m4a",
        "audio/aac": ".aac",
        "audio/ogg": ".ogg",
        "audio/webm": ".webm",
    }
    if clean_content_type in mapping:
        return mapping[clean_content_type]

    guessed = mimetypes.guess_extension(clean_content_type or "") or ""
    if guessed:
        return guessed

    match = re.search(r"\.(mp3|wav|m4a|aac|ogg|webm)(?:$|\?)", url, re.IGNORECASE)
    if match:
        return f".{match.group(1).lower()}"
    return ".mp3"


def _extract_http_urls(obj: Any) -> List[str]:
    urls: List[str] = []
    if isinstance(obj, str):
        if obj.startswith("http://") or obj.startswith("https://"):
            urls.append(obj)
        return urls
    if isinstance(obj, dict):
        for value in obj.values():
            urls.extend(_extract_http_urls(value))
        return urls
    if isinstance(obj, list):
        for item in obj:
            urls.extend(_extract_http_urls(item))
    return urls


def _recording_candidates(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    source_metadata = payload.get("source_metadata") or {}
    recording_references = payload.get("recording_references") or []

    for detail in source_metadata.get("recording_details") or []:
        if not isinstance(detail, dict):
            continue
        candidate = {
            "url": detail.get("url"),
            "recording_id": detail.get("id"),
            "recording_type": detail.get("recording_type"),
            "duration_ms": detail.get("duration"),
            "start_time": detail.get("start_time"),
            "source": "recording_details",
        }
        key = (
            str(candidate.get("url") or ""),
            str(candidate.get("recording_id") or ""),
            str(candidate.get("recording_type") or ""),
        )
        if key not in seen:
            seen.add(key)
            candidates.append(candidate)

    for ref in recording_references:
        if not isinstance(ref, dict):
            continue
        metadata = ref.get("metadata") or {}
        storage_uri = str(ref.get("storage_uri") or "")
        candidate = {
            "url": storage_uri if storage_uri.startswith("http://") or storage_uri.startswith("https://") else None,
            "recording_id": metadata.get("recording_id"),
            "recording_type": metadata.get("recording_type"),
            "duration_ms": metadata.get("duration_ms"),
            "start_time": metadata.get("start_time"),
            "source": metadata.get("reference_type") or ref.get("kind"),
        }
        if not candidate["recording_type"]:
            ref_type = str(metadata.get("reference_type") or "")
            if "voicemail" in ref_type:
                candidate["recording_type"] = "voicemail"
            elif "admin" in ref_type:
                candidate["recording_type"] = "admincallrecording"
            else:
                candidate["recording_type"] = "callrecording"
        key = (
            str(candidate.get("url") or ""),
            str(candidate.get("recording_id") or ""),
            str(candidate.get("recording_type") or ""),
        )
        if key not in seen:
            seen.add(key)
            candidates.append(candidate)

    return candidates


async def _dialpad_api_request(method: str, url: str, json_body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {DIALPAD_API_TOKEN}",
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        response = await client.request(method, url, json=json_body, headers=headers)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict):
            return payload
        return {"value": payload}


async def _resolve_recording_url(candidate: Dict[str, Any]) -> Optional[str]:
    direct_url = str(candidate.get("url") or "")
    if direct_url.startswith("http://") or direct_url.startswith("https://"):
        return direct_url

    recording_id = str(candidate.get("recording_id") or "").strip()
    recording_type = str(candidate.get("recording_type") or "").strip()
    if not (recording_id and recording_type and DIALPAD_API_TOKEN):
        return None

    share_payload = await _dialpad_api_request(
        "POST",
        "https://dialpad.com/api/v2/recordingsharelink",
        {
            "privacy": DIALPAD_RECORDING_SHARE_PRIVACY,
            "recording_id": recording_id,
            "recording_type": recording_type,
        },
    )

    urls = _extract_http_urls(share_payload)
    if urls:
        return urls[0]

    share_link_id = (
        share_payload.get("id")
        or share_payload.get("sharelink_id")
        or share_payload.get("share_link_id")
    )
    if not share_link_id:
        return None

    fetched = await _dialpad_api_request(
        "GET",
        f"https://dialpad.com/api/v2/recordingsharelink/{share_link_id}",
    )
    urls = _extract_http_urls(fetched)
    return urls[0] if urls else None


async def _download_audio_file(url: str) -> tuple[bytes, str, str]:
    headers = {}
    if DIALPAD_API_TOKEN and url.startswith("https://dialpad.com/api/"):
        headers["Authorization"] = f"Bearer {DIALPAD_API_TOKEN}"
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        response = await client.get(url, headers=headers)
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if not content_type.lower().startswith("audio/"):
            raise ValueError(f"unexpected content-type {content_type or 'unknown'}")
        return response.content, content_type, str(response.url)


async def _sync_archive_dir(archive_dir: Path) -> Optional[Dict[str, Any]]:
    if ARCHIVE_SYNC_MODE != "rsync" or not ARCHIVE_SYNC_RSYNC_DEST:
        return None

    relative_dir = archive_dir.relative_to(ARCHIVE_ROOT).as_posix()
    remote_dest = f"{ARCHIVE_SYNC_RSYNC_DEST.rstrip('/')}/{relative_dir}/"
    cmd = ["rsync", "-az"]

    ssh_parts: List[str] = []
    if ARCHIVE_SYNC_SSH_KEY_PATH:
        ssh_parts.extend(["-i", ARCHIVE_SYNC_SSH_KEY_PATH])
    if ARCHIVE_SYNC_SSH_OPTS:
        ssh_parts.extend(shlex.split(ARCHIVE_SYNC_SSH_OPTS))
    if ssh_parts:
        cmd.extend(["-e", "ssh " + " ".join(shlex.quote(part) for part in ssh_parts)])

    cmd.extend([f"{archive_dir.as_posix()}/", remote_dest])
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return {
        "mode": "rsync",
        "destination": remote_dest,
        "ok": proc.returncode == 0,
        "exit_code": proc.returncode,
        "stdout": stdout.decode("utf-8", errors="replace").strip(),
        "stderr": stderr.decode("utf-8", errors="replace").strip(),
    }


async def _download_recordings(
    payload: Dict[str, Any],
    archive_dir: Path,
    event_stem: str,
    common_meta: Dict[str, Any],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    manifests: List[Dict[str, Any]] = []
    results: List[Dict[str, Any]] = []
    candidates = _recording_candidates(payload)

    for index, candidate in enumerate(candidates, start=1):
        try:
            resolved_url = await _resolve_recording_url(candidate)
            if not resolved_url:
                results.append(
                    {
                        "index": index,
                        "status": "skipped",
                        "reason": "no_download_url",
                        "recording_id": candidate.get("recording_id"),
                        "recording_type": candidate.get("recording_type"),
                        "source": candidate.get("source"),
                    }
                )
                continue

            content, content_type, final_url = await _download_audio_file(resolved_url)
            extension = _guess_audio_extension(content_type, final_url)
            duration_suffix = _duration_suffix(candidate.get("duration_ms"))
            if len(candidates) == 1:
                filename = f"{event_stem}_{duration_suffix}{extension}"
            else:
                filename = f"{event_stem}_part{index:02d}_{duration_suffix}{extension}"
            audio_path = archive_dir / filename
            byte_size = _write_bytes(audio_path, content)
            manifests.append(
                _manifest_entry(
                    audio_path,
                    content_type or "audio/mpeg",
                    {
                        **common_meta,
                        "file_role": "provider_audio",
                        "provider": "dialpad",
                        "recording_id": candidate.get("recording_id"),
                        "recording_type": candidate.get("recording_type"),
                        "duration_ms": candidate.get("duration_ms"),
                        "source_url": final_url,
                        "source": candidate.get("source"),
                    },
                    byte_size,
                )
            )
            results.append(
                {
                    "index": index,
                    "status": "downloaded",
                    "path": str(audio_path),
                    "recording_id": candidate.get("recording_id"),
                    "recording_type": candidate.get("recording_type"),
                    "source_url": final_url,
                    "bytes": byte_size,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "index": index,
                    "status": "failed",
                    "recording_id": candidate.get("recording_id"),
                    "recording_type": candidate.get("recording_type"),
                    "source": candidate.get("source"),
                    "error": str(exc),
                }
            )

    return manifests, results


@register("ARCHIVE_EXPORT", "archive_export")
async def handle_archive_export(job: Dict[str, Any]) -> Dict[str, Any]:
    payload = job.get("payload") or {}
    event_uuid = str(job.get("event_uuid") or payload.get("event_uuid") or "")
    channel = _archive_channel(payload)
    occurred_at = _parse_occurred_at(payload.get("occurred_at"))
    source_provenance = payload.get("source_provenance") or {}
    company_slug = _company_slug(source_provenance)
    phone_bucket = _safe_component(_conversation_number(source_provenance), "unknown_number")
    event_stem = _safe_component(
        _event_stem(phone_bucket, channel, occurred_at, source_provenance),
        "unknown_event",
    )

    archive_dir = (
        ARCHIVE_ROOT
        / phone_bucket
        / _channel_dir(channel)
        / event_stem
    )
    archive_dir.mkdir(parents=True, exist_ok=True)

    manifest: List[Dict[str, Any]] = []
    common_meta = {
        "event_uuid": event_uuid,
        "channel": channel,
        "phone_bucket": phone_bucket,
        "event_stem": event_stem,
        "company_slug": company_slug,
    }

    metadata_payload = {
        "event_uuid": event_uuid,
        "external_id": payload.get("external_id"),
        "channel": channel,
        "subject": payload.get("subject"),
        "occurred_at": payload.get("occurred_at"),
        "event_stem": event_stem,
        "archive_timezone": ARCHIVE_TIMEZONE_NAME,
        "source_provenance": source_provenance,
        "company_slug": company_slug,
        "recording_references": payload.get("recording_references") or [],
    }
    metadata_path = archive_dir / f"{event_stem}_metadata.json"
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
        event_path = archive_dir / f"{event_stem}_event.json"
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
        transcript_path = archive_dir / f"{event_stem}_provider_transcript.txt"
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
        sms_path = archive_dir / f"{event_stem}_text.txt"
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
        refs_path = archive_dir / f"{event_stem}_recording_references.json"
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

    audio_manifests, audio_results = await _download_recordings(
        payload,
        archive_dir,
        event_stem,
        common_meta,
    )
    manifest.extend(audio_manifests)

    sync_result = await _sync_archive_dir(archive_dir)

    return {
        "job_uuid": job.get("job_uuid"),
        "event_uuid": event_uuid,
        "archive_dir": str(archive_dir),
        "archive_files": manifest,
        "provider_transcript_preserved": bool(provider_transcript_text),
        "sms_text_preserved": bool(sms_text),
        "recording_reference_count": len(recording_references or []),
        "audio_downloads": audio_results,
        "archive_sync": sync_result,
    }

"""modules/archive_export - preserve Dialpad event files to the shared archive."""
from __future__ import annotations

import asyncio
import csv
import io
import json
import mimetypes
import os
import re
import shlex
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
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
DIALPAD_CLIENT_ID = os.environ.get("DIALPAD_CLIENT_ID", "").strip()
DIALPAD_CLIENT_SECRET = os.environ.get("DIALPAD_CLIENT_SECRET", "").strip()
DIALPAD_REFRESH_TOKEN = os.environ.get("DIALPAD_REFRESH_TOKEN", "").strip()
DIALPAD_ACCESS_TOKEN = os.environ.get("DIALPAD_ACCESS_TOKEN", "").strip()
DIALPAD_RECORDING_SHARE_PRIVACY = os.environ.get("DIALPAD_RECORDING_SHARE_PRIVACY", "company").strip() or "company"
DIALPAD_STATS_POLL_WAIT = max(0, int(os.environ.get("DIALPAD_POLL_WAIT", "5") or "5"))
DIALPAD_STATS_POLL_MAX = max(1, int(os.environ.get("DIALPAD_POLL_MAX", "8") or "8"))
DIALPAD_STATS_POLL_RETRY = max(1, int(os.environ.get("DIALPAD_POLL_RETRY", "5") or "5"))
DIALPAD_API_BASE = "https://dialpad.com/api/v2"
DIALPAD_TOKEN_URL = "https://dialpad.com/oauth2/token"
DIALPAD_COMPANY_ID = os.environ.get("DIALPAD_COMPANY_ID", "").strip()
_dialpad_token_lock = asyncio.Lock()
_dialpad_access_token = DIALPAD_ACCESS_TOKEN
_dialpad_refresh_token = DIALPAD_REFRESH_TOKEN


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


def _load_json_file(path: Path) -> Optional[Dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


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


def _manifest_path(archive_dir: Path, event_stem: str) -> Path:
    return archive_dir / f"{event_stem}_manifest.json"


def _load_manifest_entries(archive_dir: Path, event_stem: str) -> List[Dict[str, Any]]:
    manifest_path = _manifest_path(archive_dir, event_stem)
    payload = _load_json_file(manifest_path)
    entries = payload.get("archive_files") if isinstance(payload, dict) else None
    if isinstance(entries, list):
        return [entry for entry in entries if isinstance(entry, dict)]
    return []


def _infer_manifest_entry(
    path: Path,
    common_meta: Dict[str, Any],
    audio_hints: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    suffix = path.suffix.lower()
    metadata = dict(common_meta)
    content_type = "application/octet-stream"
    if path.name.endswith("_metadata.json"):
        metadata["file_role"] = "metadata"
        content_type = "application/json"
    elif path.name.endswith("_event.json"):
        metadata["file_role"] = "event_source_metadata"
        content_type = "application/json"
    elif path.name.endswith("_provider_transcript.txt"):
        metadata["file_role"] = "provider_transcript"
        metadata["provider"] = "dialpad"
        content_type = "text/plain"
    elif path.name.endswith("_text.txt"):
        metadata["file_role"] = "sms_text"
        metadata["provider"] = "dialpad"
        content_type = "text/plain"
    elif path.name.endswith("_recording_references.json"):
        metadata["file_role"] = "recording_references"
        content_type = "application/json"
    elif path.name.endswith("_manifest.json"):
        metadata["file_role"] = "archive_manifest"
        content_type = "application/json"
    elif suffix in {".mp3", ".wav", ".m4a", ".aac", ".ogg", ".webm"}:
        metadata["file_role"] = "provider_audio"
        metadata["provider"] = "dialpad"
        if audio_hints:
            metadata.update(audio_hints)
        content_type = mimetypes.guess_type(str(path))[0] or "audio/mpeg"
    else:
        return None

    return _manifest_entry(path, content_type, metadata, path.stat().st_size)


def _rebuild_manifest_entries(archive_dir: Path, event_stem: str, common_meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    audio_hints: Dict[str, Any] = {}
    for refs_path in sorted(archive_dir.glob("*_recording_references.json")):
        payload = _load_json_file(refs_path) or {}
        refs = payload.get("recording_references") or []
        if not isinstance(refs, list):
            continue
        for ref in refs:
            if not isinstance(ref, dict):
                continue
            metadata = ref.get("metadata") or {}
            if not audio_hints.get("recording_id"):
                audio_hints["recording_id"] = (
                    metadata.get("recording_id")
                    or metadata.get("voicemail_recording_id")
                )
            if not audio_hints.get("source_url") and _is_http_url(ref.get("storage_uri")):
                audio_hints["source_url"] = ref.get("storage_uri")
            if not audio_hints.get("recording_type"):
                audio_hints["recording_type"] = metadata.get("recording_type") or (
                    "voicemail" if "voicemail" in str(metadata.get("reference_type") or "") else None
                )
        if audio_hints:
            break

    for path in sorted(archive_dir.iterdir()):
        if not path.is_file():
            continue
        if path == _manifest_path(archive_dir, event_stem):
            continue
        entry = _infer_manifest_entry(path, common_meta, audio_hints=audio_hints)
        if entry is not None:
            entries.append(entry)
    return entries


def _merge_manifest_entries(existing: List[Dict[str, Any]], current: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    for entry in existing + current:
        storage_uri = str(entry.get("storage_uri") or "")
        if not storage_uri:
            continue
        merged[storage_uri] = entry
    return list(merged.values())


def _find_existing_archive_dir(phone_channel_root: Path, event_uuid: str, external_id: str) -> Optional[Path]:
    if not phone_channel_root.exists():
        return None
    for metadata_path in sorted(phone_channel_root.rglob("*_metadata.json")):
        payload = _load_json_file(metadata_path)
        if not payload:
            continue
        if payload.get("event_uuid") == event_uuid:
            return metadata_path.parent
        if external_id and payload.get("external_id") == external_id:
            return metadata_path.parent
    return None


def _existing_event_stem(archive_dir: Path) -> Optional[str]:
    for metadata_path in sorted(archive_dir.glob("*_metadata.json")):
        payload = _load_json_file(metadata_path)
        if not payload:
            continue
        event_stem = str(payload.get("event_stem") or "").strip()
        if event_stem:
            return event_stem
    return archive_dir.name


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


def _is_http_url(value: Any) -> bool:
    text = str(value or "").strip()
    return text.startswith("http://") or text.startswith("https://")


def _is_dialpad_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and parsed.netloc.endswith("dialpad.com")


async def _refresh_dialpad_access_token() -> Optional[str]:
    global _dialpad_access_token, _dialpad_refresh_token

    if not (DIALPAD_CLIENT_ID and DIALPAD_CLIENT_SECRET and _dialpad_refresh_token):
        return None

    async with _dialpad_token_lock:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            response = await client.post(
                DIALPAD_TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": _dialpad_refresh_token,
                    "client_id": DIALPAD_CLIENT_ID,
                    "client_secret": DIALPAD_CLIENT_SECRET,
                },
                headers={"Accept": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()
            access_token = str(payload.get("access_token") or "").strip()
            if not access_token:
                return None
            _dialpad_access_token = access_token
            refreshed = str(payload.get("refresh_token") or "").strip()
            if refreshed:
                _dialpad_refresh_token = refreshed
            return _dialpad_access_token


async def _dialpad_bearer(force_refresh: bool = False) -> str:
    global _dialpad_access_token

    if force_refresh:
        refreshed = await _refresh_dialpad_access_token()
        return refreshed or _dialpad_access_token

    if _dialpad_access_token:
        return _dialpad_access_token

    refreshed = await _refresh_dialpad_access_token()
    return refreshed or ""


async def _dialpad_headers(force_refresh: bool = False) -> Dict[str, str]:
    headers = {
        "Accept": "application/json",
    }
    token = await _dialpad_bearer(force_refresh=force_refresh)
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


async def _dialpad_request(
    method: str,
    url: str,
    *,
    client: Optional[httpx.AsyncClient] = None,
    retry_on_401: bool = True,
    **kwargs: Any,
) -> httpx.Response:
    async def _once(force_refresh: bool) -> httpx.Response:
        local_client = client
        close_client = False
        if local_client is None:
            local_client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
            close_client = True
        try:
            request_kwargs = dict(kwargs)
            headers = dict(request_kwargs.pop("headers", {}))
            headers.update(await _dialpad_headers(force_refresh=force_refresh))
            response = await local_client.request(method, url, headers=headers, **request_kwargs)
            return response
        finally:
            if close_client:
                await local_client.aclose()

    first = await _once(False)
    if first.status_code != 401 or not retry_on_401:
        return first
    second = await _once(True)
    return second


def _dedupe_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    deduped: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for candidate in candidates:
        key = (
            str(candidate.get("url") or ""),
            str(candidate.get("recording_id") or ""),
            str(candidate.get("recording_type") or ""),
            str(candidate.get("source") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _call_id_for_payload(payload: Dict[str, Any]) -> str:
    source_metadata = payload.get("source_metadata") or {}
    source_provenance = payload.get("source_provenance") or {}
    return str(
        source_metadata.get("call_id")
        or source_provenance.get("call_id")
        or payload.get("external_id")
        or ""
    ).strip()


def _stats_target_candidates(payload: Dict[str, Any]) -> List[tuple[str, str]]:
    source_provenance = payload.get("source_provenance") or {}
    target = source_provenance.get("target") or {}
    candidates: List[tuple[str, str]] = []
    office_id = str(target.get("office_id") or "").strip()
    target_type = str(target.get("type") or "").strip().lower()
    target_id = str(target.get("id") or "").strip()
    if office_id:
        candidates.append(("office", office_id))
    if target_type == "user" and target_id:
        candidates.append(("user", target_id))
    if DIALPAD_COMPANY_ID:
        candidates.append(("company", DIALPAD_COMPANY_ID))
    return candidates


def _stats_date_range(payload: Dict[str, Any]) -> tuple[str, str]:
    occurred_at = _parse_occurred_at(payload.get("occurred_at"))
    local_day = occurred_at.astimezone(_archive_timezone()).date().isoformat()
    return local_day, local_day


async def _fetch_stats_rows(
    stat_type: str,
    payload: Dict[str, Any],
) -> List[Dict[str, Any]]:
    if not (DIALPAD_ACCESS_TOKEN or DIALPAD_REFRESH_TOKEN):
        return []

    date_start, date_end = _stats_date_range(payload)
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        for target_type, target_id in _stats_target_candidates(payload):
            try:
                response = await _dialpad_request(
                    "POST",
                    f"{DIALPAD_API_BASE}/stats",
                    client=client,
                    json={
                        "export_type": "records",
                        "stat_type": stat_type,
                        "target_type": target_type,
                        "target_id": target_id,
                        "date_start": date_start,
                        "date_end": date_end,
                        "timezone": ARCHIVE_TIMEZONE_NAME,
                    },
                    headers={"Content-Type": "application/json"},
                )
                response.raise_for_status()
                body = response.json()
                request_id = body.get("request_id") or body.get("id")
                if not request_id:
                    continue

                if DIALPAD_STATS_POLL_WAIT:
                    await asyncio.sleep(DIALPAD_STATS_POLL_WAIT)

                for _ in range(DIALPAD_STATS_POLL_MAX):
                    poll_response = await _dialpad_request(
                        "GET",
                        f"{DIALPAD_API_BASE}/stats/{request_id}",
                        client=client,
                    )
                    poll_response.raise_for_status()
                    poll_body = poll_response.json()
                    download_url = (
                        poll_body.get("download_url")
                        or poll_body.get("url")
                        or poll_body.get("file_url")
                    )
                    if download_url:
                        download_response = await _dialpad_request("GET", download_url, client=client)
                        download_response.raise_for_status()
                        reader = csv.DictReader(io.StringIO(download_response.text.lstrip("\ufeff")))
                        rows = [
                            {
                                str(key or "").strip().strip('"'): str(value or "").strip().strip('"')
                                for key, value in row.items()
                            }
                            for row in reader
                        ]
                        if not rows:
                            return []
                        return rows
                    state = str(poll_body.get("state") or poll_body.get("status") or "").lower()
                    if state in {"failed", "error"}:
                        break
                    await asyncio.sleep(DIALPAD_STATS_POLL_RETRY)
            except Exception:
                continue
    return []


def _match_stats_row(rows: List[Dict[str, Any]], payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    call_id = _call_id_for_payload(payload)
    external_id = str(payload.get("external_id") or "").strip()
    for row in rows:
        row_call_id = str(row.get("call_id") or row.get("id") or "").strip()
        if call_id and row_call_id == call_id:
            return row
        if external_id and row_call_id == external_id:
            return row
    return None


async def _fetch_call_detail(call_id: str) -> Dict[str, Any]:
    if not (call_id and (DIALPAD_ACCESS_TOKEN or DIALPAD_REFRESH_TOKEN)):
        return {}

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        for path in (f"{DIALPAD_API_BASE}/call/{call_id}", f"{DIALPAD_API_BASE}/calls/{call_id}"):
            try:
                response = await _dialpad_request("GET", path, client=client)
                response.raise_for_status()
                body = response.json()
                if isinstance(body, dict):
                    return body
            except Exception:
                continue
    return {}


def _detail_candidates(detail: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    if not isinstance(detail, dict):
        return candidates

    direct_url = detail.get("recording_url") or detail.get("recording")
    if _is_http_url(direct_url):
        candidates.append(
            {
                "url": direct_url,
                "recording_id": detail.get("recording_id"),
                "recording_type": detail.get("recording_type") or "callrecording",
                "duration_ms": detail.get("duration"),
                "start_time": detail.get("start_time"),
                "source": "call_detail_direct",
            }
        )

    for detail_entry in detail.get("recording_details") or []:
        if not isinstance(detail_entry, dict):
            continue
        candidates.append(
            {
                "url": detail_entry.get("url"),
                "recording_id": detail_entry.get("id"),
                "recording_type": detail_entry.get("recording_type") or "callrecording",
                "duration_ms": detail_entry.get("duration"),
                "start_time": detail_entry.get("start_time"),
                "source": "call_detail_recording_details",
            }
        )

    for field_name in ("admin_recording_urls", "call_recording_share_links", "admin_call_recording_share_links"):
        field_value = detail.get(field_name)
        if isinstance(field_value, list):
            for item in field_value:
                if _is_http_url(item):
                    candidates.append(
                        {
                            "url": item,
                            "recording_id": detail.get("recording_id"),
                            "recording_type": "admincallrecording",
                            "duration_ms": detail.get("duration"),
                            "start_time": detail.get("start_time"),
                            "source": field_name,
                        }
                    )
                elif isinstance(item, dict):
                    candidates.append(
                        {
                            "url": item.get("url") or item.get("access_link") or item.get("link"),
                            "recording_id": item.get("recording_id") or detail.get("recording_id"),
                            "recording_type": item.get("recording_type") or "admincallrecording",
                            "duration_ms": item.get("duration") or detail.get("duration"),
                            "start_time": item.get("start_time") or detail.get("start_time"),
                            "source": field_name,
                        }
                    )

    voicemail_share_link = detail.get("voicemail_share_link")
    if _is_http_url(voicemail_share_link):
        candidates.append(
            {
                "url": voicemail_share_link,
                "recording_id": detail.get("voicemail_recording_id"),
                "recording_type": "voicemail",
                "duration_ms": detail.get("duration"),
                "start_time": detail.get("start_time"),
                "source": "call_detail_voicemail_share_link",
            }
        )
    return candidates


def _recording_candidates(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    source_metadata = payload.get("source_metadata") or {}
    recording_references = payload.get("recording_references") or []

    direct_url = source_metadata.get("recording_url")
    if _is_http_url(direct_url):
        candidates.append(
            {
                "url": direct_url,
                "recording_id": source_metadata.get("voicemail_recording_id"),
                "recording_type": "voicemail",
                "duration_ms": source_metadata.get("duration"),
                "start_time": source_metadata.get("date_started"),
                "source": "source_metadata_recording_url",
            }
        )

    voicemail_link = source_metadata.get("voicemail_link")
    if _is_http_url(voicemail_link):
        candidates.append(
            {
                "url": voicemail_link,
                "recording_id": source_metadata.get("voicemail_recording_id"),
                "recording_type": "voicemail",
                "duration_ms": source_metadata.get("duration"),
                "start_time": source_metadata.get("date_started"),
                "source": "source_metadata_voicemail_link",
            }
        )

    for detail in source_metadata.get("recording_details") or []:
        if not isinstance(detail, dict):
            continue
        candidates.append(
            {
                "url": detail.get("url"),
                "recording_id": detail.get("id"),
                "recording_type": detail.get("recording_type"),
                "duration_ms": detail.get("duration"),
                "start_time": detail.get("start_time"),
                "source": "recording_details",
            }
        )

    for admin_url in source_metadata.get("admin_recording_urls") or []:
        if _is_http_url(admin_url):
            candidates.append(
                {
                    "url": admin_url,
                    "recording_id": source_metadata.get("recording_id"),
                    "recording_type": "admincallrecording",
                    "duration_ms": source_metadata.get("duration"),
                    "start_time": source_metadata.get("date_started"),
                    "source": "source_metadata_admin_recording_urls",
                }
            )

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
        candidates.append(candidate)

    return _dedupe_candidates(candidates)


async def _dialpad_api_request(method: str, url: str, json_body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        response = await _dialpad_request(method, url, client=client, json=json_body, headers=headers)
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
    if not (recording_id and recording_type and (DIALPAD_ACCESS_TOKEN or DIALPAD_REFRESH_TOKEN)):
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


async def _resolve_payload_recordings(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    channel = _archive_channel(payload)
    candidates = list(_recording_candidates(payload))
    call_id = _call_id_for_payload(payload)

    if call_id and (DIALPAD_ACCESS_TOKEN or DIALPAD_REFRESH_TOKEN):
        call_detail = await _fetch_call_detail(call_id)
        candidates.extend(_detail_candidates(call_detail))

    if DIALPAD_ACCESS_TOKEN or DIALPAD_REFRESH_TOKEN:
        if channel == "voicemail":
            voicemail_rows = await _fetch_stats_rows("voicemails", payload)
            voicemail_row = _match_stats_row(voicemail_rows, payload)
            if voicemail_row:
                candidates.append(
                    {
                        "url": voicemail_row.get("recording_url"),
                        "recording_id": voicemail_row.get("voicemail_recording_id") or voicemail_row.get("recording_id"),
                        "recording_type": "voicemail",
                        "duration_ms": voicemail_row.get("duration"),
                        "start_time": voicemail_row.get("date") or voicemail_row.get("date_started"),
                        "source": "stats_voicemails",
                    }
                )

        recordings_rows = await _fetch_stats_rows("recordings", payload)
        recordings_row = _match_stats_row(recordings_rows, payload)
        if recordings_row:
            candidates.append(
                {
                    "url": recordings_row.get("recording_url"),
                    "recording_id": recordings_row.get("recording_id"),
                    "recording_type": recordings_row.get("recording_type") or "callrecording",
                    "duration_ms": recordings_row.get("duration"),
                    "start_time": recordings_row.get("date_started") or recordings_row.get("date"),
                    "source": "stats_recordings",
                }
            )

    if call_id:
        source_metadata = payload.get("source_metadata") or {}
        if channel == "voicemail" or source_metadata.get("voicemail_recording_id"):
            candidates.append(
                {
                    "url": f"https://dialpad.com/v/{call_id}",
                    "recording_id": source_metadata.get("voicemail_recording_id"),
                    "recording_type": "voicemail",
                    "duration_ms": source_metadata.get("duration"),
                    "start_time": source_metadata.get("date_started"),
                    "source": "dialpad_v_fallback",
                }
            )
        else:
            candidates.append(
                {
                    "url": f"https://dialpad.com/v/{call_id}",
                    "recording_id": None,
                    "recording_type": "callrecording",
                    "duration_ms": source_metadata.get("duration"),
                    "start_time": source_metadata.get("date_started"),
                    "source": "dialpad_v_fallback",
                }
            )

    return _dedupe_candidates(candidates)


async def _download_audio_file(url: str) -> tuple[bytes, str, str]:
    headers = {}
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        if _is_dialpad_url(url):
            response = await _dialpad_request("GET", url, client=client, headers=headers)
        else:
            response = await client.get(url, headers=headers)
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if not content_type.lower().startswith("audio/"):
            raise ValueError(f"unexpected content-type {content_type or 'unknown'}")
        return response.content, content_type, str(response.url)


def _existing_audio_entry(
    manifest_entries: List[Dict[str, Any]],
    candidate: Dict[str, Any],
    resolved_url: str,
) -> Optional[Dict[str, Any]]:
    candidate_recording_id = str(candidate.get("recording_id") or "").strip()
    candidate_source = str(resolved_url or "").strip()
    for entry in manifest_entries:
        metadata = entry.get("metadata") or {}
        if metadata.get("file_role") != "provider_audio":
            continue
        existing_recording_id = str(metadata.get("recording_id") or "").strip()
        existing_source = str(metadata.get("source_url") or "").strip()
        if candidate_recording_id and existing_recording_id == candidate_recording_id:
            return entry
        if candidate_source and existing_source == candidate_source:
            return entry
    return None


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
    existing_manifest_entries: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    manifests: List[Dict[str, Any]] = []
    results: List[Dict[str, Any]] = []
    candidates = await _resolve_payload_recordings(payload)

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

            existing_entry = _existing_audio_entry(existing_manifest_entries, candidate, resolved_url)
            if existing_entry is not None:
                results.append(
                    {
                        "index": index,
                        "status": "already_present",
                        "path": existing_entry.get("storage_uri"),
                        "recording_id": candidate.get("recording_id"),
                        "recording_type": candidate.get("recording_type"),
                        "source_url": resolved_url,
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
    external_id = str(payload.get("external_id") or "")
    phone_channel_root = ARCHIVE_ROOT / phone_bucket / _channel_dir(channel)
    existing_archive_dir = _find_existing_archive_dir(phone_channel_root, event_uuid, external_id)
    event_stem = (
        _existing_event_stem(existing_archive_dir)
        if existing_archive_dir is not None
        else _safe_component(
            _event_stem(phone_bucket, channel, occurred_at, source_provenance),
            "unknown_event",
        )
    )

    archive_dir = existing_archive_dir or (phone_channel_root / event_stem)
    archive_dir.mkdir(parents=True, exist_ok=True)

    manifest: List[Dict[str, Any]] = []
    common_meta = {
        "event_uuid": event_uuid,
        "channel": channel,
        "phone_bucket": phone_bucket,
        "event_stem": event_stem,
        "company_slug": company_slug,
    }
    existing_manifest = _load_manifest_entries(archive_dir, event_stem)
    if not existing_manifest:
        existing_manifest = _rebuild_manifest_entries(archive_dir, event_stem, common_meta)

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
        existing_manifest,
    )
    manifest.extend(audio_manifests)

    merged_manifest = _merge_manifest_entries(existing_manifest, manifest)
    manifest_path = _manifest_path(archive_dir, event_stem)
    manifest_payload = {
        "event_uuid": event_uuid,
        "archive_files": merged_manifest,
    }
    manifest_size = _write_json(manifest_path, manifest_payload)
    manifest_entry = _manifest_entry(
        manifest_path,
        "application/json",
        {
            **common_meta,
            "file_role": "archive_manifest",
        },
        manifest_size,
    )
    final_manifest = _merge_manifest_entries(merged_manifest, [manifest_entry])

    sync_result = await _sync_archive_dir(archive_dir)

    return {
        "job_uuid": job.get("job_uuid"),
        "event_uuid": event_uuid,
        "archive_dir": str(archive_dir),
        "archive_files": final_manifest,
        "provider_transcript_preserved": bool(provider_transcript_text),
        "sms_text_preserved": bool(sms_text),
        "recording_reference_count": len(recording_references or []),
        "audio_downloads": audio_results,
        "archive_sync": sync_result,
    }

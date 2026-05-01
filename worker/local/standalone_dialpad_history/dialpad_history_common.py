"""Shared helpers for standalone Dialpad historical tools.

This module is intentionally independent from the TALOS worker app so the
historical backfill and catch-up email tools can run directly from a normal
Windows virtualenv.
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import mimetypes
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional during bootstrap
    load_dotenv = None

SCRIPT_DIR = Path(__file__).resolve().parent
if load_dotenv is not None:
    load_dotenv(SCRIPT_DIR / ".env", override=False)

ARCHIVE_ROOT = Path(
    os.environ.get("DIALPAD_ARCHIVE_ROOT", r"D:\Talos_Data\Historical\Dialpad")
).expanduser()
DEFAULT_CONTACTS_CSV = Path(
    os.environ.get(
        "DIALPAD_CONTACTS_CSV",
        r"D:\Talos_Data\Historical\Contacts\2026_0408_master_contacts_clean.csv",
    )
).expanduser()
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
STATS_TIMEZONE_NAME = os.environ.get("DIALPAD_STATS_TIMEZONE", ARCHIVE_TIMEZONE_NAME).strip() or "UTC"
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
_archive_locks: Dict[str, asyncio.Lock] = {}


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


def _dialpad_stats_timezones() -> List[str]:
    candidates = [
        STATS_TIMEZONE_NAME,
        ARCHIVE_TIMEZONE_NAME,
        "UTC",
        "America/New_York",
        "US/Eastern",
    ]
    ordered: List[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        text = str(candidate or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


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


def _normalized_sms_provenance(source_provenance: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(source_provenance, dict):
        return {}
    if str(source_provenance.get("channel") or "").strip().lower() != "sms":
        return source_provenance

    normalized = dict(source_provenance)
    contact = dict(normalized.get("contact") or {})
    target = dict(normalized.get("target") or {})
    direction = str(normalized.get("direction") or "").strip().lower()
    external_number = str(normalized.get("external_number") or "").strip()
    internal_number = str(normalized.get("internal_number") or "").strip()
    from_number = str(normalized.get("from_number") or "").strip()
    to_number = str(normalized.get("to_number") or "").strip()
    selected_caller_id = str(normalized.get("selected_caller_id") or "").strip()

    if direction in {"internal", "outbound", "sent"}:
        target_phone = str(target.get("phone") or "").strip() or selected_caller_id or from_number or internal_number
        contact_phone = str(contact.get("phone") or "").strip() or external_number or to_number or internal_number
        if from_number and contact_phone == from_number and target_phone != from_number:
            target_phone = selected_caller_id or from_number or target_phone
            contact_phone = to_number or internal_number or contact_phone
        if contact_phone and target_phone and contact_phone == target_phone:
            contact_phone = to_number or internal_number or contact_phone
        normalized["internal_number"] = target_phone
        normalized["external_number"] = contact_phone
    else:
        contact_phone = str(contact.get("phone") or "").strip() or external_number or from_number
        target_phone = str(target.get("phone") or "").strip() or internal_number or to_number or selected_caller_id
        if contact_phone and target_phone and contact_phone == target_phone:
            target_phone = to_number or internal_number or target_phone
        normalized["external_number"] = contact_phone
        normalized["internal_number"] = target_phone

    if normalized.get("external_number"):
        contact["phone"] = normalized["external_number"]
    if normalized.get("internal_number"):
        target["phone"] = normalized["internal_number"]
    normalized["contact"] = contact
    normalized["target"] = target
    return normalized


def _conversation_number(source_provenance: Dict[str, Any]) -> str:
    source_provenance = _normalized_sms_provenance(source_provenance)
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
    source_metadata = payload.get("source_metadata") or {}
    if channel == "voicemail" or source_metadata.get("voicemail_recording_id") or any(
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


def _display_party(
    party: Dict[str, Any],
    *fallbacks: Optional[Any],
    default: Optional[str] = None,
) -> Optional[str]:
    name = str(party.get("name") or "").strip()
    phone = str(
        party.get("phone")
        or party.get("phone_number")
        or next((fallback for fallback in fallbacks if str(fallback or "").strip()), "")
    ).strip()
    email = str(party.get("email") or "").strip()
    if name and phone and name != phone:
        return f"{name} ({phone})"
    if name:
        return name
    if phone:
        return phone
    if email:
        return email
    return default


def _contact_display(source_provenance: Dict[str, Any], default: Optional[str] = None) -> Optional[str]:
    source_provenance = _normalized_sms_provenance(source_provenance)
    contact = source_provenance.get("contact") or {}
    return _display_party(
        contact,
        source_provenance.get("external_number"),
        source_provenance.get("from_number"),
        default=default,
    )


def _target_display(source_provenance: Dict[str, Any], default: Optional[str] = None) -> Optional[str]:
    source_provenance = _normalized_sms_provenance(source_provenance)
    target = source_provenance.get("target") or {}
    return _display_party(
        target,
        source_provenance.get("internal_number"),
        source_provenance.get("selected_caller_id"),
        default=default,
    )


def _render_sms_text(payload: Dict[str, Any], sms_text: str) -> Optional[str]:
    body = str(sms_text or "").strip()
    if not body:
        return None
    occurred_at = _parse_occurred_at(payload.get("occurred_at"))
    time_display = occurred_at.astimezone(_archive_timezone()).strftime("%Y-%m-%d %H:%M:%S %Z")
    if body.startswith("From: ") and "\nTo: " in body:
        if body.startswith("Time: "):
            return body
        return f"Time: {time_display}\n{body}"

    source_provenance = payload.get("source_provenance") or {}
    direction = str(source_provenance.get("direction") or payload.get("direction") or "").strip().lower()
    contact_display = _contact_display(source_provenance, default="Unknown") or "Unknown"
    target_display = _target_display(source_provenance, default="Unknown") or "Unknown"
    if direction in {"internal", "outbound", "sent"}:
        sender_display = target_display
        recipient_display = contact_display
    else:
        sender_display = contact_display
        recipient_display = target_display
    return (
        f"Time: {time_display}\n"
        f"From: {sender_display}\n"
        f"To: {recipient_display}\n"
        f"{sender_display} -> {recipient_display}:\n\n"
        f"{body}"
    )


def _normalize_speaker_label(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    digits = re.findall(r"\d+", text)
    if digits:
        return f"Speaker {digits[0]}"
    lowered = text.lower()
    if lowered == "unknown":
        return "Unknown Speaker"
    if lowered.startswith("speaker "):
        return f"Speaker {text.split(' ', 1)[1].strip()}"
    return text


def _render_speaker_display(speaker_name: Optional[str], speaker_label: Optional[Any]) -> str:
    name = str(speaker_name or "").strip()
    label = _normalize_speaker_label(speaker_label)
    if name and label:
        if name.casefold() == label.casefold():
            return name
        return f"{name} ({label})"
    if name:
        return name
    if label:
        return label
    return "Unknown Speaker"


def _speaker_identity_from_rendered(rendered_speaker: str) -> tuple[str, str]:
    speaker = str(rendered_speaker or "").strip()
    if not speaker:
        return "", ""
    match = re.match(r"^(?P<name>.+?) \((?P<label>Speaker \d+)\)$", speaker)
    if match:
        return match.group("name").strip(), match.group("label").strip()
    normalized = _normalize_speaker_label(speaker)
    if normalized and (normalized == speaker or speaker.lower().startswith("speaker") or speaker == "Unknown Speaker"):
        return "", normalized
    return speaker, ""


def _looks_like_speaker_prefix(value: str) -> bool:
    speaker = str(value or "").strip()
    if not speaker:
        return False
    lowered = speaker.lower()
    if lowered.startswith("speaker ") or lowered == "unknown speaker":
        return True
    words = [word for word in re.split(r"\s+", speaker) if word]
    if len(words) > 4:
        return False
    return any(ch.isalpha() for ch in speaker)


def _is_unknown_speaker_label(value: Optional[str]) -> bool:
    return str(value or "").strip().lower() == "unknown speaker"


def _apply_speaker_turn_heuristic(
    segments: List[Dict[str, str]],
    *,
    assume_two_party: bool,
    fallback_speaker: Optional[str] = None,
) -> List[Dict[str, str]]:
    if not segments:
        return segments

    explicit_numbered_labels = [
        segment["speaker_label"]
        for segment in segments
        if str(segment.get("speaker_label") or "").strip().startswith("Speaker ")
    ]
    has_explicit_numbered = bool(explicit_numbered_labels)
    any_blank = any(not str(segment.get("speaker_label") or "").strip() for segment in segments)
    any_unknown = any(_is_unknown_speaker_label(segment.get("speaker_label")) for segment in segments)
    if not any_blank and not any_unknown:
        return segments
    if len(segments) < 2:
        return segments
    if not assume_two_party and not has_explicit_numbered:
        return segments

    next_speaker_number = 1
    previous_speaker_number: Optional[int] = None
    for index, segment in enumerate(segments):
        speaker_label = str(segment.get("speaker_label") or "").strip()
        rendered_speaker = str(segment.get("rendered_speaker") or "").strip()
        explicit_number = None
        if speaker_label.startswith("Speaker "):
            digits = re.findall(r"\d+", speaker_label)
            if digits:
                explicit_number = int(digits[0])
        elif _is_unknown_speaker_label(speaker_label):
            explicit_number = next_speaker_number

        if explicit_number is not None:
            previous_speaker_number = explicit_number
            next_speaker_number = 2 if explicit_number == 1 else 1
            if _is_unknown_speaker_label(speaker_label) or not speaker_label:
                segment["speaker_label"] = f"Speaker {explicit_number}"
                segment["rendered_speaker"] = _render_speaker_display(segment.get("speaker_name"), segment["speaker_label"])
            elif not rendered_speaker:
                segment["rendered_speaker"] = _render_speaker_display(segment.get("speaker_name"), speaker_label)
            continue

        if previous_speaker_number is None:
            assigned_number = 1
        else:
            assigned_number = 2 if previous_speaker_number == 1 else 1
        segment["speaker_label"] = f"Speaker {assigned_number}"
        if assigned_number == 1 and fallback_speaker and not str(segment.get("speaker_name") or "").strip():
            segment["speaker_name"] = fallback_speaker
        segment["rendered_speaker"] = _render_speaker_display(segment.get("speaker_name"), segment["speaker_label"])
        previous_speaker_number = assigned_number
        next_speaker_number = 2 if assigned_number == 1 else 1

    return segments


def _render_transcript_segments(segments: List[Dict[str, str]]) -> str:
    rendered: List[str] = []
    for segment in segments:
        content = str(segment.get("text") or "").strip()
        if not content:
            continue
        rendered_speaker = str(segment.get("rendered_speaker") or "").strip()
        timestamp = str(segment.get("timestamp") or "").strip()
        prefix = f"[{timestamp}] " if timestamp else ""
        rendered.append(f"{prefix}{rendered_speaker}: {content}" if rendered_speaker else f"{prefix}{content}")
    return "\n".join(rendered).strip()


def _transcript_segments_from_text(text: str) -> List[Dict[str, str]]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    rows = [row.strip() for row in normalized.split("\n") if row.strip()]
    segments: List[Dict[str, str]] = []
    if rows:
        for index, row in enumerate(rows, start=1):
            rendered_speaker = ""
            content = row
            match = re.match(r"^(?P<speaker>[^:\n]{1,200}):\s*(?P<content>.+)$", row)
            if match and _looks_like_speaker_prefix(match.group("speaker")):
                rendered_speaker = match.group("speaker").strip()
                content = match.group("content").strip()
            speaker_name, speaker_label = _speaker_identity_from_rendered(rendered_speaker)
            segments.append(
                {
                    "line_number": str(index),
                    "speaker_label": speaker_label,
                    "speaker_name": speaker_name,
                    "rendered_speaker": rendered_speaker,
                    "timestamp": "",
                    "text": content,
                }
            )
        return _apply_speaker_turn_heuristic(segments, assume_two_party=True)
    if normalized:
        speaker_name, speaker_label = _speaker_identity_from_rendered("")
        return [
            {
                "line_number": "1",
                "speaker_label": speaker_label,
                "speaker_name": speaker_name,
                "rendered_speaker": "",
                "timestamp": "",
                "text": normalized,
            }
        ]
    return []


def _write_transcript_csv(path: Path, text: str) -> int:
    segments = _transcript_segments_from_text(text)
    return _write_transcript_segments_csv(path, segments)


def _write_transcript_segments_csv(path: Path, segments: List[Dict[str, str]]) -> int:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["line_number", "timestamp", "speaker_label", "speaker_name", "rendered_speaker", "text"])
        for segment in segments:
            writer.writerow(
                [
                    segment["line_number"],
                    segment.get("timestamp", ""),
                    segment["speaker_label"],
                    segment["speaker_name"],
                    segment["rendered_speaker"],
                    segment["text"],
                ]
            )
    return path.stat().st_size


def _decode_transcript_csv_bytes(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _looks_like_html_document(text: str) -> bool:
    normalized = text.lstrip().lower()
    return normalized.startswith("<!doctype html") or normalized.startswith("<html")


def _transcript_segments_from_csv_text(
    text: str,
    *,
    fallback_speaker: Optional[str],
    assume_two_party: bool,
) -> List[Dict[str, str]]:
    reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")))
    if not reader.fieldnames:
        return []

    segments: List[Dict[str, str]] = []
    for index, raw_row in enumerate(reader, start=1):
        normalized_row = {
            _normalized_lookup_key(str(key or "")): value
            for key, value in raw_row.items()
            if str(key or "").strip()
        }
        line_type = str(
            normalized_row.get("type")
            or normalized_row.get("linetype")
            or normalized_row.get("segmenttype")
            or ""
        ).strip().lower()
        if line_type and line_type not in {"transcript", "utterance", "segment", "line"}:
            continue

        content = _clean_text_candidate(
            normalized_row.get("content")
            or normalized_row.get("text")
            or normalized_row.get("transcript")
            or normalized_row.get("body")
            or normalized_row.get("utterance")
            or normalized_row.get("line")
            or normalized_row.get("message")
        )
        if not content:
            continue

        speaker_name = _clean_text_candidate(
            normalized_row.get("speakername")
            or normalized_row.get("name")
            or normalized_row.get("speakerdisplayname")
            or normalized_row.get("participantname")
        ) or fallback_speaker
        speaker_label = (
            normalized_row.get("speakerlabel")
            or normalized_row.get("speakerid")
            or normalized_row.get("speakernumber")
            or normalized_row.get("speaker")
            or normalized_row.get("participant")
        )
        timestamp = _clean_text_candidate(
            normalized_row.get("time")
            or normalized_row.get("timestamp")
            or normalized_row.get("starttime")
            or normalized_row.get("start")
            or normalized_row.get("begintime")
            or normalized_row.get("utterancetime")
        ) or ""
        rendered_speaker = _render_speaker_display(speaker_name, speaker_label)
        segments.append(
            {
                "line_number": str(
                    normalized_row.get("linenumber")
                    or normalized_row.get("line")
                    or normalized_row.get("index")
                    or index
                ),
                "speaker_label": str(_normalize_speaker_label(speaker_label) or ""),
                "speaker_name": str(speaker_name or ""),
                "rendered_speaker": rendered_speaker,
                "timestamp": timestamp,
                "text": content,
            }
        )

    return _apply_speaker_turn_heuristic(
        segments,
        assume_two_party=assume_two_party,
        fallback_speaker=fallback_speaker,
    )


def _render_transcript_from_csv_bytes(
    content: bytes,
    *,
    fallback_speaker: Optional[str],
    assume_two_party: bool,
) -> tuple[Optional[str], List[Dict[str, str]]]:
    transcript_csv_text = _decode_transcript_csv_bytes(content)
    segments = _transcript_segments_from_csv_text(
        transcript_csv_text,
        fallback_speaker=fallback_speaker,
        assume_two_party=assume_two_party,
    )
    rendered = _render_transcript_segments(segments)
    return rendered or None, segments


def _provider_transcript_json_payload(
    payload: Dict[str, Any],
    *,
    event_uuid: str,
    source_mode: Optional[str],
    rendered_text: Optional[str],
    segments: List[Dict[str, str]],
    fallback_speaker: Optional[str],
) -> Dict[str, Any]:
    transcript_segments = segments or _transcript_segments_from_text(str(rendered_text or ""))
    normalized_segments: List[Dict[str, Any]] = []
    for index, segment in enumerate(transcript_segments, start=1):
        content = str(segment.get("text") or "").strip()
        if not content:
            continue
        speaker_name = str(segment.get("speaker_name") or "").strip() or None
        speaker_label = str(segment.get("speaker_label") or "").strip() or None
        rendered_speaker = str(segment.get("rendered_speaker") or "").strip() or None
        timestamp = str(segment.get("timestamp") or "").strip() or None
        normalized_segments.append(
            {
                "line_number": index,
                "timestamp": timestamp,
                "speaker_name": speaker_name,
                "speaker_label": speaker_label,
                "rendered_speaker": rendered_speaker,
                "speaker_identity_locked": False,
                "speaker_name_source": "provider_or_heuristic" if (speaker_name or speaker_label) else "unknown",
                "text": content,
            }
        )
    return {
        "event_uuid": event_uuid,
        "channel": _archive_channel(payload),
        "provider": "dialpad",
        "source_mode": source_mode,
        "speaker_identity_locked": False,
        "speaker_identity_note": (
            "Speaker names are provisional hints from Dialpad/provider data or archive heuristics "
            "until voice fingerprints are trained."
        ),
        "fallback_speaker": fallback_speaker,
        "contact_display": _contact_display(payload.get("source_provenance") or {}),
        "target_display": _target_display(payload.get("source_provenance") or {}),
        "rendered_text": str(rendered_text or "").strip() or None,
        "segments": normalized_segments,
    }


def _write_json(path: Path, payload: Dict[str, Any]) -> int:
    rendered = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    path.write_text(rendered, encoding="utf-8")
    return path.stat().st_size


def _normalized_lookup_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _iter_named_values(obj: Any, target_keys: List[str] | tuple[str, ...]) -> List[Any]:
    normalized_targets = {_normalized_lookup_key(key) for key in target_keys}
    results: List[Any] = []

    def _walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if _normalized_lookup_key(key) in normalized_targets:
                    results.append(child)
                _walk(child)
        elif isinstance(value, list):
            for child in value:
                _walk(child)

    _walk(obj)
    return results


def _clean_text_candidate(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    text = value.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not text:
        return None
    lowered = text.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return None
    return text


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
    elif path.name.endswith("_provider_transcript.csv"):
        metadata["file_role"] = "provider_transcript_csv"
        metadata["provider"] = "dialpad"
        content_type = "text/csv"
    elif path.name.endswith("_provider_transcript.json"):
        metadata["file_role"] = "provider_transcript_json"
        metadata["provider"] = "dialpad"
        content_type = "application/json"
    elif path.name.endswith("_provider_transcript_normalized.csv"):
        metadata["file_role"] = "provider_transcript_csv_normalized"
        metadata["provider"] = "dialpad"
        content_type = "text/csv"
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


def _find_matching_archive_dirs(event_uuid: str, external_id: str) -> List[Path]:
    matches: List[Path] = []
    if not ARCHIVE_ROOT.exists():
        return matches
    for metadata_path in sorted(ARCHIVE_ROOT.rglob("*_metadata.json")):
        payload = _load_json_file(metadata_path)
        if not payload:
            continue
        if payload.get("event_uuid") == event_uuid or (external_id and payload.get("external_id") == external_id):
            archive_dir = metadata_path.parent
            if archive_dir not in matches:
                matches.append(archive_dir)
    return matches


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
    preferred_source_rank = {
        "call_detail_recording_details": 0,
        "recording_detail_url": 1,
        "stats_recordings": 2,
        "stats_voicemails": 2,
        "recording_details": 3,
        "call_detail_direct": 4,
        "call_detail_admin_recording_urls": 5,
        "call_detail_admin_call_recording_share_links": 5,
        "call_detail_call_recording_share_links": 5,
        "source_metadata_recording_url": 6,
        "source_metadata_voicemail_link": 6,
        "source_metadata_admin_recording_urls": 7,
        "dialpad_v_fallback": 9,
    }

    def _recording_id_from_url(url: str) -> str:
        match = re.search(
            r"/blob/(?:adminrecording|callrecording|voicemail(?:_recording)?)/(\d+)(?:\.[A-Za-z0-9]+)?(?:$|[?#])",
            url,
            re.IGNORECASE,
        )
        return match.group(1) if match else ""

    def _normalized_source_url(url: str) -> str:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return ""
        canonical_id = _recording_id_from_url(url)
        if canonical_id:
            recording_type = "callrecording"
            lowered_path = parsed.path.lower()
            if "adminrecording" in lowered_path:
                recording_type = "admincallrecording"
            elif "voicemail" in lowered_path:
                recording_type = "voicemail"
            return f"{parsed.netloc.lower()}:{recording_type}:{canonical_id}"
        return f"{parsed.netloc.lower()}{parsed.path}"

    def _candidate_recording_id(candidate: Dict[str, Any]) -> str:
        explicit = str(candidate.get("recording_id") or "").strip()
        if explicit:
            return explicit
        return _recording_id_from_url(str(candidate.get("url") or "").strip())

    def _candidate_identity_key(candidate: Dict[str, Any]) -> tuple[str, str, str]:
        recording_type = str(candidate.get("recording_type") or "").strip()
        recording_id = _candidate_recording_id(candidate)
        if recording_id:
            return ("recording", recording_type, recording_id)
        normalized_url = _normalized_source_url(str(candidate.get("url") or "").strip())
        return ("url", recording_type, normalized_url)

    def _candidate_rank(candidate: Dict[str, Any]) -> tuple[int, int, int]:
        source = str(candidate.get("source") or "").strip()
        rank = preferred_source_rank.get(source, 50)
        has_recording_id = 0 if _candidate_recording_id(candidate) else 1
        has_duration = 0 if candidate.get("duration_ms") not in (None, "", 0, 0.0) else 1
        return (rank, has_recording_id, has_duration)

    candidates = [dict(candidate) for candidate in candidates]

    unique_ids_by_type: Dict[str, set[str]] = {}
    for candidate in candidates:
        recording_type = str(candidate.get("recording_type") or "").strip()
        recording_id = _candidate_recording_id(candidate)
        if recording_type and recording_id:
            unique_ids_by_type.setdefault(recording_type, set()).add(recording_id)

    for candidate in candidates:
        if _candidate_recording_id(candidate):
            candidate["recording_id"] = _candidate_recording_id(candidate)
            continue
        recording_type = str(candidate.get("recording_type") or "").strip()
        type_ids = unique_ids_by_type.get(recording_type) or set()
        if len(type_ids) == 1:
            candidate["recording_id"] = next(iter(type_ids))

    deduped_by_key: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    ordered_keys: List[tuple[str, str, str]] = []
    for candidate in candidates:
        key = _candidate_identity_key(candidate)
        existing = deduped_by_key.get(key)
        if existing is None:
            deduped_by_key[key] = candidate
            ordered_keys.append(key)
            continue
        if _candidate_rank(candidate) < _candidate_rank(existing):
            deduped_by_key[key] = candidate

    return [deduped_by_key[key] for key in ordered_keys]


def _call_id_for_payload(payload: Dict[str, Any]) -> str:
    source_metadata = payload.get("source_metadata") or {}
    source_provenance = payload.get("source_provenance") or {}
    return str(
        source_metadata.get("call_id")
        or source_provenance.get("call_id")
        or payload.get("external_id")
        or ""
    ).strip()


def _inferred_transcript_speaker(payload: Dict[str, Any]) -> Optional[str]:
    source_provenance = payload.get("source_provenance") or {}
    channel = _archive_channel(payload)
    direction = str(source_provenance.get("direction") or payload.get("direction") or "").strip().lower()
    if channel == "voicemail":
        if direction in {"outbound", "sent", "internal"}:
            return _target_display(source_provenance)
        return str(source_provenance.get("external_number") or "").strip() or None
    return None


def _structured_transcript_lines(candidate: Any) -> List[Dict[str, Any]]:
    if isinstance(candidate, dict):
        direct_lines = candidate.get("lines")
        if isinstance(direct_lines, list):
            return [line for line in direct_lines if isinstance(line, dict)]
        for value in _iter_named_values(candidate, ("lines", "transcript_lines", "segments")):
            if isinstance(value, list):
                return [line for line in value if isinstance(line, dict)]
    return []


def _structured_transcript_segments(candidate: Any, fallback_speaker: Optional[str]) -> List[Dict[str, str]]:
    lines = _structured_transcript_lines(candidate)
    if not lines:
        return []
    segments: List[Dict[str, str]] = []
    for index, line in enumerate(lines, start=1):
        line_type = str(line.get("type") or "").strip().lower()
        if line_type and line_type not in {"transcript", "utterance", "segment"}:
            continue
        content = _clean_text_candidate(
            line.get("content") or line.get("text") or line.get("transcript") or line.get("body")
        )
        if not content:
            continue
        speaker_name = _clean_text_candidate(line.get("speaker_name") or line.get("name")) or fallback_speaker
        speaker_label = line.get("speaker_label") or line.get("speaker") or line.get("speaker_id")
        timestamp = _clean_text_candidate(
            line.get("time")
            or line.get("timestamp")
            or line.get("start_time")
            or line.get("start")
            or line.get("utterance_time")
        ) or ""
        if not speaker_label and fallback_speaker:
            speaker_label = "Speaker 1"
        rendered_speaker = _render_speaker_display(speaker_name, speaker_label)
        segments.append(
            {
                "line_number": str(index),
                "speaker_label": str(_normalize_speaker_label(speaker_label) or ""),
                "speaker_name": str(speaker_name or ""),
                "rendered_speaker": rendered_speaker,
                "timestamp": timestamp,
                "text": content,
            }
        )
    return _apply_speaker_turn_heuristic(
        segments,
        assume_two_party=True,
        fallback_speaker=fallback_speaker,
    )


def _render_structured_transcript(candidate: Any, fallback_speaker: Optional[str]) -> Optional[str]:
    segments = _structured_transcript_segments(candidate, fallback_speaker)
    rendered = _render_transcript_segments(segments)
    return rendered or None


def _looks_like_labeled_transcript(text: str) -> bool:
    first_line = next((line.strip() for line in text.splitlines() if line.strip()), "")
    return bool(re.match(r"^[^:\n]{1,200}:\s+\S", first_line))


def _archive_lock_key(payload: Dict[str, Any]) -> str:
    event_uuid = str(payload.get("event_uuid") or "").strip()
    external_id = str(payload.get("external_id") or "").strip()
    phone_bucket = _safe_component(
        _conversation_number(payload.get("source_provenance") or {}),
        "unknown_number",
    )
    channel = _archive_channel(payload)
    return event_uuid or f"{channel}:{phone_bucket}:{external_id or 'unknown'}"


def _archive_lock_for(payload: Dict[str, Any]) -> asyncio.Lock:
    key = _archive_lock_key(payload)
    lock = _archive_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _archive_locks[key] = lock
    return lock


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
            for stats_timezone in _dialpad_stats_timezones():
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
                            "timezone": stats_timezone,
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
                except httpx.HTTPStatusError as exc:
                    body = exc.response.text if exc.response is not None else ""
                    if exc.response is not None and exc.response.status_code == 400 and "Invalid timezone name" in body:
                        continue
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
    if channel == "sms":
        return []
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
    def _recording_id_from_url(url: str) -> str:
        match = re.search(
            r"/blob/(?:adminrecording|callrecording|voicemail(?:_recording)?)/(\d+)(?:\.[A-Za-z0-9]+)?(?:$|[?#])",
            url,
            re.IGNORECASE,
        )
        return match.group(1) if match else ""

    def _normalized_source_url(url: str) -> str:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return ""
        canonical_id = _recording_id_from_url(url)
        if canonical_id:
            recording_type = "callrecording"
            lowered_path = parsed.path.lower()
            if "adminrecording" in lowered_path:
                recording_type = "admincallrecording"
            elif "voicemail" in lowered_path:
                recording_type = "voicemail"
            return f"{parsed.netloc.lower()}:{recording_type}:{canonical_id}"
        return f"{parsed.netloc.lower()}{parsed.path}"

    candidate_recording_id = str(candidate.get("recording_id") or "").strip() or _recording_id_from_url(str(candidate.get("url") or "").strip()) or _recording_id_from_url(str(resolved_url or "").strip())
    candidate_source = _normalized_source_url(str(resolved_url or "").strip())
    for entry in manifest_entries:
        metadata = entry.get("metadata") or {}
        if metadata.get("file_role") != "provider_audio":
            continue
        existing_recording_id = str(metadata.get("recording_id") or "").strip()
        existing_source = _normalized_source_url(str(metadata.get("source_url") or "").strip())
        if candidate_recording_id and existing_recording_id == candidate_recording_id:
            return entry
        if candidate_source and existing_source == candidate_source:
            return entry
    return None


async def _download_recordings(
    payload: Dict[str, Any],
    archive_dir: Path,
    event_stem: str,
    common_meta: Dict[str, Any],
    existing_manifest_entries: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    def _recording_id_from_url(url: str) -> str:
        match = re.search(
            r"/blob/(?:adminrecording|callrecording|voicemail(?:_recording)?)/(\d+)(?:\.[A-Za-z0-9]+)?(?:$|[?#])",
            url,
            re.IGNORECASE,
        )
        return match.group(1) if match else ""

    def _normalized_source_url(url: str) -> str:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return ""
        canonical_id = _recording_id_from_url(url)
        if canonical_id:
            recording_type = "callrecording"
            lowered_path = parsed.path.lower()
            if "adminrecording" in lowered_path:
                recording_type = "admincallrecording"
            elif "voicemail" in lowered_path:
                recording_type = "voicemail"
            return f"{parsed.netloc.lower()}:{recording_type}:{canonical_id}"
        return f"{parsed.netloc.lower()}{parsed.path}"

    manifests: List[Dict[str, Any]] = []
    results: List[Dict[str, Any]] = []
    candidates = await _resolve_payload_recordings(payload)
    seen_download_keys: set[str] = set()
    saved_audio_infos: List[Dict[str, Any]] = []

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

            candidate_recording_id = str(candidate.get("recording_id") or "").strip() or _recording_id_from_url(str(candidate.get("url") or "").strip()) or _recording_id_from_url(str(resolved_url or "").strip())
            dedupe_key = candidate_recording_id or _normalized_source_url(str(resolved_url or "").strip())
            if dedupe_key in seen_download_keys:
                results.append(
                    {
                        "index": index,
                        "status": "duplicate_candidate",
                        "recording_id": candidate.get("recording_id"),
                        "recording_type": candidate.get("recording_type"),
                        "source_url": resolved_url,
                    }
                )
                continue

            existing_entry = _existing_audio_entry(existing_manifest_entries, candidate, resolved_url)
            if existing_entry is not None:
                seen_download_keys.add(dedupe_key)
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
            sequence_no = len(saved_audio_infos) + 1
            if sequence_no == 1:
                filename = f"{event_stem}_{duration_suffix}{extension}"
            else:
                if sequence_no == 2:
                    first_audio = saved_audio_infos[0]
                    first_part_path = archive_dir / (
                        f"{event_stem}_part01_{first_audio['duration_suffix']}{first_audio['extension']}"
                    )
                    if first_audio["path"] != first_part_path:
                        first_audio["path"].replace(first_part_path)
                        manifests[first_audio["manifest_index"]]["storage_uri"] = str(first_part_path)
                        results[first_audio["result_index"]]["path"] = str(first_part_path)
                        first_audio["path"] = first_part_path
                filename = f"{event_stem}_part{sequence_no:02d}_{duration_suffix}{extension}"
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
                        "recording_id": candidate_recording_id or candidate.get("recording_id"),
                        "recording_type": candidate.get("recording_type"),
                        "duration_ms": candidate.get("duration_ms"),
                        "source_url": resolved_url,
                        "source_url_final": final_url,
                        "source": candidate.get("source"),
                    },
                    byte_size,
                )
            )
            saved_audio_infos.append(
                {
                    "path": audio_path,
                    "duration_suffix": duration_suffix,
                    "extension": extension,
                    "manifest_index": len(manifests) - 1,
                    "result_index": len(results),
                }
            )
            existing_manifest_entries.append(manifests[-1])
            seen_download_keys.add(dedupe_key)
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


async def handle_archive_export(job: Dict[str, Any]) -> Dict[str, Any]:
    payload = job.get("payload") or {}
    async with _archive_lock_for(payload):
        event_uuid = str(job.get("event_uuid") or payload.get("event_uuid") or "")
        channel = _archive_channel(payload)
        overwrite_archive = bool(payload.get("overwrite_archive"))
        occurred_at = _parse_occurred_at(payload.get("occurred_at"))
        source_provenance = _normalized_sms_provenance(payload.get("source_provenance") or {})
        payload["source_provenance"] = source_provenance
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

        if overwrite_archive:
            for matching_dir in _find_matching_archive_dirs(event_uuid, external_id):
                if matching_dir.exists():
                    shutil.rmtree(matching_dir)
            existing_archive_dir = None

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
        existing_manifest: List[Dict[str, Any]] = []
        if not overwrite_archive:
            existing_manifest = _load_manifest_entries(archive_dir, event_stem)
        if not existing_manifest and not overwrite_archive:
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
        provider_transcript_csv_bytes = payload.get("provider_transcript_csv_bytes")
        provider_transcript_source_mode = payload.get("provider_transcript_source_mode")
        fallback_speaker = _inferred_transcript_speaker(payload)
        rendered_provider_transcript = None
        normalized_provider_transcript_segments: List[Dict[str, str]] = []
        transcript_csv_path = archive_dir / f"{event_stem}_provider_transcript.csv"
        if isinstance(provider_transcript_csv_bytes, (bytes, bytearray)) and provider_transcript_csv_bytes:
            rendered_provider_transcript, normalized_provider_transcript_segments = _render_transcript_from_csv_bytes(
                bytes(provider_transcript_csv_bytes),
                fallback_speaker=fallback_speaker,
                assume_two_party=_archive_channel(payload) == "call",
            )
        for candidate in (
            payload.get("provider_transcript_payload"),
            payload.get("source_metadata") or {},
            provider_transcript_text if isinstance(provider_transcript_text, dict) else None,
        ):
            if rendered_provider_transcript:
                break
            normalized_provider_transcript_segments = _structured_transcript_segments(candidate, fallback_speaker)
            rendered_provider_transcript = _render_structured_transcript(candidate, fallback_speaker)
            if rendered_provider_transcript:
                break
        if not rendered_provider_transcript and isinstance(provider_transcript_text, str) and provider_transcript_text.strip():
            if _looks_like_labeled_transcript(provider_transcript_text):
                rendered_provider_transcript = provider_transcript_text
                normalized_provider_transcript_segments = _transcript_segments_from_text(provider_transcript_text)
            else:
                transcript_segments = _transcript_segments_from_text(provider_transcript_text)
                transcript_segments = _apply_speaker_turn_heuristic(
                    transcript_segments,
                    assume_two_party=_archive_channel(payload) == "call",
                    fallback_speaker=fallback_speaker,
                )
                if len(transcript_segments) == 1:
                    transcript_segments[0]["speaker_name"] = transcript_segments[0].get("speaker_name") or str(fallback_speaker or "")
                    transcript_segments[0]["speaker_label"] = transcript_segments[0].get("speaker_label") or (
                        "Speaker 1" if fallback_speaker else "Unknown Speaker"
                    )
                    transcript_segments[0]["rendered_speaker"] = _render_speaker_display(
                        transcript_segments[0]["speaker_name"],
                        transcript_segments[0]["speaker_label"],
                    )
                normalized_provider_transcript_segments = transcript_segments
                rendered_provider_transcript = _render_transcript_segments(transcript_segments)
        if isinstance(provider_transcript_csv_bytes, (bytes, bytearray)) and provider_transcript_csv_bytes:
            transcript_csv_size = _write_bytes(transcript_csv_path, bytes(provider_transcript_csv_bytes))
            manifest.append(
                _manifest_entry(
                    transcript_csv_path,
                    "text/csv",
                    {
                        **common_meta,
                        "file_role": "provider_transcript_csv",
                        "provider": "dialpad",
                        "source_mode": provider_transcript_source_mode,
                    },
                    transcript_csv_size,
                )
            )
        elif transcript_csv_path.exists():
            try:
                transcript_csv_path.unlink()
            except OSError:
                pass
        if rendered_provider_transcript:
            transcript_path = archive_dir / f"{event_stem}_provider_transcript.txt"
            transcript_size = _write_text(transcript_path, rendered_provider_transcript)
            manifest.append(
                _manifest_entry(
                    transcript_path,
                    "text/plain",
                    {
                        **common_meta,
                        "file_role": "provider_transcript",
                        "provider": "dialpad",
                        "source_mode": provider_transcript_source_mode,
                    },
                    transcript_size,
                )
            )
            transcript_json_path = archive_dir / f"{event_stem}_provider_transcript.json"
            transcript_json_size = _write_json(
                transcript_json_path,
                _provider_transcript_json_payload(
                    payload,
                    event_uuid=event_uuid,
                    source_mode=provider_transcript_source_mode,
                    rendered_text=rendered_provider_transcript,
                    segments=normalized_provider_transcript_segments,
                    fallback_speaker=fallback_speaker,
                ),
            )
            manifest.append(
                _manifest_entry(
                    transcript_json_path,
                    "application/json",
                    {
                        **common_meta,
                        "file_role": "provider_transcript_json",
                        "provider": "dialpad",
                        "source_mode": provider_transcript_source_mode,
                    },
                    transcript_json_size,
                )
            )
        sms_text = payload.get("sms_text")
        rendered_sms_text = _render_sms_text(payload, sms_text) if isinstance(sms_text, str) else None
        if rendered_sms_text:
            sms_path = archive_dir / f"{event_stem}_text.txt"
            sms_size = _write_text(sms_path, rendered_sms_text)
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

        audio_manifests: List[Dict[str, Any]] = []
        audio_results: List[Dict[str, Any]] = []
        if channel != "sms":
            audio_manifests, audio_results = await _download_recordings(
                payload,
                archive_dir,
                event_stem,
                common_meta,
                existing_manifest,
            )
        manifest.extend(audio_manifests)

        metadata_payload["provider_transcript_source_mode"] = provider_transcript_source_mode
        metadata_payload["provider_transcript_preserved"] = bool(rendered_provider_transcript)
        metadata_payload["provider_transcript_csv_preserved"] = bool(provider_transcript_csv_bytes)
        metadata_payload["provider_transcript_json_preserved"] = bool(rendered_provider_transcript)
        metadata_size = _write_json(metadata_path, metadata_payload)
        for entry in manifest:
            if entry.get("storage_uri") == str(metadata_path):
                entry["byte_size"] = metadata_size
                break

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

        return {
            "job_uuid": job.get("job_uuid"),
            "event_uuid": event_uuid,
            "archive_dir": str(archive_dir),
            "archive_files": final_manifest,
            "provider_transcript_preserved": bool(rendered_provider_transcript),
            "provider_transcript_csv_preserved": bool(provider_transcript_csv_bytes),
            "provider_transcript_json_preserved": bool(rendered_provider_transcript),
            "sms_text_preserved": bool(rendered_sms_text),
            "recording_reference_count": len(recording_references or []),
            "audio_downloads": audio_results,
        }

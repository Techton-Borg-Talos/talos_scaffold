"""Send a human-readable Dialpad review email for a requested time window.

This is intentionally separate from the historical backfill flow. It reads the
already-archived Dialpad files on disk, resolves contact names from a CSV that
can live alongside this script, and emails a plain-text timeline that an AI can
read to catch up quickly.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import smtplib
from collections import defaultdict
from datetime import datetime, time, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import dialpad_history_common as dhc

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ARCHIVE_ROOT = dhc.ARCHIVE_ROOT
ENV_CONTACTS_CSV = str(dhc.DEFAULT_CONTACTS_CSV)
ARCHIVE_TZ_NAME = (os.environ.get("DIALPAD_ARCHIVE_TIMEZONE", dhc.ARCHIVE_TIMEZONE_NAME).strip() or dhc.ARCHIVE_TIMEZONE_NAME)
DEFAULT_PROGRESS_EVERY = 100
SMTP_HOST = os.environ.get("DIALPAD_CATCHUP_SMTP_HOST", "").strip()
SMTP_PORT = int(os.environ.get("DIALPAD_CATCHUP_SMTP_PORT", "587") or "587")
SMTP_USERNAME = os.environ.get("DIALPAD_CATCHUP_SMTP_USERNAME", "").strip()
SMTP_PASSWORD = os.environ.get("DIALPAD_CATCHUP_SMTP_PASSWORD", "").strip()
SMTP_STARTTLS = (os.environ.get("DIALPAD_CATCHUP_SMTP_STARTTLS", "true").strip().lower() in {"1", "true", "yes", "y"})
SMTP_SSL = (os.environ.get("DIALPAD_CATCHUP_SMTP_SSL", "false").strip().lower() in {"1", "true", "yes", "y"})
EMAIL_FROM = os.environ.get("DIALPAD_CATCHUP_EMAIL_FROM", "").strip()
EMAIL_TO = os.environ.get("DIALPAD_CATCHUP_EMAIL_TO", "").strip()
SUBJECT_PREFIX = os.environ.get("DIALPAD_CATCHUP_SUBJECT_PREFIX", "Dialpad Catch-Up").strip() or "Dialpad Catch-Up"

PHONE_FIELDS = ("primary_phone", "all_phones")
NAME_FIELDS = ("full_name", "caller_name", "company")
AUDIO_SUFFIXES = {".mp3", ".wav", ".m4a", ".aac", ".ogg", ".webm"}
PLACEHOLDER_SUMMARIES = {
    "dialpad historical call",
    "dialpad historical sms",
    "dialpad historical voicemail",
}


def _archive_timezone() -> timezone | ZoneInfo:
    try:
        return ZoneInfo(ARCHIVE_TZ_NAME)
    except ZoneInfoNotFoundError:
        return timezone.utc


def _normalize_phone(value: Any) -> Optional[str]:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not digits:
        return None
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return f"+{digits}"


def _split_multi_value(value: str) -> Iterable[str]:
    text = str(value or "").replace("\n", ",").replace(";", ",")
    for part in text.split(","):
        cleaned = part.strip()
        if cleaned:
            yield cleaned


def _display_name(row: Dict[str, str]) -> Optional[str]:
    for field in NAME_FIELDS:
        value = str(row.get(field) or "").strip()
        if value:
            return value
    first_name = str(row.get("first_name") or "").strip()
    last_name = str(row.get("last_name") or "").strip()
    full = " ".join(part for part in (first_name, last_name) if part).strip()
    return full or None


def _discover_contacts_csv() -> Optional[Path]:
    env_contacts = os.environ.get("DIALPAD_CONTACTS_CSV", "").strip()
    if env_contacts:
        env_path = Path(env_contacts).expanduser()
        if env_path.exists():
            return env_path
    csv_paths = sorted(path for path in SCRIPT_DIR.glob("*.csv") if path.is_file())
    if not csv_paths:
        return None
    preferred = [path for path in csv_paths if "contact" in path.name.lower()]
    if preferred:
        return preferred[0]
    return csv_paths[0]


def _load_contacts_index(path: Optional[Path]) -> Dict[str, Dict[str, str]]:
    if path is None or not path.exists():
        return {}

    index: Dict[str, Dict[str, str]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not isinstance(row, dict):
                continue
            display = _display_name(row)
            if not display:
                continue
            for field in PHONE_FIELDS:
                for phone in _split_multi_value(str(row.get(field) or "")):
                    normalized = _normalize_phone(phone)
                    if not normalized:
                        continue
                    index[normalized] = {
                        "display_name": display,
                        "primary_email": str(row.get("primary_email") or "").strip(),
                        "company": str(row.get("company") or "").strip(),
                    }
    return index


def _parse_window(value: str, *, is_end: bool) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError("empty datetime")
    archive_tz = _archive_timezone()
    if len(text) == 10:
        parsed_date = datetime.fromisoformat(text).date()
        parsed_time = time.max if is_end else time.min
        return datetime.combine(parsed_date, parsed_time, tzinfo=archive_tz)
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=archive_tz)
    return parsed.astimezone(archive_tz)


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _read_text(path: Path) -> Optional[str]:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except Exception:
        return None
    return text or None


def _transcript_inline_text_from_json(payload: Dict[str, Any]) -> Optional[str]:
    rendered_text = str(payload.get("rendered_text") or "").strip()
    if rendered_text:
        return rendered_text
    segments = payload.get("segments")
    if not isinstance(segments, list):
        return None
    lines: List[str] = []
    for segment in segments:
        if not isinstance(segment, dict):
            continue
        text = str(segment.get("text") or "").strip()
        if not text:
            continue
        rendered_speaker = str(segment.get("rendered_speaker") or "").strip()
        lines.append(f"{rendered_speaker}: {text}" if rendered_speaker else text)
    rendered = "\n".join(lines).strip()
    return rendered or None


def _event_paths(archive_dir: Path, event_stem: str) -> Dict[str, Any]:
    metadata_path = archive_dir / f"{event_stem}_metadata.json"
    event_path = archive_dir / f"{event_stem}_event.json"
    manifest_path = archive_dir / f"{event_stem}_manifest.json"
    transcript_json_path = archive_dir / f"{event_stem}_provider_transcript.json"
    transcript_path = archive_dir / f"{event_stem}_provider_transcript.txt"
    summary_path = archive_dir / f"{event_stem}_provider_summary.txt"
    action_items_path = archive_dir / f"{event_stem}_provider_action_items.txt"
    sms_text_path = archive_dir / f"{event_stem}_text.txt"
    call_detail_path = archive_dir / f"{event_stem}_call_detail.json"
    recording_refs_path = archive_dir / f"{event_stem}_recording_references.json"
    audio_paths = sorted(path for path in archive_dir.iterdir() if path.is_file() and path.suffix.lower() in AUDIO_SUFFIXES)
    return {
        "archive_dir": archive_dir,
        "metadata": metadata_path if metadata_path.exists() else None,
        "event": event_path if event_path.exists() else None,
        "manifest": manifest_path if manifest_path.exists() else None,
        "transcript_json": transcript_json_path if transcript_json_path.exists() else None,
        "transcript": transcript_path if transcript_path.exists() else None,
        "summary": summary_path if summary_path.exists() else None,
        "action_items": action_items_path if action_items_path.exists() else None,
        "sms_text": sms_text_path if sms_text_path.exists() else None,
        "call_detail": call_detail_path if call_detail_path.exists() else None,
        "recording_references": recording_refs_path if recording_refs_path.exists() else None,
        "audio": audio_paths,
    }


def _contact_phone_candidates(source_provenance: Dict[str, Any], source_metadata: Dict[str, Any]) -> List[str]:
    contact = source_provenance.get("contact") or {}
    target = source_provenance.get("target") or {}
    candidates = [
        source_provenance.get("external_number"),
        source_provenance.get("from_number"),
        source_provenance.get("internal_number"),
        contact.get("phone"),
        target.get("phone"),
        source_metadata.get("external_number"),
        source_metadata.get("from_phone"),
        source_metadata.get("to_phone"),
        source_metadata.get("internal_number"),
        source_metadata.get("selected_caller_id"),
    ]
    normalized: List[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        phone = _normalize_phone(candidate)
        if not phone or phone in seen:
            continue
        seen.add(phone)
        normalized.append(phone)
    return normalized


def _resolve_contact_display(
    source_provenance: Dict[str, Any],
    source_metadata: Dict[str, Any],
    contacts_index: Dict[str, Dict[str, str]],
) -> str:
    contact = source_provenance.get("contact") or {}
    for phone in _contact_phone_candidates(source_provenance, source_metadata):
        matched = contacts_index.get(phone)
        if matched:
            label = matched["display_name"]
            company = matched.get("company") or ""
            return f"{label} ({phone})" if not company else f"{label} ({company}) [{phone}]"
    contact_name = str(contact.get("name") or "").strip()
    contact_phone = str(contact.get("phone") or source_provenance.get("external_number") or "").strip()
    if contact_name and contact_phone:
        return f"{contact_name} ({contact_phone})"
    if contact_name:
        return contact_name
    return contact_phone or "Unknown contact"


def _display_for_phone(
    phone: Any,
    contacts_index: Dict[str, Dict[str, str]],
    *,
    fallback_name: Optional[str] = None,
) -> str:
    normalized = _normalize_phone(phone)
    if normalized:
        matched = contacts_index.get(normalized)
        if matched:
            return f"{matched['display_name']} ({normalized})"
    fallback = str(fallback_name or "").strip()
    if fallback and normalized and fallback != normalized:
        return f"{fallback} ({normalized})"
    if normalized:
        return normalized
    return fallback or "Unknown"


def _infer_sms_party_info(
    source_provenance: Dict[str, Any],
    source_metadata: Dict[str, Any],
    contacts_index: Dict[str, Dict[str, str]],
) -> Dict[str, str]:
    contact = source_provenance.get("contact") or {}
    target = source_provenance.get("target") or {}
    direction = _event_direction(source_provenance)
    from_phone = _normalize_phone(source_metadata.get("from_phone") or source_provenance.get("from_number"))
    to_phone = _normalize_phone(source_metadata.get("to_phone") or source_provenance.get("internal_number"))
    contact_phone = _normalize_phone(contact.get("phone") or source_provenance.get("external_number"))
    target_phone = _normalize_phone(target.get("phone") or source_provenance.get("internal_number"))
    source_name = str(source_metadata.get("name") or "").strip()
    contact_name = str(contact.get("name") or "").strip()
    target_name = str(target.get("name") or "").strip()

    sender_fallback_name = source_name
    recipient_fallback_name = target_name
    if from_phone and contact_phone and from_phone == contact_phone and contact_name:
        sender_fallback_name = contact_name or sender_fallback_name
    if from_phone and target_phone and from_phone == target_phone and target_name:
        sender_fallback_name = target_name or sender_fallback_name
    if to_phone and target_phone and to_phone == target_phone and target_name:
        recipient_fallback_name = target_name or recipient_fallback_name
    if to_phone and contact_phone and to_phone == contact_phone and contact_name:
        recipient_fallback_name = contact_name or recipient_fallback_name

    sender_display = _display_for_phone(from_phone, contacts_index, fallback_name=sender_fallback_name)
    recipient_display = _display_for_phone(to_phone, contacts_index, fallback_name=recipient_fallback_name)
    if direction in {"outbound", "sent", "internal"}:
        thread_key = to_phone or recipient_display
        thread_display = recipient_display
    else:
        thread_key = from_phone or sender_display
        thread_display = sender_display
    return {
        "sender_display": sender_display,
        "recipient_display": recipient_display,
        "thread_key": thread_key,
        "thread_display": thread_display,
    }


def _event_direction(source_provenance: Dict[str, Any]) -> str:
    return str(source_provenance.get("direction") or "unknown").strip().lower() or "unknown"


def _event_inline_text(paths: Dict[str, Any]) -> Optional[str]:
    transcript_json_path = paths.get("transcript_json")
    if isinstance(transcript_json_path, Path):
        transcript_payload = _load_json(transcript_json_path)
        if isinstance(transcript_payload, dict):
            text = _transcript_inline_text_from_json(transcript_payload)
            if text:
                return text
    for key in ("transcript", "sms_text"):
        path = paths.get(key)
        if isinstance(path, Path):
            text = _read_text(path)
            if text:
                return text
    return None


def _normalized_lookup_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _iter_named_values(obj: Any, target_keys: Iterable[str]) -> Iterable[Any]:
    normalized_targets = {_normalized_lookup_key(key) for key in target_keys}

    def _walk(value: Any) -> Iterable[Any]:
        if isinstance(value, dict):
            for key, child in value.items():
                if _normalized_lookup_key(key) in normalized_targets:
                    yield child
                yield from _walk(child)
        elif isinstance(value, list):
            for child in value:
                yield from _walk(child)

    yield from _walk(obj)


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


def _first_text_candidate(obj: Any, target_keys: Iterable[str]) -> Optional[str]:
    for value in _iter_named_values(obj, target_keys):
        text = _clean_text_candidate(value)
        if text:
            return text
    return None


def _event_inline_text_from_metadata(source_metadata: Dict[str, Any]) -> Optional[str]:
    return _first_text_candidate(
        source_metadata,
        (
            "transcription_text",
            "transcript_text",
            "transcript",
            "transcription",
            "full_transcript",
            "text",
            "encrypted_aes_text",
            "encrypted_text",
        ),
    )


def _first_line(text: Optional[str], fallback: str = "") -> str:
    if not text:
        return fallback
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return lines[0] if lines else fallback


def _event_record(
    metadata_path: Path,
    contacts_index: Dict[str, Dict[str, str]],
) -> Optional[Dict[str, Any]]:
    metadata = _load_json(metadata_path)
    if not metadata:
        return None

    channel = str(metadata.get("channel") or "").strip().lower()
    if channel not in {"call", "sms", "voicemail"}:
        return None

    occurred_text = str(metadata.get("occurred_at") or "").strip()
    if not occurred_text:
        return None
    occurred_at = datetime.fromisoformat(occurred_text.replace("Z", "+00:00")).astimezone(_archive_timezone())
    archive_dir = metadata_path.parent
    event_stem = str(metadata.get("event_stem") or metadata_path.stem.replace("_metadata", "")).strip()
    paths = _event_paths(archive_dir, event_stem)
    source_provenance = metadata.get("source_provenance") or {}
    event_payload = _load_json(paths["event"]) if isinstance(paths["event"], Path) else {}
    source_metadata = event_payload if isinstance(event_payload, dict) else {}
    call_detail = _load_json(paths["call_detail"]) if isinstance(paths["call_detail"], Path) else {}
    call_detail_payload = call_detail if isinstance(call_detail, dict) else {}
    transcript_payload = _load_json(paths["transcript_json"]) if isinstance(paths["transcript_json"], Path) else {}
    transcript_json_payload = transcript_payload if isinstance(transcript_payload, dict) else {}
    inline_text = (
        _event_inline_text(paths)
        or _transcript_inline_text_from_json(transcript_json_payload)
        or _event_inline_text_from_metadata(source_metadata)
        or _event_inline_text_from_metadata(call_detail_payload)
    )
    summary_text = _read_text(paths["summary"]) if isinstance(paths.get("summary"), Path) else None
    if not summary_text:
        summary_text = _first_text_candidate(
            source_metadata,
            ("summary", "ai_summary", "call_summary", "review_summary", "conversation_summary", "recap"),
        ) or _first_text_candidate(
            call_detail_payload,
            ("summary", "ai_summary", "call_summary", "review_summary", "conversation_summary", "recap"),
        )
    action_items_text = _read_text(paths["action_items"]) if isinstance(paths.get("action_items"), Path) else None
    sms_party_info = _infer_sms_party_info(source_provenance, source_metadata, contacts_index) if channel == "sms" else {}

    return {
        "channel": channel,
        "occurred_at": occurred_at,
        "event_stem": event_stem,
        "archive_dir": archive_dir,
        "paths": paths,
        "metadata": metadata,
        "source_metadata": source_metadata,
        "call_detail": call_detail_payload,
        "transcript_payload": transcript_json_payload,
        "source_provenance": source_provenance,
        "contact_display": _resolve_contact_display(source_provenance, source_metadata, contacts_index),
        "direction": _event_direction(source_provenance),
        "sender_display": sms_party_info.get("sender_display"),
        "recipient_display": sms_party_info.get("recipient_display"),
        "sms_thread_key": sms_party_info.get("thread_key"),
        "sms_thread_display": sms_party_info.get("thread_display"),
        "inline_text": inline_text,
        "summary_text": summary_text or _first_line(inline_text, fallback=str(metadata.get("subject") or "").strip()),
        "action_items_text": action_items_text,
    }


def _scan_events(
    archive_root: Path,
    start_dt: datetime,
    end_dt: datetime,
    contacts_index: Dict[str, Dict[str, str]],
    progress_every: int,
) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    metadata_paths = sorted(archive_root.rglob("*_metadata.json"))
    total = len(metadata_paths)
    for index, metadata_path in enumerate(metadata_paths, start=1):
        if progress_every > 0 and (index == 1 or index % progress_every == 0 or index == total):
            print(f"[CATCHUP_EMAIL] scanned {index}/{total} metadata files")
        event = _event_record(metadata_path, contacts_index)
        if event is None:
            continue
        occurred_at = event["occurred_at"]
        if start_dt <= occurred_at <= end_dt:
            events.append(event)
    events.sort(key=lambda item: item["occurred_at"])
    return events


def _format_paths(paths: Dict[str, Any]) -> List[str]:
    lines: List[str] = []
    archive_dir = paths.get("archive_dir")
    if isinstance(archive_dir, Path):
        lines.append(f"Archive folder: {archive_dir}")
    for label in ("metadata", "event", "manifest", "transcript_json", "transcript", "sms_text", "recording_references"):
        path = paths.get(label)
        if isinstance(path, Path):
            lines.append(f"{label.replace('_', ' ').title()}: {path}")
    audio_paths = paths.get("audio") or []
    for idx, path in enumerate(audio_paths, start=1):
        lines.append(f"Audio {idx}: {path}")
    return lines


def _channel_label(channel: str) -> str:
    labels = {
        "call": "Calls",
        "sms": "Texts",
        "voicemail": "Voicemails",
    }
    return labels.get(channel, channel.title())


def _event_label(channel: str) -> str:
    labels = {
        "call": "Call",
        "sms": "Text",
        "voicemail": "Voicemail",
    }
    return labels.get(channel, channel.title())


def _content_label(channel: str) -> str:
    labels = {
        "call": "Full transcript",
        "sms": "Message",
        "voicemail": "Full transcript",
    }
    return labels.get(channel, "Content")


def _render_duration_seconds(total_seconds: int) -> Optional[str]:
    if total_seconds <= 0:
        return None
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"


def _format_duration(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        raw = float(text)
    except ValueError:
        return None
    if raw <= 0:
        return None
    total_seconds = int(raw / 1000.0) if raw > 36000 else int(raw)
    return _render_duration_seconds(total_seconds)


def _format_duration_ms(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        raw = float(text)
    except ValueError:
        return None
    if raw <= 0:
        return None
    return _render_duration_seconds(int(raw / 1000.0))


def _duration_from_audio_filename(paths: Dict[str, Any]) -> Optional[str]:
    audio_paths = paths.get("audio") or []
    for path in audio_paths:
        if not isinstance(path, Path):
            continue
        match = re.search(r"_(\d{2})-(\d{2})-(\d{2})(?:\.[A-Za-z0-9]+)$", path.name)
        if not match:
            continue
        hours, minutes, seconds = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        if hours == 0 and minutes == 0 and seconds == 0:
            continue
        if hours:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        return f"{minutes}:{seconds:02d}"
    return None


def _event_duration(event: Dict[str, Any]) -> Optional[str]:
    if event["channel"] not in {"call", "voicemail"}:
        return None
    source_metadata = event["source_metadata"]
    detail_payload = event.get("call_detail") if isinstance(event.get("call_detail"), dict) else {}
    for payload in (source_metadata, detail_payload):
        duration = _format_duration(payload.get("duration"))
        if duration:
            return duration
        duration = _format_duration_ms(payload.get("total_duration"))
        if duration:
            return duration
        for detail in payload.get("recording_details") or []:
            if isinstance(detail, dict):
                duration = _format_duration(detail.get("duration"))
                if duration:
                    return duration
                duration = _format_duration_ms(detail.get("duration_ms"))
                if duration:
                    return duration
        for key in ("talk_duration", "time_in_system", "ringing_duration", "hold_time"):
            duration = _format_duration(payload.get(key))
            if duration:
                return duration
        date_started = payload.get("date_started") or payload.get("date_connected")
        date_ended = payload.get("date_ended")
        if date_started not in (None, "") and date_ended not in (None, ""):
            try:
                started = float(date_started)
                ended = float(date_ended)
                if ended > started:
                    duration = _format_duration_ms(ended - started)
                    if duration:
                        return duration
            except (TypeError, ValueError):
                pass
    duration = _duration_from_audio_filename(event["paths"])
    if duration:
        return duration
    return None


def _target_display(source_provenance: Dict[str, Any]) -> str:
    target = source_provenance.get("target") or {}
    target_name = str(target.get("name") or "").strip()
    target_phone = str(target.get("phone") or "").strip()
    if target_name and target_phone:
        return f"{target_name} ({target_phone})"
    if target_phone:
        return target_phone
    if target_name:
        return target_name
    return "Unknown"


def _from_to_lines(event: Dict[str, Any]) -> List[str]:
    source_provenance = event["source_provenance"] if isinstance(event["source_provenance"], dict) else {}
    direction = str(event.get("direction") or "").strip().lower()
    contact_display = event["contact_display"]
    target_display = _target_display(source_provenance)
    if direction in {"outbound", "sent"}:
        return [
            f"From: {target_display}",
            f"To: {contact_display}",
        ]
    return [
        f"From: {contact_display}",
        f"To: {target_display}",
    ]


def _event_call_id(event: Dict[str, Any]) -> str:
    source_provenance = event["source_provenance"] if isinstance(event["source_provenance"], dict) else {}
    source_metadata = event["source_metadata"] if isinstance(event["source_metadata"], dict) else {}
    for candidate in (
        source_provenance.get("call_id"),
        source_metadata.get("call_id"),
        event["metadata"].get("external_id") if isinstance(event.get("metadata"), dict) else None,
    ):
        text = str(candidate or "").strip()
        if text:
            return text
    return ""


def _event_recording_urls(event: Dict[str, Any]) -> List[str]:
    source_metadata = event["source_metadata"] if isinstance(event.get("source_metadata"), dict) else {}
    urls: List[str] = []
    for candidate in source_metadata.get("admin_recording_urls") or []:
        text = str(candidate or "").strip()
        if text.startswith("http"):
            urls.append(text)
    voicemail_link = str(source_metadata.get("voicemail_link") or "").strip()
    if voicemail_link.startswith("http"):
        urls.append(voicemail_link)
    return urls


def _event_attachment_urls(event: Dict[str, Any]) -> List[str]:
    source_metadata = event["source_metadata"] if isinstance(event.get("source_metadata"), dict) else {}
    urls: List[str] = []
    for key in ("mms", "mms_url", "media_url", "attachment_url"):
        text = str(source_metadata.get(key) or "").strip()
        if text.startswith("http"):
            urls.append(text)
    media = source_metadata.get("media") or source_metadata.get("attachments") or []
    if isinstance(media, list):
        for item in media:
            if isinstance(item, dict):
                for key in ("url", "media_url", "attachment_url"):
                    text = str(item.get(key) or "").strip()
                    if text.startswith("http"):
                        urls.append(text)
            else:
                text = str(item or "").strip()
                if text.startswith("http"):
                    urls.append(text)
    deduped: List[str] = []
    seen: set[str] = set()
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped


def _clean_summary_text(event: Dict[str, Any]) -> Optional[str]:
    summary_text = str(event.get("summary_text") or "").strip()
    if not summary_text:
        return None
    if summary_text.strip().lower() in PLACEHOLDER_SUMMARIES:
        return None
    return summary_text


def _event_fallback_text(event: Dict[str, Any]) -> str:
    channel = str(event.get("channel") or "").strip().lower()
    source_metadata = event["source_metadata"] if isinstance(event.get("source_metadata"), dict) else {}
    category = str(source_metadata.get("category") or "").strip().lower()
    categories = str(source_metadata.get("categories") or "").strip().lower()
    attachment_urls = _event_attachment_urls(event)
    recording_urls = _event_recording_urls(event)
    has_audio = bool(event.get("paths", {}).get("audio"))

    if channel == "sms":
        if attachment_urls:
            return f"Attachment-only message archived: {attachment_urls[0]}"
        return "Message text was not available in the historical export."

    if channel == "voicemail":
        if recording_urls or has_audio:
            return "Transcript unavailable; voicemail recording was archived."
        return "Transcript unavailable for this historical voicemail."

    if channel == "call":
        if any(token in {category, categories} for token in ("abandoned", "unanswered", "missed")) or any(
            token in categories for token in ("abandoned", "unanswered", "missed")
        ):
            return "No transcript available; historical record shows an unanswered or abandoned call."
        if recording_urls or has_audio:
            return "Transcript unavailable; call recording was archived."
        return "Transcript unavailable for this historical call."

    return "Content not available."


def _is_same_phone(left: Any, right: Any) -> bool:
    normalized_left = _normalize_phone(left)
    normalized_right = _normalize_phone(right)
    return bool(normalized_left and normalized_right and normalized_left == normalized_right)


def _should_hide_call_event(event: Dict[str, Any]) -> bool:
    if str(event.get("channel") or "").strip().lower() != "call":
        return False
    if event.get("matching_voicemail_transcript"):
        return True

    source_metadata = event["source_metadata"] if isinstance(event.get("source_metadata"), dict) else {}
    source_provenance = event["source_provenance"] if isinstance(event.get("source_provenance"), dict) else {}
    category = str(source_metadata.get("category") or "").strip().lower()
    categories = str(source_metadata.get("categories") or "").strip().lower()
    if any(token in {category, categories} for token in ("abandoned", "unanswered", "missed")) or any(
        token in categories for token in ("abandoned", "unanswered", "missed")
    ):
        return True

    from_phone = source_metadata.get("from_phone") or source_provenance.get("from_number") or source_provenance.get("external_number")
    to_phone = source_metadata.get("to_phone") or source_provenance.get("internal_number")
    target = source_provenance.get("target") or {}
    contact = source_provenance.get("contact") or {}
    if _is_same_phone(from_phone, to_phone) or _is_same_phone(contact.get("phone"), target.get("phone")):
        return True

    return False


def _sms_sender_display(event: Dict[str, Any]) -> str:
    sender_display = str(event.get("sender_display") or "").strip()
    return sender_display or event["contact_display"]


def _sms_body_text(event: Dict[str, Any]) -> str:
    inline_text = str(event.get("inline_text") or "").strip()
    if inline_text.startswith("From: ") and "\nTo: " in inline_text:
        parts = inline_text.split("\n\n", 1)
        if len(parts) == 2:
            body = parts[1].strip()
            if body:
                return body
    if inline_text:
        return inline_text
    summary_text = _clean_summary_text(event)
    if summary_text:
        return summary_text
    return _event_fallback_text(event)


def _sms_thread_key(event: Dict[str, Any]) -> str:
    return str(event.get("sms_thread_key") or "").strip() or event["contact_display"]


def _sms_thread_title(events: List[Dict[str, Any]]) -> str:
    first = events[0]
    return str(first.get("sms_thread_display") or "").strip() or first["contact_display"]


def _sms_thread_block(index: int, events: List[Dict[str, Any]]) -> str:
    first = events[0]
    start_time = first["occurred_at"].strftime("%I:%M:%S %p")
    end_time = events[-1]["occurred_at"].strftime("%I:%M:%S %p")
    lines = [
        f"{index}. {_sms_thread_title(events)}",
        f"Messages: {len(events)}",
        f"Time range: {start_time}" if start_time == end_time else f"Time range: {start_time} -> {end_time}",
        "Thread:",
    ]
    for event in events:
        timestamp = event["occurred_at"].strftime("%I:%M:%S %p")
        sender = _sms_sender_display(event)
        body = _sms_body_text(event)
        body_lines = [line.rstrip() for line in body.splitlines()] or [body]
        lines.append(f"[{timestamp}] {sender}: {body_lines[0]}")
        for extra_line in body_lines[1:]:
            lines.append(f"  {extra_line}")
    return "\n".join(lines)


def _event_block(index: int, event: Dict[str, Any]) -> str:
    occurred_at = event["occurred_at"].strftime("%I:%M:%S %p")
    event_type = _event_label(event["channel"])
    duration = _event_duration(event)
    inline_text = str(event.get("inline_text") or "").strip()
    inline_has_explicit_from_to = inline_text.startswith("From: ") and "\nTo: " in inline_text
    lines = [
        f"{index}. {event['contact_display']}",
        f"Time: {occurred_at}",
        f"Event: {event_type}",
    ]
    if not (event["channel"] == "sms" and inline_has_explicit_from_to):
        lines.extend(_from_to_lines(event))
    if duration:
        lines.append(f"Duration: {duration}")
    if inline_text:
        lines.append(f"{_content_label(event['channel'])}:")
        lines.append(inline_text)
    else:
        summary_text = _clean_summary_text(event)
        action_items_text = str(event.get("action_items_text") or "").strip()
        if event.get("matching_voicemail_transcript"):
            lines.append(f"{_content_label(event['channel'])}: see matching voicemail event below")
        elif summary_text:
            lines.append("Summary:")
            lines.append(summary_text)
            if action_items_text:
                lines.append("Action items:")
                lines.append(action_items_text)
        else:
            lines.append("Summary:")
            lines.append(_event_fallback_text(event))
    return "\n".join(lines)


def _day_header(day_key: str, day_events: List[Dict[str, Any]]) -> List[str]:
    counts = {"call": 0, "sms": 0, "voicemail": 0}
    for event in day_events:
        counts[event["channel"]] += 1
    return [
        f"{day_key}",
        f"Calls: {counts['call']} | Texts: {counts['sms']} | Voicemails: {counts['voicemail']} | Total: {len(day_events)}",
    ]


def _day_channel_blocks(day_events: List[Dict[str, Any]]) -> List[str]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for event in day_events:
        grouped[event["channel"]].append(event)

    blocks: List[str] = []
    for channel in ("call", "sms", "voicemail"):
        channel_events = grouped.get(channel) or []
        if not channel_events:
            continue
        blocks.append(f"{_channel_label(channel)}")
        if channel == "call":
            visible_events = [event for event in channel_events if not _should_hide_call_event(event)]
            hidden_count = len(channel_events) - len(visible_events)
            if hidden_count:
                blocks.append(f"Filtered {hidden_count} low-signal call record(s) from the detailed list.")
            for index, event in enumerate(visible_events, start=1):
                blocks.append(_event_block(index, event))
            continue
        if channel == "sms":
            threads: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            order: List[str] = []
            for event in channel_events:
                thread_key = _sms_thread_key(event)
                if thread_key not in threads:
                    order.append(thread_key)
                threads[thread_key].append(event)
            for index, thread_key in enumerate(order, start=1):
                thread_events = sorted(threads[thread_key], key=lambda item: item["occurred_at"])
                blocks.append(_sms_thread_block(index, thread_events))
            continue
        for index, event in enumerate(channel_events, start=1):
            blocks.append(_event_block(index, event))
    return blocks


def _build_email_body(
    *,
    archive_root: Path,
    contacts_csv: Optional[Path],
    start_dt: datetime,
    end_dt: datetime,
    events: List[Dict[str, Any]],
) -> str:
    header_lines = [
        "Dialpad Daily Review Report",
        "",
        f"Window: {start_dt.strftime('%Y-%m-%d %H:%M:%S %Z')} -> {end_dt.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        "",
        "Report",
    ]
    if not events:
        return "\n\n".join(header_lines + ["", "No archived Dialpad events were found in the requested window."]) + "\n"

    voicemail_call_ids = {
        _event_call_id(event)
        for event in events
        if event["channel"] == "voicemail" and event.get("inline_text")
    }
    for event in events:
        if event["channel"] == "call" and not event.get("inline_text"):
            event["matching_voicemail_transcript"] = _event_call_id(event) in voicemail_call_ids

    by_day: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for event in events:
        day_key = event["occurred_at"].strftime("%A, %B %d, %Y")
        by_day[day_key].append(event)

    blocks: List[str] = []
    for day_key, day_events in by_day.items():
        blocks.extend(_day_header(day_key, day_events))
        blocks.append("")
        blocks.extend(_day_channel_blocks(day_events))
        blocks.append("")

    while blocks and not blocks[-1].strip():
        blocks.pop()
    return "\n\n".join(header_lines + [""] + blocks) + "\n"


def _split_recipients(value: str) -> List[str]:
    recipients: List[str] = []
    for part in _split_multi_value(value):
        recipients.append(part)
    return recipients


def _send_email(subject: str, body: str, recipients: List[str]) -> None:
    if not SMTP_HOST:
        raise SystemExit("DIALPAD_CATCHUP_SMTP_HOST is not configured.")
    if not EMAIL_FROM:
        raise SystemExit("DIALPAD_CATCHUP_EMAIL_FROM is not configured.")
    if not recipients:
        raise SystemExit("No recipient email address was provided.")

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = EMAIL_FROM
    message["To"] = ", ".join(recipients)
    message.set_content(body)

    if SMTP_SSL:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
            if SMTP_USERNAME:
                smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
            smtp.send_message(message)
        return

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
        smtp.ehlo()
        if SMTP_STARTTLS:
            smtp.starttls()
            smtp.ehlo()
        if SMTP_USERNAME:
            smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
        smtp.send_message(message)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Email a human-readable Dialpad review report for a time window.")
    parser.add_argument("--start", required=True, help="Inclusive start in YYYY-MM-DD or ISO datetime form.")
    parser.add_argument("--end", required=True, help="Inclusive end in YYYY-MM-DD or ISO datetime form.")
    parser.add_argument(
        "--archive-root",
        default=str(DEFAULT_ARCHIVE_ROOT),
        help="Dialpad archive root directory to scan.",
    )
    parser.add_argument(
        "--contacts-csv",
        default="",
        help="Optional contacts CSV. Defaults to DIALPAD_CONTACTS_CSV, then the first CSV found beside this script.",
    )
    parser.add_argument(
        "--send-to",
        default="",
        help="Optional comma-separated recipient override. Defaults to DIALPAD_CATCHUP_EMAIL_TO.",
    )
    parser.add_argument(
        "--subject",
        default="",
        help="Optional full subject override.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        help="Print scan progress every N metadata files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the email body to stdout instead of sending it.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    archive_root = Path(args.archive_root).expanduser()
    if not archive_root.exists():
        raise SystemExit(f"Archive root does not exist: {archive_root}")

    contacts_csv = Path(args.contacts_csv).expanduser() if args.contacts_csv else _discover_contacts_csv()
    contacts_index = _load_contacts_index(contacts_csv)
    start_dt = _parse_window(args.start, is_end=False)
    end_dt = _parse_window(args.end, is_end=True)
    if end_dt < start_dt:
        raise SystemExit("--end must be the same as or later than --start")

    print(f"[CATCHUP_EMAIL] archive root: {archive_root}")
    print(f"[CATCHUP_EMAIL] contacts csv: {contacts_csv if contacts_csv else 'none'}")
    print(f"[CATCHUP_EMAIL] scanning window: {start_dt.isoformat()} -> {end_dt.isoformat()}")
    events = _scan_events(archive_root, start_dt, end_dt, contacts_index, max(1, args.progress_every))
    print(f"[CATCHUP_EMAIL] matched {len(events)} events")

    subject = args.subject.strip() or f"{SUBJECT_PREFIX}: {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}"
    body = _build_email_body(
        archive_root=archive_root,
        contacts_csv=contacts_csv,
        start_dt=start_dt,
        end_dt=end_dt,
        events=events,
    )

    if args.dry_run:
        print(body)
        return

    recipients = _split_recipients(args.send_to or EMAIL_TO)
    _send_email(subject, body, recipients)
    print(f"[CATCHUP_EMAIL] sent to: {', '.join(recipients)}")


if __name__ == "__main__":
    main()

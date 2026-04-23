"""TALOS Intake API. Archive-first + normalized event and participant insert."""
#This is a test
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional, Tuple

import asyncpg
import httpx
import jwt
from fastapi import FastAPI, Header, HTTPException, Request
from pydantic import BaseModel

LOG = logging.getLogger("talos.intake")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

RAW = Path(os.environ.get("RAW_ARTIFACT_DIR", "/var/lib/talos/raw"))
RAW.mkdir(parents=True, exist_ok=True)

DIALPAD_SECRET = os.environ.get("DIALPAD_WEBHOOK_SECRET", "")
DATABASE_URL = os.environ["DATABASE_URL"]
CONTACT_ENGINE_URL = os.environ.get("CONTACT_ENGINE_URL", "http://contact_engine:8083")
WORKER_WRITEBACK_TOKEN = os.environ.get("WORKER_WRITEBACK_TOKEN", "")

app = FastAPI(title="talos-intake", version="0.3.0")
_pool: Optional[asyncpg.Pool] = None


@app.on_event("startup")
async def startup() -> None:
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=4)


@app.on_event("shutdown")
async def shutdown() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()


@app.get("/healthz")
async def healthz():
    return {"ok": True, "service": "intake_api"}


class WorkerJobUpdate(BaseModel):
    job_uuid: str
    state: Literal["PROCESSING", "SUCCEEDED", "FAILED"]
    result: Dict = {}
    error: Optional[str] = None
    worker_id: Optional[str] = None


def _require_worker_writeback_token(authorization: Optional[str]) -> None:
    if not WORKER_WRITEBACK_TOKEN:
        raise HTTPException(503, "worker writeback token not configured")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "missing bearer token")
    if authorization[len("Bearer ") :].strip() != WORKER_WRITEBACK_TOKEN:
        raise HTTPException(401, "bad token")


def _archive(source: str, raw: bytes, content_type: str) -> Dict:
    artifact_uuid = uuid.uuid4()
    sha = hashlib.sha256(raw).hexdigest()
    subdir = RAW / source / sha[:2]
    subdir.mkdir(parents=True, exist_ok=True)
    path = subdir / f"{artifact_uuid}.bin"
    path.write_bytes(raw)
    return {
        "artifact_uuid": str(artifact_uuid),
        "storage_scheme": "local",
        "storage_uri": str(path),
        "sha256": sha,
        "byte_size": len(raw),
        "content_type": content_type,
    }


def _decode_dialpad_event(raw: bytes) -> Dict:
    text = raw.decode("utf-8", errors="replace").strip()

    if DIALPAD_SECRET:
        try:
            payload = jwt.decode(
                text,
                DIALPAD_SECRET,
                algorithms=["HS256"],
                options={"verify_aud": False},
            )
            if not isinstance(payload, dict):
                raise HTTPException(401, "bad dialpad jwt payload")
            return payload
        except jwt.InvalidTokenError as e:
            LOG.warning("dialpad jwt verification failed: %s", e)
            raise HTTPException(401, "bad signature") from e

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as e:
        LOG.warning("dialpad json decode failed: %s", e)
        raise HTTPException(400, "invalid json") from e

    if not isinstance(payload, dict):
        raise HTTPException(400, "invalid payload type")

    return payload


def _ms_to_ts(ms: Optional[object]) -> datetime:
    if ms is None:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def _normalize_email(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    v = value.strip().lower()
    return v or None


def _normalize_phone(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    digits = re.sub(r"\D+", "", value)
    if not digits:
        return None
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if value.strip().startswith("+") and digits:
        return f"+{digits}"
    return f"+{digits}"


def _channel_and_external_id(evt: Dict) -> Tuple[str, str]:
    if "call_id" in evt:
        state = str(evt.get("state") or "").lower()
        if state in {"voicemail", "voicemail_uploaded", "transcription"}:
            return "voicemail", str(evt["call_id"])
        return "call", str(evt["call_id"])

    if "id" in evt:
        return "sms", str(evt["id"])

    raise HTTPException(400, "unable to classify dialpad payload")


def _direction(evt: Dict) -> str:
    d = str(evt.get("direction") or "unknown").lower()
    if d in {"inbound", "outbound", "internal"}:
        return d
    return "unknown"


def _subject(evt: Dict, channel: str) -> Optional[str]:
    if channel == "sms":
        txt = evt.get("text_content") or evt.get("text")
        if txt:
            return str(txt)[:255]
        return "dialpad sms"

    state = evt.get("state")
    if state:
        return f"dialpad {channel} {state}"
    return f"dialpad {channel}"


def _event_identity_fields(evt: Dict, channel: str) -> Tuple[Optional[str], Optional[str]]:
    if channel in {"call", "voicemail"}:
        remote_identifier = _normalize_phone(
            evt.get("external_number") or (evt.get("contact") or {}).get("phone")
        )
        local_identifier = _normalize_phone(
            evt.get("internal_number") or (evt.get("target") or {}).get("phone")
        )
        return remote_identifier, local_identifier

    remote_identifier = _normalize_phone(
        evt.get("from_number") or (evt.get("contact") or {}).get("phone")
    )
    to_number = evt.get("to_number")
    if isinstance(to_number, list) and to_number:
        local_identifier = _normalize_phone(to_number[0])
    else:
        local_identifier = _normalize_phone((evt.get("target") or {}).get("phone"))
    return remote_identifier, local_identifier


def _dialpad_participants(evt: Dict, channel: str) -> List[Dict]:
    participants: List[Dict] = []

    contact = evt.get("contact") or {}
    target = evt.get("target") or {}

    if channel in {"call", "voicemail"}:
        participants.append(
            {
                "participant_index": 0,
                "role": "counterparty",
                "display_name": contact.get("name"),
                "email_raw": contact.get("email"),
                "email_normalized": _normalize_email(contact.get("email")),
                "phone_raw": contact.get("phone"),
                "phone_normalized": _normalize_phone(contact.get("phone")),
                "source_person_id": str(contact.get("id")) if contact.get("id") is not None else None,
                "source_system": "dialpad",
                "is_internal": False,
            }
        )
        participants.append(
            {
                "participant_index": 1,
                "role": "target",
                "display_name": target.get("name"),
                "email_raw": target.get("email"),
                "email_normalized": _normalize_email(target.get("email")),
                "phone_raw": target.get("phone"),
                "phone_normalized": _normalize_phone(target.get("phone")),
                "source_person_id": str(target.get("id")) if target.get("id") is not None else None,
                "source_system": "dialpad",
                "is_internal": True,
            }
        )
        return participants

    participants.append(
        {
            "participant_index": 0,
            "role": "sender",
            "display_name": contact.get("name"),
            "email_raw": contact.get("email"),
            "email_normalized": _normalize_email(contact.get("email")),
            "phone_raw": evt.get("from_number") or contact.get("phone"),
            "phone_normalized": _normalize_phone(evt.get("from_number") or contact.get("phone")),
            "source_person_id": str(contact.get("id")) if contact.get("id") is not None else None,
            "source_system": "dialpad",
            "is_internal": False,
        }
    )

    to_number = evt.get("to_number")
    to_numbers = to_number if isinstance(to_number, list) else [to_number] if to_number else []

    if to_numbers:
        for idx, phone in enumerate(to_numbers, start=1):
            participants.append(
                {
                    "participant_index": idx,
                    "role": "recipient",
                    "display_name": target.get("name") if idx == 1 else None,
                    "email_raw": target.get("email") if idx == 1 else None,
                    "email_normalized": _normalize_email(target.get("email") if idx == 1 else None),
                    "phone_raw": phone,
                    "phone_normalized": _normalize_phone(phone),
                    "source_person_id": str(target.get("id")) if idx == 1 and target.get("id") is not None else None,
                    "source_system": "dialpad",
                    "is_internal": True,
                }
            )
    else:
        participants.append(
            {
                "participant_index": 1,
                "role": "recipient",
                "display_name": target.get("name"),
                "email_raw": target.get("email"),
                "email_normalized": _normalize_email(target.get("email")),
                "phone_raw": target.get("phone"),
                "phone_normalized": _normalize_phone(target.get("phone")),
                "source_person_id": str(target.get("id")) if target.get("id") is not None else None,
                "source_system": "dialpad",
                "is_internal": True,
            }
        )

    return participants


def _source_provenance(evt: Dict, channel: str) -> Dict:
    target = evt.get("target") or {}
    contact = evt.get("contact") or {}
    return {
        "source_system": "dialpad",
        "channel": channel,
        "direction": _direction(evt),
        "external_id": str(evt.get("call_id") or evt.get("id") or ""),
        "call_id": str(evt.get("call_id")) if evt.get("call_id") is not None else None,
        "target": {
            "id": str(target.get("id")) if target.get("id") is not None else None,
            "name": target.get("name"),
            "type": target.get("type"),
            "email": _normalize_email(target.get("email")),
            "phone": _normalize_phone(target.get("phone")),
            "office_id": str(target.get("office_id")) if target.get("office_id") is not None else None,
        },
        "contact": {
            "id": str(contact.get("id")) if contact.get("id") is not None else None,
            "name": contact.get("name"),
            "type": contact.get("type"),
            "email": _normalize_email(contact.get("email")),
            "phone": _normalize_phone(contact.get("phone")),
        },
        "internal_number": _normalize_phone(evt.get("internal_number")),
        "external_number": _normalize_phone(evt.get("external_number")),
        "from_number": _normalize_phone(evt.get("from_number")),
        "selected_caller_id": _normalize_phone(evt.get("selected_caller_id")),
    }


def _enriched_source_metadata(evt: Dict, channel: str) -> Dict:
    payload = dict(evt)
    payload["_talos"] = {
        "source_provenance": _source_provenance(evt, channel),
    }
    return payload


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


def _extract_action_items(obj: Any) -> List[str]:
    action_items: List[str] = []
    for value in _iter_named_values(obj, ("action_items", "actionItems", "next_steps", "tasks", "todos")):
        if isinstance(value, str):
            text = _clean_text_candidate(value)
            if text:
                action_items.append(text)
            continue
        if isinstance(value, list):
            for item in value:
                if isinstance(item, str):
                    text = _clean_text_candidate(item)
                    if text:
                        action_items.append(text)
                elif isinstance(item, dict):
                    for candidate_key in ("text", "label", "title", "description", "body"):
                        text = _clean_text_candidate(item.get(candidate_key))
                        if text:
                            action_items.append(text)
                            break
    deduped: List[str] = []
    seen: set[str] = set()
    for item in action_items:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _provider_summary_payload(evt: Dict, channel: str, event_uuid: str) -> Dict[str, Any]:
    summary_text = _first_text_candidate(
        evt,
        (
            "summary",
            "ai_summary",
            "call_summary",
            "review_summary",
            "conversation_summary",
            "recap",
        ),
    )
    action_items = _extract_action_items(evt)
    provenance = _source_provenance(evt, channel)
    return {
        "event_uuid": event_uuid,
        "summary_text": summary_text,
        "action_items": action_items,
        "metadata": {
            "provider": "dialpad",
            "provider_state": evt.get("state"),
            "source_provenance": provenance,
        },
    }


def _provider_transcript_payload(evt: Dict, channel: str, event_uuid: str) -> Optional[Dict]:
    text = _first_text_candidate(
        evt,
        (
            "transcription_text",
            "transcript_text",
            "transcript",
            "transcription",
            "full_transcript",
        ),
    )
    if not text:
        return None
    provenance = _source_provenance(evt, channel)
    return {
        "event_uuid": event_uuid,
        "engine": "dialpad_provider",
        "language": evt.get("language"),
        "diarized": False,
        "text": text,
        "confidence": None,
        "metadata": {
            "provider": "dialpad",
            "provider_state": evt.get("state"),
            "voicemail_link": evt.get("voicemail_link"),
            "voicemail_recording_id": evt.get("voicemail_recording_id"),
            "call_recording_ids": evt.get("call_recording_ids") or [],
            "recording_details": evt.get("recording_details") or [],
            "source_provenance": provenance,
        },
    }


def _provider_recording_artifacts(evt: Dict, channel: str, event_uuid: str) -> List[Dict]:
    provenance = _source_provenance(evt, channel)
    refs: List[Dict] = []
    recording_details = evt.get("recording_details") or []
    direct_recording_url = evt.get("recording_url") or evt.get("recording")

    if direct_recording_url:
        refs.append(
            {
                "event_uuid": event_uuid,
                "kind": "provider_recording_reference",
                "storage_scheme": "dialpad",
                "storage_uri": str(direct_recording_url),
                "content_type": "text/uri-list",
                "metadata": {
                    "provider": "dialpad",
                    "reference_type": "recording_url",
                    "recording_id": evt.get("recording_id"),
                    "recording_type": evt.get("recording_type") or "callrecording",
                    "duration_ms": evt.get("duration"),
                    "start_time": evt.get("date_started") or evt.get("start_time"),
                    "source_provenance": provenance,
                },
            }
        )

    voicemail_link = evt.get("voicemail_link")
    if voicemail_link:
        refs.append(
            {
                "event_uuid": event_uuid,
                "kind": "provider_recording_reference",
                "storage_scheme": "dialpad",
                "storage_uri": str(voicemail_link),
                "content_type": "text/uri-list",
                "metadata": {
                    "provider": "dialpad",
                    "reference_type": "voicemail_link",
                    "voicemail_recording_id": evt.get("voicemail_recording_id"),
                    "source_provenance": provenance,
                },
            }
        )

    voicemail_recording_id = evt.get("voicemail_recording_id")
    if voicemail_recording_id:
        refs.append(
            {
                "event_uuid": event_uuid,
                "kind": "provider_recording_reference",
                "storage_scheme": "dialpad",
                "storage_uri": f"dialpad://voicemail_recording/{voicemail_recording_id}",
                "content_type": "application/x.dialpad.recording-reference",
                "metadata": {
                    "provider": "dialpad",
                    "reference_type": "voicemail_recording_id",
                    "recording_id": voicemail_recording_id,
                    "source_provenance": provenance,
                },
            }
        )

    for detail in recording_details:
        if not isinstance(detail, dict):
            continue
        detail_url = detail.get("url")
        if not detail_url:
            continue
        refs.append(
            {
                "event_uuid": event_uuid,
                "kind": "provider_recording_reference",
                "storage_scheme": "dialpad",
                "storage_uri": str(detail_url),
                "content_type": "text/uri-list",
                "metadata": {
                    "provider": "dialpad",
                    "reference_type": "recording_detail_url",
                    "recording_id": detail.get("id"),
                    "recording_type": detail.get("recording_type"),
                    "duration_ms": detail.get("duration"),
                    "start_time": detail.get("start_time"),
                    "source_provenance": provenance,
                },
            }
        )

    for recording_id in evt.get("call_recording_ids") or []:
        refs.append(
            {
                "event_uuid": event_uuid,
                "kind": "provider_recording_reference",
                "storage_scheme": "dialpad",
                "storage_uri": f"dialpad://call_recording/{recording_id}",
                "content_type": "application/x.dialpad.recording-reference",
                "metadata": {
                    "provider": "dialpad",
                    "reference_type": "call_recording_id",
                    "recording_id": recording_id,
                    "source_provenance": provenance,
                },
            }
        )

    if evt.get("recording_id"):
        refs.append(
            {
                "event_uuid": event_uuid,
                "kind": "provider_recording_reference",
                "storage_scheme": "dialpad",
                "storage_uri": f"dialpad://call_recording/{evt.get('recording_id')}",
                "content_type": "application/x.dialpad.recording-reference",
                "metadata": {
                    "provider": "dialpad",
                    "reference_type": "recording_id",
                    "recording_id": evt.get("recording_id"),
                    "recording_type": evt.get("recording_type") or "callrecording",
                    "source_provenance": provenance,
                },
            }
        )

    for field_name, default_type in (
        ("admin_recording_urls", "admincallrecording"),
        ("call_recording_share_links", "callrecording"),
        ("admin_call_recording_share_links", "admincallrecording"),
    ):
        field_value = evt.get(field_name) or []
        if not isinstance(field_value, list):
            continue
        for item in field_value:
            if isinstance(item, str):
                refs.append(
                    {
                        "event_uuid": event_uuid,
                        "kind": "provider_recording_reference",
                        "storage_scheme": "dialpad",
                        "storage_uri": item,
                        "content_type": "text/uri-list",
                        "metadata": {
                            "provider": "dialpad",
                            "reference_type": field_name,
                            "recording_id": evt.get("recording_id"),
                            "recording_type": default_type,
                            "duration_ms": evt.get("duration"),
                            "start_time": evt.get("date_started") or evt.get("start_time"),
                            "source_provenance": provenance,
                        },
                    }
                )
            elif isinstance(item, dict):
                storage_uri = str(item.get("url") or item.get("access_link") or item.get("link") or "").strip()
                if not storage_uri:
                    continue
                refs.append(
                    {
                        "event_uuid": event_uuid,
                        "kind": "provider_recording_reference",
                        "storage_scheme": "dialpad",
                        "storage_uri": storage_uri,
                        "content_type": "text/uri-list",
                        "metadata": {
                            "provider": "dialpad",
                            "reference_type": field_name,
                            "recording_id": item.get("recording_id") or evt.get("recording_id"),
                            "recording_type": item.get("recording_type") or default_type,
                            "duration_ms": item.get("duration") or evt.get("duration"),
                            "start_time": item.get("start_time") or evt.get("date_started") or evt.get("start_time"),
                            "source_provenance": provenance,
                        },
                    }
                )

    return refs


def _archive_export_payload(
    evt: Dict,
    channel: str,
    event_uuid: str,
    occurred_at: datetime,
    subject: Optional[str],
    enriched_source_metadata: Dict,
    provider_recording_artifacts: List[Dict],
) -> Dict:
    provider_transcript = _first_text_candidate(
        evt,
        (
            "transcription_text",
            "transcript_text",
            "transcript",
            "transcription",
            "full_transcript",
        ),
    )
    provider_summary = _provider_summary_payload(evt, channel, event_uuid)
    sms_text = None
    if channel == "sms":
        sms_text = (evt.get("text_content") or evt.get("text") or "").strip() or None

    return {
        "event_uuid": event_uuid,
        "channel": channel,
        "external_id": str(evt.get("call_id") or evt.get("id") or ""),
        "occurred_at": occurred_at.isoformat(),
        "subject": subject,
        "source_metadata": enriched_source_metadata,
        "source_provenance": _source_provenance(evt, channel),
        "provider_transcript_text": provider_transcript,
        "provider_summary_text": provider_summary.get("summary_text"),
        "provider_action_items": provider_summary.get("action_items") or [],
        "sms_text": sms_text,
        "recording_references": [
            {
                "kind": ref["kind"],
                "storage_scheme": ref["storage_scheme"],
                "storage_uri": ref["storage_uri"],
                "content_type": ref["content_type"],
                "metadata": ref["metadata"],
            }
            for ref in provider_recording_artifacts
        ],
    }


def _archive_payload_signature(archive_payload: Dict) -> str:
    recording_refs = []
    for ref in archive_payload.get("recording_references") or []:
        metadata = ref.get("metadata") or {}
        recording_refs.append(
            {
                "storage_uri": ref.get("storage_uri"),
                "reference_type": metadata.get("reference_type"),
                "recording_id": metadata.get("recording_id"),
                "recording_type": metadata.get("recording_type"),
                "voicemail_recording_id": metadata.get("voicemail_recording_id"),
            }
        )

    source_metadata = archive_payload.get("source_metadata") or {}
    signature_payload = {
        "event_uuid": archive_payload.get("event_uuid"),
        "channel": archive_payload.get("channel"),
        "provider_transcript_text": archive_payload.get("provider_transcript_text") or None,
        "provider_summary_text": archive_payload.get("provider_summary_text") or None,
        "provider_action_items": archive_payload.get("provider_action_items") or [],
        "sms_text": archive_payload.get("sms_text") or None,
        "recording_references": sorted(
            recording_refs,
            key=lambda item: (
                str(item.get("storage_uri") or ""),
                str(item.get("reference_type") or ""),
                str(item.get("recording_id") or ""),
            ),
        ),
        "voicemail_recording_id": source_metadata.get("voicemail_recording_id"),
        "call_recording_ids": sorted(str(item) for item in (source_metadata.get("call_recording_ids") or [])),
    }
    rendered = json.dumps(signature_payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(rendered.encode("utf-8")).hexdigest()[:16]


async def _insert_communication_event(evt: Dict, artifact: Dict) -> Dict:
    assert _pool is not None

    channel, external_id = _channel_and_external_id(evt)
    direction = _direction(evt)
    occurred_at = _ms_to_ts(
        evt.get("event_timestamp")
        or evt.get("created_date")
        or evt.get("date_started")
        or evt.get("date_connected")
        or evt.get("date_ended")
    )
    remote_identifier, local_identifier = _event_identity_fields(evt, channel)
    subject = _subject(evt, channel)
    participants = _dialpad_participants(evt, channel)
    enriched_source_metadata = _enriched_source_metadata(evt, channel)

    async with _pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                INSERT INTO communication_events
                (
                    source_system,
                    channel,
                    direction,
                    external_id,
                    remote_identifier,
                    local_identifier,
                    occurred_at,
                    subject,
                    source_metadata,
                    verified
                )
                VALUES
                (
                    'dialpad'::source_system,
                    $1::event_channel,
                    $2::event_direction,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7,
                    $8::jsonb,
                    TRUE
                )
                ON CONFLICT (source_system, external_id)
                DO UPDATE SET
                    channel = EXCLUDED.channel,
                    direction = EXCLUDED.direction,
                    remote_identifier = EXCLUDED.remote_identifier,
                    local_identifier = EXCLUDED.local_identifier,
                    occurred_at = EXCLUDED.occurred_at,
                    subject = EXCLUDED.subject,
                    source_metadata = EXCLUDED.source_metadata,
                    verified = TRUE,
                    updated_at = now()
                RETURNING event_uuid
                """,
                channel,
                direction,
                external_id,
                remote_identifier,
                local_identifier,
                occurred_at,
                subject,
                json.dumps(enriched_source_metadata),
            )

            event_uuid = row["event_uuid"]
            event_uuid_str = str(event_uuid)
            provider_transcript = _provider_transcript_payload(evt, channel, event_uuid_str)
            provider_summary = _provider_summary_payload(evt, channel, event_uuid_str)
            provider_recording_artifacts = _provider_recording_artifacts(evt, channel, event_uuid_str)
            archive_export_payload = _archive_export_payload(
                evt,
                channel,
                event_uuid_str,
                occurred_at,
                subject,
                enriched_source_metadata,
                provider_recording_artifacts,
            )

            await conn.execute(
                """
                DELETE FROM event_participants
                WHERE event_uuid = $1
                """,
                event_uuid,
            )

            for p in participants:
                await conn.execute(
                    """
                    INSERT INTO event_participants
                    (
                        event_uuid,
                        participant_index,
                        role,
                        display_name,
                        email_raw,
                        email_normalized,
                        phone_raw,
                        phone_normalized,
                        source_person_id,
                        source_system,
                        is_internal
                    )
                    VALUES
                    (
                        $1,
                        $2,
                        $3,
                        $4,
                        $5,
                        $6,
                        $7,
                        $8,
                        $9,
                        $10::source_system,
                        $11
                    )
                    """,
                    event_uuid,
                    p["participant_index"],
                    p["role"],
                    p["display_name"],
                    p["email_raw"],
                    p["email_normalized"],
                    p["phone_raw"],
                    p["phone_normalized"],
                    p["source_person_id"],
                    p["source_system"],
                    p["is_internal"],
                )

            await conn.execute(
                """
                INSERT INTO event_artifacts
                (
                    event_uuid,
                    kind,
                    storage_scheme,
                    storage_uri,
                    sha256,
                    byte_size,
                    content_type,
                    metadata
                )
                VALUES
                (
                    $1,
                    'webhook_raw',
                    $2,
                    $3,
                    $4,
                    $5,
                    $6,
                    $7::jsonb
                )
                """,
                event_uuid,
                artifact["storage_scheme"],
                artifact["storage_uri"],
                artifact["sha256"],
                artifact["byte_size"],
                artifact["content_type"],
                json.dumps({"artifact_uuid": artifact["artifact_uuid"]}),
            )

            await conn.execute(
                """
                DELETE FROM transcripts
                WHERE event_uuid = $1 AND engine = 'dialpad_provider'
                """,
                event_uuid,
            )

            await conn.execute(
                """
                DELETE FROM event_artifacts
                WHERE event_uuid = $1 AND kind = 'provider_recording_reference'
                """,
                event_uuid,
            )

            if provider_transcript is not None:
                transcript_row = await conn.fetchrow(
                    """
                    INSERT INTO transcripts
                    (
                        event_uuid,
                        engine,
                        language,
                        diarized,
                        text,
                        confidence,
                        metadata
                    )
                    VALUES
                    (
                        $1,
                        $2,
                        $3,
                        $4,
                        $5,
                        $6,
                        $7::jsonb
                    )
                    RETURNING transcript_uuid
                    """,
                    event_uuid,
                    provider_transcript["engine"],
                    provider_transcript["language"],
                    provider_transcript["diarized"],
                    provider_transcript["text"],
                    provider_transcript["confidence"],
                    json.dumps(provider_transcript["metadata"]),
                )
                await conn.execute(
                    """
                    INSERT INTO utterances
                    (
                        transcript_uuid,
                        sequence,
                        speaker_label,
                        text,
                        confidence,
                        metadata
                    )
                    VALUES
                    (
                        $1,
                        1,
                        $2,
                        $3,
                        $4,
                        $5::jsonb
                    )
                    """,
                    transcript_row["transcript_uuid"],
                    "provider_unknown",
                    provider_transcript["text"],
                    provider_transcript["confidence"],
                    json.dumps(
                        {
                            "provider": "dialpad",
                            "source_provenance": provider_transcript["metadata"]["source_provenance"],
                        }
                    ),
                )

            for ref in provider_recording_artifacts:
                await conn.execute(
                    """
                    INSERT INTO event_artifacts
                    (
                        event_uuid,
                        kind,
                        storage_scheme,
                        storage_uri,
                        sha256,
                        byte_size,
                        content_type,
                        metadata
                    )
                    VALUES
                    (
                        $1,
                        $2,
                        $3,
                        $4,
                        NULL,
                        NULL,
                        $5,
                        $6::jsonb
                    )
                    """,
                    event_uuid,
                    ref["kind"],
                    ref["storage_scheme"],
                    ref["storage_uri"],
                    ref["content_type"],
                    json.dumps(ref["metadata"]),
                )

            return {
                "event_uuid": str(event_uuid),
                "channel": channel,
                "external_id": external_id,
                "participant_count": len(participants),
                "participants": participants,
                "remote_identifier": remote_identifier,
                "occurred_at": occurred_at.isoformat(),
                "subject": subject,
                "archive_payload": archive_export_payload,
                "provider_transcript_preserved": provider_transcript is not None,
                "provider_summary_preserved": bool(provider_summary.get("summary_text")),
                "provider_action_item_count": len(provider_summary.get("action_items") or []),
                "provider_recording_ref_count": len(provider_recording_artifacts),
            }


def _contact_identity_for_participant(participant: Dict) -> Optional[Tuple[str, str]]:
    if participant.get("is_internal"):
        return None

    email = participant.get("email_normalized")
    if email:
        return ("email", email)

    phone = participant.get("phone_normalized")
    if phone:
        return ("phone_e164", phone)

    return None


async def _resolve_contacts_for_event(normalized: Dict) -> Dict[str, int]:
    assert _pool is not None

    event_uuid = normalized["event_uuid"]
    participants = normalized.get("participants", [])
    remote_identifier = normalized.get("remote_identifier")

    linked_contacts = 0
    linked_candidates = 0
    resolved_primary_contact: Optional[str] = None
    resolved_contact_ids: set[str] = set()

    async with httpx.AsyncClient(timeout=5.0) as client:
        async with _pool.acquire() as conn:
            async with conn.transaction():
                for participant in participants:
                    identity = _contact_identity_for_participant(participant)
                    if identity is None:
                        continue

                    identity_kind, identity_value = identity
                    try:
                        response = await client.post(
                            f"{CONTACT_ENGINE_URL.rstrip('/')}/resolve_or_track",
                            json={
                                "identity_kind": identity_kind,
                                "identity_value": identity_value,
                                "event_uuid": event_uuid,
                                "source_system": "dialpad",
                            },
                        )
                        response.raise_for_status()
                    except Exception:
                        LOG.exception(
                            "contact resolution failed: event_uuid=%s role=%s identity=%s",
                            event_uuid,
                            participant.get("role"),
                            identity_value,
                        )
                        continue

                    payload = response.json()
                    role = str(participant["role"])
                    contact_uuid = payload.get("contact_uuid")
                    candidate_uuid = payload.get("candidate_uuid")

                    if contact_uuid:
                        resolved_contact_ids.add(contact_uuid)
                        await conn.execute(
                            """
                            INSERT INTO event_contact_links
                            (
                                event_uuid,
                                contact_uuid,
                                role,
                                confidence,
                                resolver
                            )
                            VALUES
                            (
                                $1,
                                $2,
                                $3,
                                $4,
                                $5
                            )
                            ON CONFLICT (event_uuid, role, contact_uuid)
                            WHERE contact_uuid IS NOT NULL
                            DO UPDATE SET
                                confidence = EXCLUDED.confidence,
                                resolver = EXCLUDED.resolver
                            """,
                            event_uuid,
                            contact_uuid,
                            role,
                            1.0,
                            "contact_engine",
                        )
                        linked_contacts += 1
                        if identity_value == remote_identifier:
                            resolved_primary_contact = contact_uuid

                    elif candidate_uuid:
                        await conn.execute(
                            """
                            INSERT INTO event_contact_links
                            (
                                event_uuid,
                                candidate_uuid,
                                role,
                                confidence,
                                resolver
                            )
                            VALUES
                            (
                                $1,
                                $2,
                                $3,
                                $4,
                                $5
                            )
                            ON CONFLICT (event_uuid, role, candidate_uuid)
                            WHERE candidate_uuid IS NOT NULL
                            DO UPDATE SET
                                confidence = EXCLUDED.confidence,
                                resolver = EXCLUDED.resolver
                            """,
                            event_uuid,
                            candidate_uuid,
                            role,
                            1.0,
                            "contact_engine",
                        )
                        linked_candidates += 1

                if resolved_primary_contact is None and len(resolved_contact_ids) == 1:
                    resolved_primary_contact = next(iter(resolved_contact_ids))

                if resolved_primary_contact is not None:
                    await conn.execute(
                        """
                        UPDATE communication_events
                        SET contact_uuid = $2
                        WHERE event_uuid = $1
                        """,
                        event_uuid,
                        resolved_primary_contact,
                    )

    return {
        "linked_contacts": linked_contacts,
        "linked_candidates": linked_candidates,
    }


async def _enqueue_processing_jobs(normalized: Dict) -> Dict[str, int]:
    assert _pool is not None

    event_uuid = normalized["event_uuid"]
    channel = normalized["channel"]
    enqueued_jobs = 0

    jobs_to_queue: List[Tuple[str, Dict, str]] = []
    archive_payload = normalized.get("archive_payload")
    if archive_payload and channel in {"call", "voicemail", "sms"}:
        archive_hash = _archive_payload_signature(archive_payload)
        jobs_to_queue.append(
            (
                "ARCHIVE_EXPORT",
                archive_payload,
                f"archive_export:{event_uuid}:{archive_hash}",
            )
        )

    if channel in {"call", "voicemail"}:
        jobs_to_queue.append(
            (
                "TRANSCRIBE",
                {
                    "channel": channel,
                    "archive": archive_payload,
                },
                f"transcribe:{event_uuid}",
            )
        )

    if not jobs_to_queue:
        return {"enqueued_jobs": 0}

    async with _pool.acquire() as conn:
        async with conn.transaction():
            for job_type, payload, idempotency_key in jobs_to_queue:
                row = await conn.fetchrow(
                    """
                    INSERT INTO processing_jobs
                    (
                        job_type,
                        state,
                        event_uuid,
                        payload,
                        idempotency_key
                    )
                    VALUES
                    (
                        $1,
                        'QUEUED'::job_state,
                        $2,
                        $3::jsonb,
                        $4
                    )
                    ON CONFLICT (idempotency_key)
                    DO NOTHING
                    RETURNING job_uuid
                    """,
                    job_type,
                    event_uuid,
                    json.dumps(payload),
                    idempotency_key,
                )
                if row is not None:
                    enqueued_jobs += 1

    return {"enqueued_jobs": enqueued_jobs}


@app.post("/webhooks/dialpad")
async def dialpad(request: Request):
    raw = await request.body()
    content_type = request.headers.get("content-type", "application/json")

    evt = _decode_dialpad_event(raw)
    artifact = _archive("dialpad", raw, content_type)
    normalized = await _insert_communication_event(evt, artifact)
    resolution = await _resolve_contacts_for_event(normalized)
    jobs = await _enqueue_processing_jobs(normalized)

    LOG.info(
        "dialpad accepted: artifact=%s event_uuid=%s channel=%s external_id=%s participants=%s contacts=%s candidates=%s jobs=%s provider_transcript=%s provider_summary=%s action_items=%s recording_refs=%s",
        artifact["artifact_uuid"],
        normalized["event_uuid"],
        normalized["channel"],
        normalized["external_id"],
        normalized["participant_count"],
        resolution["linked_contacts"],
        resolution["linked_candidates"],
        jobs["enqueued_jobs"],
        normalized["provider_transcript_preserved"],
        normalized["provider_summary_preserved"],
        normalized["provider_action_item_count"],
        normalized["provider_recording_ref_count"],
    )

    return {
        "accepted": True,
        "artifact_uuid": artifact["artifact_uuid"],
        "event_uuid": normalized["event_uuid"],
        "channel": normalized["channel"],
        "participant_count": normalized["participant_count"],
        "linked_contacts": resolution["linked_contacts"],
        "linked_candidates": resolution["linked_candidates"],
        "enqueued_jobs": jobs["enqueued_jobs"],
        "provider_transcript_preserved": normalized["provider_transcript_preserved"],
        "provider_summary_preserved": normalized["provider_summary_preserved"],
        "provider_action_item_count": normalized["provider_action_item_count"],
        "provider_recording_ref_count": normalized["provider_recording_ref_count"],
    }


@app.post("/webhooks/dialpad_debug")
async def dialpad_debug(request: Request):
    raw = await request.body()
    LOG.warning("DIALPAD DEBUG headers=%s", dict(request.headers))
    LOG.warning("DIALPAD DEBUG body[:500]=%r", raw[:500])
    return {"ok": True}


@app.post("/internal/worker/jobs/update")
async def worker_job_update(
    req: WorkerJobUpdate,
    authorization: Optional[str] = Header(default=None),
):
    _require_worker_writeback_token(authorization)
    assert _pool is not None

    result_payload = dict(req.result or {})
    if req.error:
        result_payload["error"] = req.error
    if req.worker_id:
        result_payload["worker_id"] = req.worker_id

    async with _pool.acquire() as conn:
        async with conn.transaction():
            if req.state == "PROCESSING":
                row = await conn.fetchrow(
                    """
                    UPDATE processing_jobs
                    SET
                        state = 'PROCESSING'::job_state,
                        started_at = COALESCE(started_at, now()),
                        result = COALESCE(result, '{}'::jsonb) || $2::jsonb
                    WHERE job_uuid = $1
                    RETURNING job_uuid
                    """,
                    req.job_uuid,
                    json.dumps(result_payload),
                )
            else:
                row = await conn.fetchrow(
                    """
                    UPDATE processing_jobs
                    SET
                        state = $2::job_state,
                        started_at = COALESCE(started_at, now()),
                        finished_at = now(),
                        result = $3::jsonb
                    WHERE job_uuid = $1
                    RETURNING job_uuid
                    """,
                    req.job_uuid,
                    req.state,
                    json.dumps(result_payload),
                )

            if row is None:
                raise HTTPException(404, "job not found")

            if req.state == "SUCCEEDED":
                job_row = await conn.fetchrow(
                    """
                    SELECT event_uuid
                    FROM processing_jobs
                    WHERE job_uuid = $1
                    """,
                    req.job_uuid,
                )

                archive_result = result_payload.get("result")
                archive_files = (
                    archive_result.get("archive_files")
                    if isinstance(archive_result, dict)
                    else None
                )
                event_uuid = job_row["event_uuid"] if job_row is not None else None

                if event_uuid is not None and isinstance(archive_files, list) and archive_files:
                    await conn.execute(
                        """
                        DELETE FROM event_artifacts
                        WHERE event_uuid = $1 AND kind = 'worker_archive_file'
                        """,
                        event_uuid,
                    )

                    for archive_file in archive_files:
                        await conn.execute(
                            """
                            INSERT INTO event_artifacts
                            (
                                event_uuid,
                                kind,
                                storage_scheme,
                                storage_uri,
                                sha256,
                                byte_size,
                                content_type,
                                metadata
                            )
                            VALUES
                            (
                                $1,
                                'worker_archive_file',
                                $2,
                                $3,
                                NULL,
                                $4,
                                $5,
                                $6::jsonb
                            )
                            """,
                            event_uuid,
                            archive_file.get("storage_scheme", "local"),
                            archive_file.get("storage_uri", ""),
                            archive_file.get("byte_size"),
                            archive_file.get("content_type"),
                            json.dumps(archive_file.get("metadata") or {}),
                        )

    return {"ok": True, "job_uuid": req.job_uuid, "state": req.state}


@app.post("/ingest/gmail")
async def gmail(request: Request):
    raw = await request.body()
    ptr = _archive(
        "gmail",
        raw,
        request.headers.get("content-type", "application/json"),
    )
    LOG.info("gmail archived: %s", ptr["artifact_uuid"])
    return {"accepted": True, "artifact_uuid": ptr["artifact_uuid"]}

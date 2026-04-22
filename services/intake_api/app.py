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
from typing import Dict, List, Literal, Optional, Tuple

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
                json.dumps(evt),
            )

            event_uuid = row["event_uuid"]

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

            return {
                "event_uuid": str(event_uuid),
                "channel": channel,
                "external_id": external_id,
                "participant_count": len(participants),
                "participants": participants,
                "remote_identifier": remote_identifier,
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

    jobs_to_queue: List[Tuple[str, Dict]] = []
    if channel in {"call", "voicemail"}:
        jobs_to_queue.append(("TRANSCRIBE", {"channel": channel}))

    if not jobs_to_queue:
        return {"enqueued_jobs": 0}

    async with _pool.acquire() as conn:
        async with conn.transaction():
            for job_type, payload in jobs_to_queue:
                idempotency_key = f"{job_type.lower()}:{event_uuid}"
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
        "dialpad accepted: artifact=%s event_uuid=%s channel=%s external_id=%s participants=%s contacts=%s candidates=%s jobs=%s",
        artifact["artifact_uuid"],
        normalized["event_uuid"],
        normalized["channel"],
        normalized["external_id"],
        normalized["participant_count"],
        resolution["linked_contacts"],
        resolution["linked_candidates"],
        jobs["enqueued_jobs"],
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

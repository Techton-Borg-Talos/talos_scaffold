"""TALOS Intake API. Archive-first (Rule 1) + normalized event insert."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

import asyncpg
import jwt
from fastapi import FastAPI, HTTPException, Request

LOG = logging.getLogger("talos.intake")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

RAW = Path(os.environ.get("RAW_ARTIFACT_DIR", "/var/lib/talos/raw"))
RAW.mkdir(parents=True, exist_ok=True)

DIALPAD_SECRET = os.environ.get("DIALPAD_WEBHOOK_SECRET", "")
DATABASE_URL = os.environ["DATABASE_URL"]

app = FastAPI(title="talos-intake", version="0.2.0")
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


def _archive(source: str, raw: bytes, content_type: str) -> Dict:
    au = uuid.uuid4()
    sha = hashlib.sha256(raw).hexdigest()
    sd = RAW / source / sha[:2]
    sd.mkdir(parents=True, exist_ok=True)
    p = sd / f"{au}.bin"
    p.write_bytes(raw)
    return {
        "artifact_uuid": str(au),
        "storage_scheme": "local",
        "storage_uri": str(p),
        "sha256": sha,
        "byte_size": len(raw),
        "content_type": content_type,
    }


def _decode_dialpad_event(raw: bytes) -> Dict:
    """
    Dialpad webhook behavior:
    - if secret is provided: body is JWT signed with HS256
    - if no secret is provided: body is plain JSON
    """
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


def _channel_and_external_id(evt: Dict) -> Tuple[str, str]:
    # SMS payloads do not carry call_id; calls/voicemails do.
    if "call_id" in evt:
        state = str(evt.get("state") or "").lower()
        if state in {"voicemail", "voicemail_uploaded", "transcription"}:
            return "voicemail", str(evt["call_id"])
        return "call", str(evt["call_id"])

    # SMS/message payload
    if "id" in evt:
        return "sms", str(evt["id"])

    raise HTTPException(400, "unable to classify dialpad payload")


def _direction(evt: Dict) -> str:
    d = str(evt.get("direction") or "unknown").lower()
    if d in {"inbound", "outbound", "internal"}:
        return d
    return "unknown"


def _remote_identifier(evt: Dict, channel: str) -> Optional[str]:
    if channel in {"call", "voicemail"}:
        return evt.get("external_number") or evt.get("contact", {}).get("phone")
    # sms
    return evt.get("from_number") or evt.get("contact", {}).get("phone")


def _local_identifier(evt: Dict, channel: str) -> Optional[str]:
    if channel in {"call", "voicemail"}:
        return evt.get("internal_number") or evt.get("target", {}).get("phone")
    to_number = evt.get("to_number")
    if isinstance(to_number, list) and to_number:
        return to_number[0]
    return evt.get("target", {}).get("phone")


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
    remote_identifier = _remote_identifier(evt, channel)
    local_identifier = _local_identifier(evt, channel)
    subject = _subject(evt, channel)

    async with _pool.acquire() as conn:
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
            ON CONFLICT DO NOTHING
            """,
            row["event_uuid"],
            artifact["storage_scheme"],
            artifact["storage_uri"],
            artifact["sha256"],
            artifact["byte_size"],
            artifact["content_type"],
            json.dumps({"artifact_uuid": artifact["artifact_uuid"]}),
        )

        return {
            "event_uuid": str(row["event_uuid"]),
            "channel": channel,
            "external_id": external_id,
        }


@app.post("/webhooks/dialpad")
async def dialpad(request: Request):
    raw = await request.body()
    content_type = request.headers.get("content-type", "application/json")

    evt = _decode_dialpad_event(raw)
    artifact = _archive("dialpad", raw, content_type)
    normalized = await _insert_communication_event(evt, artifact)

    LOG.info(
        "dialpad accepted: artifact=%s event_uuid=%s channel=%s external_id=%s",
        artifact["artifact_uuid"],
        normalized["event_uuid"],
        normalized["channel"],
        normalized["external_id"],
    )

    return {
        "accepted": True,
        "artifact_uuid": artifact["artifact_uuid"],
        "event_uuid": normalized["event_uuid"],
        "channel": normalized["channel"],
    }


@app.post("/webhooks/dialpad_debug")
async def dialpad_debug(request: Request):
    raw = await request.body()
    LOG.warning("DIALPAD DEBUG headers=%s", dict(request.headers))
    LOG.warning("DIALPAD DEBUG body[:500]=%r", raw[:500])
    return {"ok": True}


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
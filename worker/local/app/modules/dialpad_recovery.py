"""modules/dialpad_recovery - recover missed Dialpad archive events after downtime."""
from __future__ import annotations

import csv
import io
import json
import logging
import os
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import NAMESPACE_URL, uuid4, uuid5

import asyncpg
import httpx

from worker_main import register
from modules import archive_export as ae

LOG = logging.getLogger("talos.worker.dialpad_recovery")

PRODUCT_DATABASE_URL = os.environ.get("PRODUCT_DATABASE_URL", "").strip()
RECOVERY_KEY = "dialpad_missed_event_recovery"
RECOVERY_LOOKBACK_HOURS = max(1, int(os.environ.get("DIALPAD_RECOVERY_LOOKBACK_HOURS", "72") or "72"))
RECOVERY_OVERLAP_MINUTES = max(0, int(os.environ.get("DIALPAD_RECOVERY_OVERLAP_MINUTES", "15") or "15"))
RECOVERY_SAFETY_DELAY_MINUTES = max(0, int(os.environ.get("DIALPAD_RECOVERY_SAFETY_DELAY_MINUTES", "5") or "5"))
RECOVERY_CHUNK_DAYS = max(1, int(os.environ.get("DIALPAD_RECOVERY_CHUNK_DAYS", "3") or "3"))
DEFAULT_OFFICE_ID = os.environ.get("DIALPAD_OFFICE_ID", "").strip() or "4943523498434560"
TEXTS_POLL_MAX = max(ae.DIALPAD_STATS_POLL_MAX, int(os.environ.get("DIALPAD_TEXTS_POLL_MAX", "24") or "24"))

_RECOVERY_DDL = """
CREATE TABLE IF NOT EXISTS dialpad_recovery_state (
    recovery_key             TEXT PRIMARY KEY,
    last_recovered_through   TIMESTAMPTZ,
    last_run_started_at      TIMESTAMPTZ,
    last_run_finished_at     TIMESTAMPTZ,
    last_run_status          TEXT NOT NULL DEFAULT 'idle',
    last_error               TEXT,
    last_job_uuid            UUID REFERENCES processing_jobs(job_uuid) ON DELETE SET NULL,
    last_payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

_OUTBOX_DDL = """
CREATE TABLE IF NOT EXISTS bridge_outbox (
    outbox_uuid   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_code    TEXT NOT NULL,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at  TIMESTAMPTZ,
    delivery_error TEXT
);
CREATE INDEX IF NOT EXISTS idx_bridge_outbox_undelivered
    ON bridge_outbox (created_at) WHERE delivered_at IS NULL;
"""


def _parse_timestamp(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.isdigit():
        raw = int(text)
        if raw > 10_000_000_000:
            return datetime.fromtimestamp(raw / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    for candidate in (
        text.replace("Z", "+00:00"),
        text.replace(" UTC", "+00:00"),
    ):
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _parse_iso_utc(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _iter_date_chunks(start_day: date, end_day: date, chunk_days: int) -> List[tuple[date, date]]:
    chunks: List[tuple[date, date]] = []
    current = start_day
    delta = timedelta(days=max(1, chunk_days) - 1)
    while current <= end_day:
        chunk_end = min(current + delta, end_day)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def _normalize_phone(value: Any) -> Optional[str]:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if not digits:
        return None
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return f"+{digits}"


def _normalize_email(value: Any) -> Optional[str]:
    text = str(value or "").strip().lower()
    return text or None


def _boolish(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _duration_ms(value: Any) -> Optional[int]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        raw = float(text)
    except ValueError:
        return None
    if raw <= 0:
        return None
    if raw > 100_000:
        return int(raw)
    return int(raw * 1000.0)


def _recording_id_from_url(url: str) -> Optional[str]:
    match = ae.re.search(
        r"/blob/(?:adminrecording|callrecording|voicemail(?:_recording)?)/(\d+)(?:\.[A-Za-z0-9]+)?(?:$|[?#])",
        str(url or ""),
        ae.re.IGNORECASE,
    )
    return match.group(1) if match else None


def _stats_target_candidates(stat_type: str) -> List[tuple[str, str]]:
    include_related = stat_type in {"texts", "calls", "voicemails", "recordings"}
    explicit_type = os.environ.get("DIALPAD_BACKFILL_TARGET_TYPE", "").strip().lower()
    explicit_id = os.environ.get("DIALPAD_BACKFILL_TARGET_ID", "").strip()
    office_id = DEFAULT_OFFICE_ID
    candidates: List[tuple[str, str]] = []
    if explicit_type and explicit_id:
        candidates.append((explicit_type, explicit_id))
    elif office_id:
        candidates.append(("office", office_id))

    if include_related:
        user_id = os.environ.get("DIALPAD_USER_ID", "").strip()
        if user_id and ("user", user_id) not in candidates:
            candidates.append(("user", user_id))
        if ae.DIALPAD_COMPANY_ID and ("company", ae.DIALPAD_COMPANY_ID) not in candidates:
            candidates.append(("company", ae.DIALPAD_COMPANY_ID))
        if office_id and ("office", office_id) not in candidates:
            candidates.append(("office", office_id))

    deduped: List[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for candidate in candidates:
        if candidate[0] and candidate[1] and candidate not in seen:
            seen.add(candidate)
            deduped.append(candidate)
    if deduped:
        return deduped
    raise RuntimeError(
        "No Dialpad recovery target configured. Set DIALPAD_BACKFILL_TARGET_ID or DIALPAD_OFFICE_ID/DIALPAD_USER_ID/DIALPAD_COMPANY_ID."
    )


async def _fetch_csv_rows(
    *,
    stat_type: str,
    target_type: str,
    target_id: str,
    date_start: str,
    date_end: str,
) -> List[Dict[str, str]]:
    poll_max = TEXTS_POLL_MAX if stat_type == "texts" else ae.DIALPAD_STATS_POLL_MAX
    request_body: Dict[str, Any] = {
        "export_type": "records",
        "stat_type": stat_type,
        "target_type": target_type,
        "target_id": target_id,
        "timezone": ae.STATS_TIMEZONE_NAME,
    }
    try:
        start_day = _parse_date(date_start)
        end_day = _parse_date(date_end)
    except Exception:
        start_day = None
        end_day = None
    today_local = datetime.now(ae._archive_timezone()).date()
    if start_day is not None and end_day is not None and end_day < today_local:
        request_body["days_ago_start"] = (today_local - start_day).days
        request_body["days_ago_end"] = (today_local - end_day).days
    else:
        request_body["date_start"] = date_start
        request_body["date_end"] = date_end

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        last_error: Optional[httpx.HTTPStatusError] = None
        for stats_timezone in ae._dialpad_stats_timezones():
            response = await ae._dialpad_request(
                "POST",
                f"{ae.DIALPAD_API_BASE}/stats",
                client=client,
                json={**request_body, "timezone": stats_timezone},
                headers={"Content-Type": "application/json"},
            )
            if response.is_error:
                body = response.text.strip()
                if response.status_code == 400 and "Invalid timezone name" in body:
                    continue
                last_error = httpx.HTTPStatusError(
                    f"Dialpad stats request failed for {stat_type} target={target_type}:{target_id}",
                    request=response.request,
                    response=response,
                )
                break
            body = response.json()
            request_id = body.get("request_id") or body.get("id")
            if not request_id:
                return []
            if ae.DIALPAD_STATS_POLL_WAIT:
                await ae.asyncio.sleep(ae.DIALPAD_STATS_POLL_WAIT)
            for _ in range(1, poll_max + 1):
                poll = await ae._dialpad_request(
                    "GET",
                    f"{ae.DIALPAD_API_BASE}/stats/{request_id}",
                    client=client,
                )
                poll.raise_for_status()
                poll_body = poll.json()
                download_url = poll_body.get("download_url") or poll_body.get("url") or poll_body.get("file_url")
                if download_url:
                    download = await ae._dialpad_request("GET", download_url, client=client)
                    download.raise_for_status()
                    reader = csv.DictReader(io.StringIO(download.text.lstrip("\ufeff")))
                    return [
                        {
                            str(key or "").strip().strip('"'): str(value or "").strip().strip('"')
                            for key, value in row.items()
                        }
                        for row in reader
                    ]
                state = str(poll_body.get("state") or poll_body.get("status") or "").lower()
                if state in {"failed", "error"}:
                    return []
                await ae.asyncio.sleep(ae.DIALPAD_STATS_POLL_RETRY)
        if last_error is not None:
            raise last_error
    return []


def _row_unique_key(stat_type: str, row: Dict[str, Any]) -> str:
    if stat_type == "texts":
        explicit = str(row.get("message_id") or row.get("id") or "").strip()
        if explicit:
            return explicit
        sms_key = "|".join(
            [
                str(row.get("date") or "").strip(),
                str(row.get("created_date") or "").strip(),
                str(row.get("from_phone") or "").strip(),
                str(row.get("to_phone") or "").strip(),
                str(row.get("encrypted_aes_text") or row.get("encrypted_text") or row.get("text") or "").strip(),
            ]
        )
        return str(uuid5(NAMESPACE_URL, f"dialpad-historical:texts:{sms_key}"))
    if stat_type == "recordings":
        recording_key = "|".join(
            [
                str(row.get("call_id") or row.get("id") or "").strip(),
                str(row.get("recording_id") or "").strip(),
                str(row.get("recording_url") or row.get("url") or "").strip(),
                str(row.get("date_started") or row.get("date") or "").strip(),
            ]
        )
        return str(uuid5(NAMESPACE_URL, f"dialpad-historical:recordings:{recording_key}"))
    explicit = str(row.get("call_id") or row.get("id") or "").strip()
    if explicit:
        return explicit
    return json.dumps(row, sort_keys=True)


def _row_timestamp_candidates(stat_type: str, row: Dict[str, Any]) -> List[Any]:
    if stat_type == "texts":
        return [row.get("date"), row.get("created_date")]
    if stat_type == "voicemails":
        return [row.get("date"), row.get("date_started"), row.get("date_ended")]
    return [row.get("date_started"), row.get("date_connected"), row.get("date_ended"), row.get("date")]


def _row_occurred_at(stat_type: str, row: Dict[str, Any]) -> Optional[datetime]:
    for candidate in _row_timestamp_candidates(stat_type, row):
        parsed = _parse_timestamp(candidate)
        if parsed is not None:
            return parsed.astimezone(timezone.utc)
    return None


def _row_in_window(stat_type: str, row: Dict[str, Any], start_at: datetime, end_at: datetime) -> bool:
    occurred_at = _row_occurred_at(stat_type, row)
    if occurred_at is None:
        return False
    return start_at <= occurred_at <= end_at


def _sort_rows(stat_type: str, rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    def _sort_key(row: Dict[str, str]) -> tuple[str, str]:
        occurred_at = _row_occurred_at(stat_type, row)
        return ((occurred_at.isoformat() if occurred_at is not None else ""), _row_unique_key(stat_type, row))
    return sorted(rows, key=_sort_key)


def _deterministic_event_uuid(channel: str, external_id: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"dialpad-historical:{channel}:{external_id}"))


def _row_target_type(row: Dict[str, Any]) -> str:
    raw = str(
        row.get("target_type")
        or row.get("TargetKind")
        or row.get("target_kind")
        or os.environ.get("DIALPAD_BACKFILL_TARGET_ENTITY_TYPE", "user")
        or "user"
    ).strip()
    lowered = raw.lower()
    if lowered in {"userprofile", "user"}:
        return "user"
    if lowered in {"office", "company"}:
        return lowered
    return lowered or "user"


def _row_target_id(row: Dict[str, Any]) -> Optional[str]:
    return str(
        row.get("target_id")
        or row.get("TargetID")
        or os.environ.get("DIALPAD_BACKFILL_TARGET_ENTITY_ID", "")
        or ""
    ).strip() or None


def _base_target_metadata(internal_number: Optional[str], office_id: str, row: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    row = row or {}
    return {
        "id": _row_target_id(row),
        "name": os.environ.get("DIALPAD_BACKFILL_TARGET_NAME", "").strip() or None,
        "type": _row_target_type(row),
        "email": _normalize_email(os.environ.get("DIALPAD_BACKFILL_TARGET_EMAIL", "")),
        "phone": internal_number,
        "office_id": office_id,
    }


def _contact_metadata(row: Dict[str, Any], external_number: Optional[str]) -> Dict[str, Any]:
    return {
        "id": str(row.get("contact_id") or row.get("message_id") or row.get("call_id") or "").strip() or None,
        "name": row.get("contact_name") or None,
        "type": row.get("contact_type") or "historical",
        "email": _normalize_email(row.get("contact_email")),
        "phone": external_number,
    }


def _source_provenance(row: Dict[str, Any], channel: str, external_id: str, office_id: str) -> Dict[str, Any]:
    direction = str(row.get("direction") or "").strip().lower() or None
    from_number = _normalize_phone(row.get("from_phone"))
    to_number = _normalize_phone(row.get("to_phone"))
    raw_selected_caller_id = _normalize_phone(row.get("selected_caller_id"))
    selected_caller_id = raw_selected_caller_id
    external_number = _normalize_phone(row.get("external_number") or row.get("from_phone"))
    internal_number = _normalize_phone(row.get("internal_number") or row.get("to_phone"))
    if channel == "sms":
        if direction in {"internal", "outbound", "sent"}:
            internal_number = raw_selected_caller_id or from_number or internal_number
            external_number = to_number or external_number or internal_number
            selected_caller_id = raw_selected_caller_id or from_number or internal_number
        else:
            external_number = from_number or external_number
            internal_number = to_number or internal_number or raw_selected_caller_id
            selected_caller_id = raw_selected_caller_id or internal_number
    else:
        selected_caller_id = _normalize_phone(row.get("selected_caller_id") or row.get("internal_number"))
    return {
        "source_system": "dialpad",
        "capture_mode": "historical_backfill",
        "external_id": external_id,
        "call_id": str(row.get("call_id") or external_id or "").strip() or None,
        "channel": channel,
        "direction": direction,
        "external_number": external_number,
        "internal_number": internal_number,
        "from_number": from_number,
        "to_number": to_number,
        "selected_caller_id": selected_caller_id,
        "target": _base_target_metadata(internal_number, office_id, row),
        "contact": _contact_metadata(row, external_number),
    }


def _row_external_id(channel: str, row: Dict[str, Any]) -> str:
    if channel == "sms":
        explicit = str(row.get("message_id") or row.get("id") or "").strip()
        if explicit:
            return explicit
        sms_key = "|".join(
            [
                str(row.get("date") or "").strip(),
                str(row.get("from_phone") or "").strip(),
                str(row.get("to_phone") or "").strip(),
                str(row.get("encrypted_aes_text") or row.get("encrypted_text") or "").strip(),
            ]
        )
        return str(uuid5(NAMESPACE_URL, f"dialpad-historical:sms:{sms_key}"))
    return str(row.get("call_id") or row.get("id") or "").strip()


def _recording_detail_from_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    url = str(row.get("recording_url") or row.get("url") or "").strip()
    if not url.startswith("http"):
        return None
    recording_id = str(row.get("recording_id") or "").strip() or _recording_id_from_url(url)
    return {
        "id": recording_id or None,
        "url": url,
        "duration": _duration_ms(row.get("duration")),
        "start_time": row.get("date_started") or row.get("date"),
        "recording_type": row.get("recording_type") or "admincallrecording",
    }


def _recording_reference(
    *,
    event_uuid: str,
    source_provenance: Dict[str, Any],
    detail: Dict[str, Any],
    reference_type: str,
) -> Dict[str, Any]:
    return {
        "event_uuid": event_uuid,
        "kind": "provider_recording_reference",
        "storage_scheme": "dialpad",
        "storage_uri": str(detail.get("url") or ""),
        "content_type": "text/uri-list",
        "metadata": {
            "provider": "dialpad",
            "reference_type": reference_type,
            "recording_id": detail.get("id"),
            "recording_type": detail.get("recording_type"),
            "duration_ms": detail.get("duration"),
            "start_time": detail.get("start_time"),
            "source_provenance": source_provenance,
        },
    }


def _base_source_metadata(
    *,
    row: Dict[str, Any],
    channel: str,
    source_provenance: Dict[str, Any],
    recording_details: List[Dict[str, Any]],
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = dict(row)
    payload["state"] = payload.get("state") or "historical_backfill"
    payload["call_id"] = payload.get("call_id") or source_provenance.get("call_id")
    payload["external_number"] = source_provenance.get("external_number")
    payload["internal_number"] = source_provenance.get("internal_number")
    payload["direction"] = source_provenance.get("direction")
    if recording_details:
        payload["recording_details"] = recording_details
        payload["admin_recording_urls"] = [detail.get("url") for detail in recording_details if detail.get("url")]
    if extra:
        payload.update(extra)
    payload["_talos"] = {
        "source_provenance": source_provenance,
        "capture_mode": "historical_backfill",
        "stat_channel": channel,
    }
    return payload


async def _archive_sms_row(row: Dict[str, Any], office_id: str) -> Dict[str, Any]:
    external_id = _row_external_id("sms", row)
    event_uuid = _deterministic_event_uuid("sms", external_id)
    occurred_at = _row_occurred_at("texts", row) or datetime.now(timezone.utc)
    source_provenance = _source_provenance(row, "sms", external_id, office_id)
    payload = {
        "event_uuid": event_uuid,
        "channel": "sms",
        "external_id": external_id,
        "occurred_at": occurred_at.isoformat(),
        "subject": "dialpad historical sms",
        "source_provenance": source_provenance,
        "source_metadata": _base_source_metadata(
            row=row,
            channel="sms",
            source_provenance=source_provenance,
            recording_details=[],
        ),
        "sms_text": str(row.get("encrypted_aes_text") or row.get("encrypted_text") or row.get("text") or "").strip() or None,
        "recording_references": [],
    }
    return await ae.handle_archive_export({
        "job_uuid": str(uuid4()),
        "job_type": "ARCHIVE_EXPORT",
        "event_uuid": event_uuid,
        "payload": payload,
    })


async def _archive_voicemail_row(row: Dict[str, Any], office_id: str) -> Dict[str, Any]:
    external_id = _row_external_id("voicemail", row)
    event_uuid = _deterministic_event_uuid("voicemail", external_id)
    occurred_at = _row_occurred_at("voicemails", row) or datetime.now(timezone.utc)
    source_provenance = _source_provenance(row, "voicemail", external_id, office_id)
    recording_detail = _recording_detail_from_row(
        {
            "recording_url": row.get("recording_url"),
            "recording_id": row.get("voicemail_recording_id") or row.get("recording_id"),
            "recording_type": "voicemail",
            "duration": row.get("duration"),
            "date_started": row.get("date") or row.get("date_started"),
        }
    )
    recording_details = [recording_detail] if recording_detail is not None else []
    recording_refs = [
        _recording_reference(
            event_uuid=event_uuid,
            source_provenance=source_provenance,
            detail=recording_detail,
            reference_type="historical_recording_url",
        )
        for recording_detail in recording_details
        if recording_detail.get("url")
    ]
    payload = {
        "event_uuid": event_uuid,
        "channel": "voicemail",
        "external_id": external_id,
        "occurred_at": occurred_at.isoformat(),
        "subject": "dialpad historical voicemail",
        "source_provenance": source_provenance,
        "source_metadata": _base_source_metadata(
            row=row,
            channel="voicemail",
            source_provenance=source_provenance,
            recording_details=recording_details,
            extra={
                "voicemail_link": row.get("recording_url") or None,
                "voicemail_recording_id": row.get("voicemail_recording_id") or row.get("recording_id") or None,
            },
        ),
        "recording_references": recording_refs,
    }
    return await ae.handle_archive_export({
        "job_uuid": str(uuid4()),
        "job_type": "ARCHIVE_EXPORT",
        "event_uuid": event_uuid,
        "payload": payload,
    })


async def _archive_call_row(row: Dict[str, Any], office_id: str, recording_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    external_id = _row_external_id("call", row)
    event_uuid = _deterministic_event_uuid("call", external_id)
    occurred_at = _row_occurred_at("calls", row) or datetime.now(timezone.utc)
    source_provenance = _source_provenance(row, "call", external_id, office_id)
    recording_details = [
        detail for detail in (_recording_detail_from_row(item) for item in recording_rows)
        if detail is not None
    ]
    recording_refs = [
        _recording_reference(
            event_uuid=event_uuid,
            source_provenance=source_provenance,
            detail=detail,
            reference_type="historical_recording_url",
        )
        for detail in recording_details
        if detail.get("url")
    ]
    payload = {
        "event_uuid": event_uuid,
        "channel": "call",
        "external_id": external_id,
        "occurred_at": occurred_at.isoformat(),
        "subject": "dialpad historical call",
        "source_provenance": source_provenance,
        "source_metadata": _base_source_metadata(
            row=row,
            channel="call",
            source_provenance=source_provenance,
            recording_details=recording_details,
            extra={"was_recorded": _boolish(row.get("was_recorded"))},
        ),
        "recording_references": recording_refs,
    }
    return await ae.handle_archive_export({
        "job_uuid": str(uuid4()),
        "job_type": "ARCHIVE_EXPORT",
        "event_uuid": event_uuid,
        "payload": payload,
    })


async def _emit_conn(conn: asyncpg.Connection, event_code: str, payload: Dict[str, Any]) -> None:
    await conn.execute(_OUTBOX_DDL)
    await conn.execute(
        "INSERT INTO bridge_outbox (event_code, payload) VALUES ($1, $2::jsonb)",
        event_code,
        json.dumps(payload),
    )


def _compute_window(state_row: Optional[asyncpg.Record], payload: Dict[str, Any]) -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    overlap = timedelta(minutes=RECOVERY_OVERLAP_MINUTES)
    safety_delay = timedelta(minutes=RECOVERY_SAFETY_DELAY_MINUTES)
    fallback_lookback = timedelta(hours=RECOVERY_LOOKBACK_HOURS)
    observed_offline_since = _parse_iso_utc(payload.get("observed_offline_since"))
    observed_online_at = _parse_iso_utc(payload.get("observed_online_at")) or now
    last_recovered = (
        state_row["last_recovered_through"].astimezone(timezone.utc)
        if state_row and state_row["last_recovered_through"] is not None
        else None
    )

    candidates = []
    if last_recovered is not None:
        candidates.append(last_recovered - overlap)
    if observed_offline_since is not None:
        candidates.append(observed_offline_since - overlap)
    if not candidates:
        candidates.append(now - fallback_lookback)
    start_at = min(candidates)
    end_at = observed_online_at - safety_delay
    if end_at <= start_at:
        end_at = start_at + timedelta(minutes=1)
    return start_at, end_at


async def _mark_state(
    conn: asyncpg.Connection,
    *,
    job_uuid: str,
    status: str,
    payload: Dict[str, Any],
    last_recovered_through: Optional[datetime] = None,
    error: Optional[str] = None,
) -> None:
    await conn.execute(_RECOVERY_DDL)
    await conn.execute(
        """
        INSERT INTO dialpad_recovery_state
            (recovery_key, last_recovered_through, last_run_started_at, last_run_finished_at,
             last_run_status, last_error, last_job_uuid, last_payload, updated_at)
        VALUES
            ($1, $2, CASE WHEN $3='running' THEN now() ELSE NULL END, CASE WHEN $3='running' THEN NULL ELSE now() END,
             $3, $4, $5::uuid, $6::jsonb, now())
        ON CONFLICT (recovery_key) DO UPDATE SET
            last_recovered_through = COALESCE($2, dialpad_recovery_state.last_recovered_through),
            last_run_started_at = CASE WHEN $3='running' THEN now() ELSE dialpad_recovery_state.last_run_started_at END,
            last_run_finished_at = CASE WHEN $3='running' THEN NULL ELSE now() END,
            last_run_status = $3,
            last_error = $4,
            last_job_uuid = $5::uuid,
            last_payload = $6::jsonb,
            updated_at = now()
        """,
        RECOVERY_KEY,
        last_recovered_through,
        status,
        error,
        job_uuid,
        json.dumps(payload),
    )


@register("DIALPAD_MISSED_EVENT_RECOVERY", "dialpad_recovery")
async def handle_dialpad_missed_event_recovery(job: Dict[str, Any]) -> Dict[str, Any]:
    if not PRODUCT_DATABASE_URL:
        raise RuntimeError("PRODUCT_DATABASE_URL is not configured for Dialpad recovery")
    if not (ae.DIALPAD_ACCESS_TOKEN or ae.DIALPAD_REFRESH_TOKEN):
        raise RuntimeError("Dialpad OAuth credentials are not configured for recovery")

    job_uuid = str(job.get("job_uuid") or "")
    payload = dict(job.get("payload") or {})
    office_id = DEFAULT_OFFICE_ID
    pool = await asyncpg.create_pool(PRODUCT_DATABASE_URL, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            await conn.execute(_RECOVERY_DDL)
            state_row = await conn.fetchrow(
                "SELECT * FROM dialpad_recovery_state WHERE recovery_key=$1",
                RECOVERY_KEY,
            )
            start_at, end_at = _compute_window(state_row, payload)
            state_payload = {
                **payload,
                "window_start": start_at.isoformat(),
                "window_end": end_at.isoformat(),
            }
            await _mark_state(conn, job_uuid=job_uuid, status="running", payload=state_payload)
            await _emit_conn(conn, "DIALPAD_RECOVERY_STARTED", {
                "job_uuid": job_uuid,
                "window_start": start_at.isoformat(),
                "window_end": end_at.isoformat(),
            })

        chunks = _iter_date_chunks(
            start_at.astimezone(ae._archive_timezone()).date(),
            end_at.astimezone(ae._archive_timezone()).date(),
            RECOVERY_CHUNK_DAYS,
        )
        text_targets = _stats_target_candidates("texts")
        call_targets = _stats_target_candidates("calls")
        voicemail_targets = _stats_target_candidates("voicemails")
        recording_targets = _stats_target_candidates("recordings")
        seen_keys: Dict[str, set[str]] = {name: set() for name in ("texts", "calls", "voicemails", "recordings")}
        recordings_by_call_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        summary: Dict[str, Any] = {
            "window_start": start_at.isoformat(),
            "window_end": end_at.isoformat(),
            "chunk_days": RECOVERY_CHUNK_DAYS,
            "counts": {
                "texts_archived": 0,
                "calls_archived": 0,
                "voicemails_archived": 0,
                "recordings_seen": 0,
            },
        }

        for chunk_start, chunk_end in chunks:
            date_start = chunk_start.isoformat()
            date_end = chunk_end.isoformat()

            text_rows: List[Dict[str, Any]] = []
            for target_type, target_id in text_targets:
                text_rows.extend(await _fetch_csv_rows(
                    stat_type="texts",
                    target_type=target_type,
                    target_id=target_id,
                    date_start=date_start,
                    date_end=date_end,
                ))
            for row in _sort_rows("texts", text_rows):
                key = _row_unique_key("texts", row)
                if key in seen_keys["texts"] or not _row_in_window("texts", row, start_at, end_at):
                    continue
                seen_keys["texts"].add(key)
                await _archive_sms_row(row, office_id)
                summary["counts"]["texts_archived"] += 1

            voicemail_rows: List[Dict[str, Any]] = []
            for target_type, target_id in voicemail_targets:
                voicemail_rows.extend(await _fetch_csv_rows(
                    stat_type="voicemails",
                    target_type=target_type,
                    target_id=target_id,
                    date_start=date_start,
                    date_end=date_end,
                ))
            for row in _sort_rows("voicemails", voicemail_rows):
                key = _row_unique_key("voicemails", row)
                if key in seen_keys["voicemails"] or not _row_in_window("voicemails", row, start_at, end_at):
                    continue
                seen_keys["voicemails"].add(key)
                await _archive_voicemail_row(row, office_id)
                summary["counts"]["voicemails_archived"] += 1

            recording_rows: List[Dict[str, Any]] = []
            for target_type, target_id in recording_targets:
                recording_rows.extend(await _fetch_csv_rows(
                    stat_type="recordings",
                    target_type=target_type,
                    target_id=target_id,
                    date_start=date_start,
                    date_end=date_end,
                ))
            for row in recording_rows:
                key = _row_unique_key("recordings", row)
                if key in seen_keys["recordings"]:
                    continue
                seen_keys["recordings"].add(key)
                call_id = str(row.get("call_id") or row.get("id") or "").strip()
                if call_id:
                    recordings_by_call_id[call_id].append(row)
                if _row_in_window("calls", row, start_at, end_at):
                    summary["counts"]["recordings_seen"] += 1

            call_rows: List[Dict[str, Any]] = []
            for target_type, target_id in call_targets:
                call_rows.extend(await _fetch_csv_rows(
                    stat_type="calls",
                    target_type=target_type,
                    target_id=target_id,
                    date_start=date_start,
                    date_end=date_end,
                ))
            for row in _sort_rows("calls", call_rows):
                key = _row_unique_key("calls", row)
                if key in seen_keys["calls"] or not _row_in_window("calls", row, start_at, end_at):
                    continue
                seen_keys["calls"].add(key)
                call_id = str(row.get("call_id") or row.get("id") or "").strip()
                await _archive_call_row(row, office_id, recordings_by_call_id.get(call_id, []))
                summary["counts"]["calls_archived"] += 1

        async with pool.acquire() as conn:
            await _mark_state(
                conn,
                job_uuid=job_uuid,
                status="succeeded",
                payload=summary,
                last_recovered_through=end_at,
                error=None,
            )
            await _emit_conn(conn, "DIALPAD_RECOVERY_SUCCEEDED", {
                "job_uuid": job_uuid,
                "window_start": start_at.isoformat(),
                "window_end": end_at.isoformat(),
                "counts": summary["counts"],
            })
        summary["status"] = "ok"
        return summary
    except Exception as exc:
        async with pool.acquire() as conn:
            await _mark_state(
                conn,
                job_uuid=job_uuid,
                status="failed",
                payload=payload,
                last_recovered_through=None,
                error=str(exc),
            )
            await _emit_conn(conn, "DIALPAD_RECOVERY_FAILED", {
                "job_uuid": job_uuid,
                "error": str(exc),
            })
        LOG.exception("Dialpad missed-event recovery failed")
        raise
    finally:
        await pool.close()

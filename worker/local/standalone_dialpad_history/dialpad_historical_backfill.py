"""Standalone historical Dialpad backfill.

This host-run script fetches historical Dialpad records from the Stats API and
archives them directly into ``DIALPAD_ARCHIVE_ROOT`` using the same archive
layout as the live system.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import os
import textwrap
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse
from uuid import NAMESPACE_URL, uuid4, uuid5

import httpx

import dialpad_history_common as ae


DEFAULT_START_DATE = "2026-01-01"
DEFAULT_OFFICE_ID = "4943523498434560"
DEFAULT_CHUNK_DAYS = 7
DEFAULT_PROGRESS_EVERY = 10
DEFAULT_TYPES = ("calls", "texts", "voicemails")


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


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


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
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


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
    parsed = urlparse(url)
    path = parsed.path or ""
    parts = [part for part in path.split("/") if part]
    if len(parts) >= 2 and parts[0] == "blob":
        basename = parts[1].split(".")[0]
        digits = "".join(ch for ch in basename if ch.isdigit())
        return digits or None
    return None


def _iter_date_chunks(start_day: date, end_day: date, chunk_days: int) -> Iterable[tuple[date, date]]:
    current = start_day
    delta = timedelta(days=max(1, chunk_days) - 1)
    while current <= end_day:
        chunk_end = min(current + delta, end_day)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


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
    return [
        row.get("date_started"),
        row.get("date_connected"),
        row.get("date_ended"),
        row.get("date"),
    ]


def _row_local_day(stat_type: str, row: Dict[str, Any]) -> Optional[date]:
    archive_tz = ae._archive_timezone()
    for candidate in _row_timestamp_candidates(stat_type, row):
        parsed = _parse_timestamp(candidate)
        if parsed is not None:
            return parsed.astimezone(archive_tz).date()
    return None


def _sort_rows_for_archive(stat_type: str, rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    def _sort_key(row: Dict[str, str]) -> tuple[str, str]:
        parsed_text = ""
        for candidate in _row_timestamp_candidates(stat_type, row):
            parsed = _parse_timestamp(candidate)
            if parsed is not None:
                parsed_text = parsed.astimezone(timezone.utc).isoformat()
                break
        return parsed_text, _row_unique_key(stat_type, row)

    return sorted(rows, key=_sort_key)


def _filter_rows_to_chunk(
    stat_type: str,
    rows: List[Dict[str, str]],
    chunk_start: date,
    chunk_end: date,
) -> tuple[List[Dict[str, str]], int]:
    kept: List[Dict[str, str]] = []
    dropped = 0
    for row in rows:
        local_day = _row_local_day(stat_type, row)
        if local_day is not None and not (chunk_start <= local_day <= chunk_end):
            dropped += 1
            continue
        kept.append(row)
    return kept, dropped


def _should_log_archive_progress(index: int, total: int, every: int) -> bool:
    if total <= 0:
        return False
    if index == 1 or index == total:
        return True
    return every > 0 and index % every == 0


def _deterministic_event_uuid(channel: str, external_id: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"dialpad-historical:{channel}:{external_id}"))


def _stats_target_candidates(target_type: Optional[str], target_id: Optional[str], office_id: str) -> List[tuple[str, str]]:
    explicit_type = str(target_type or "").strip().lower()
    explicit_id = str(target_id or "").strip()
    if explicit_type and explicit_id:
        return [(explicit_type, explicit_id)]

    env_type = os.environ.get("DIALPAD_BACKFILL_TARGET_TYPE", "").strip().lower()
    env_id = os.environ.get("DIALPAD_BACKFILL_TARGET_ID", "").strip()
    if env_type and env_id:
        return [(env_type, env_id)]

    candidates: List[tuple[str, str]] = []
    if office_id:
        candidates.append(("office", office_id))

    user_id = os.environ.get("DIALPAD_USER_ID", "").strip()
    if user_id:
        candidates.append(("user", user_id))

    if ae.DIALPAD_COMPANY_ID:
        candidates.append(("company", ae.DIALPAD_COMPANY_ID))

    deduped: List[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    if deduped:
        return deduped
    raise SystemExit(
        "No Dialpad backfill target configured. Set DIALPAD_BACKFILL_TARGET_ID or one of DIALPAD_OFFICE_ID/DIALPAD_USER_ID/DIALPAD_COMPANY_ID."
    )


def _base_target_metadata(internal_number: Optional[str], office_id: str) -> Dict[str, Any]:
    return {
        "id": os.environ.get("DIALPAD_BACKFILL_TARGET_ENTITY_ID", "").strip() or None,
        "name": os.environ.get("DIALPAD_BACKFILL_TARGET_NAME", "").strip() or None,
        "type": os.environ.get("DIALPAD_BACKFILL_TARGET_ENTITY_TYPE", "user").strip() or "user",
        "email": _normalize_email(os.environ.get("DIALPAD_BACKFILL_TARGET_EMAIL", "")),
        "phone": internal_number,
        "office_id": office_id,
    }


def _contact_metadata(row: Dict[str, Any], external_number: Optional[str]) -> Dict[str, Any]:
    return {
        "id": str(row.get("contact_id") or row.get("message_id") or row.get("call_id") or "").strip() or None,
        "name": row.get("contact_name") or row.get("name") or None,
        "type": row.get("contact_type") or "historical",
        "email": _normalize_email(row.get("contact_email")),
        "phone": external_number,
    }


def _source_provenance(row: Dict[str, Any], channel: str, external_id: str, office_id: str) -> Dict[str, Any]:
    external_number = _normalize_phone(row.get("external_number") or row.get("from_phone"))
    internal_number = _normalize_phone(row.get("internal_number") or row.get("to_phone"))
    return {
        "source_system": "dialpad",
        "capture_mode": "historical_backfill",
        "external_id": external_id,
        "call_id": str(row.get("call_id") or external_id or "").strip() or None,
        "channel": channel,
        "direction": str(row.get("direction") or "").strip().lower() or None,
        "external_number": external_number,
        "internal_number": internal_number,
        "from_number": _normalize_phone(row.get("from_phone")),
        "selected_caller_id": _normalize_phone(row.get("selected_caller_id") or row.get("internal_number")),
        "target": _base_target_metadata(internal_number, office_id),
        "contact": _contact_metadata(row, external_number),
    }


def _recording_reference(
    *,
    event_uuid: str,
    source_provenance: Dict[str, Any],
    url: str,
    reference_type: str,
    recording_id: Optional[str],
    recording_type: Optional[str],
    duration_ms: Optional[int],
    start_time: Any,
) -> Dict[str, Any]:
    return {
        "event_uuid": event_uuid,
        "kind": "provider_recording_reference",
        "storage_scheme": "dialpad",
        "storage_uri": url,
        "content_type": "text/uri-list",
        "metadata": {
            "provider": "dialpad",
            "reference_type": reference_type,
            "recording_id": recording_id,
            "recording_type": recording_type,
            "duration_ms": duration_ms,
            "start_time": start_time,
            "source_provenance": source_provenance,
        },
    }


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


def _flatten_transcript_payload(payload: Dict[str, Any]) -> Optional[str]:
    lines = payload.get("lines") or []
    if not isinstance(lines, list):
        direct_text = str(payload.get("text") or payload.get("transcript") or "").strip()
        return direct_text or None
    rendered: List[str] = []
    for line in lines:
        if not isinstance(line, dict):
            continue
        if str(line.get("type") or "").strip().lower() != "transcript":
            continue
        content = str(line.get("content") or line.get("text") or "").strip()
        if not content:
            continue
        speaker = (
            str(line.get("speaker_name") or line.get("speaker_label") or line.get("speaker") or "").strip()
        )
        rendered.append(f"{speaker}: {content}" if speaker else content)
    joined = "\n".join(rendered).strip()
    return joined or None


async def _fetch_csv_rows(
    *,
    stat_type: str,
    target_type: str,
    target_id: str,
    date_start: str,
    date_end: str,
) -> List[Dict[str, str]]:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        last_error: Optional[httpx.HTTPStatusError] = None
        for stats_timezone in ae._dialpad_stats_timezones():
            response = await ae._dialpad_request(
                "POST",
                f"{ae.DIALPAD_API_BASE}/stats",
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
            if response.is_error:
                body = response.text.strip()
                if response.status_code == 400 and "Invalid timezone name" in body:
                    continue
                detail = body
                try:
                    parsed = response.json()
                    detail = json.dumps(parsed, sort_keys=True)
                except Exception:
                    pass
                detail = textwrap.shorten(detail.replace("\n", " "), width=500, placeholder="...")
                last_error = httpx.HTTPStatusError(
                    f"Dialpad stats request failed for stat_type={stat_type} "
                    f"target={target_type}:{target_id} {date_start}->{date_end}: "
                    f"HTTP {response.status_code} {detail}",
                    request=response.request,
                    response=response,
                )
                break
            body = response.json()
            request_id = body.get("request_id") or body.get("id")
            if not request_id:
                return []

            if ae.DIALPAD_STATS_POLL_WAIT:
                await asyncio.sleep(ae.DIALPAD_STATS_POLL_WAIT)

            for _ in range(ae.DIALPAD_STATS_POLL_MAX):
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
                await asyncio.sleep(ae.DIALPAD_STATS_POLL_RETRY)
            return []
        if last_error is not None:
            raise last_error
    return []


async def _fetch_call_transcript(call_id: str) -> Optional[str]:
    if not call_id:
        return None
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        response = await ae._dialpad_request(
            "GET",
            f"{ae.DIALPAD_API_BASE}/transcripts/{call_id}",
            client=client,
            retry_on_401=True,
        )
        if response.status_code in {403, 404}:
            return None
        response.raise_for_status()
        body = response.json()
    return _flatten_transcript_payload(body if isinstance(body, dict) else {})


def _occurred_at_for_row(channel: str, row: Dict[str, Any], fallback_day: date) -> datetime:
    candidates: List[Any] = []
    if channel == "sms":
        candidates.extend([row.get("date"), row.get("created_date")])
    elif channel == "voicemail":
        candidates.extend([row.get("date"), row.get("date_started"), row.get("date_ended")])
    else:
        candidates.extend(
            [
                row.get("date_started"),
                row.get("date_connected"),
                row.get("date_ended"),
                row.get("date"),
            ]
        )
    for candidate in candidates:
        parsed = _parse_timestamp(candidate)
        if parsed is not None:
            return parsed.astimezone(timezone.utc)
    return datetime.combine(fallback_day, time.min, tzinfo=timezone.utc)


def _row_external_id(channel: str, row: Dict[str, Any]) -> str:
    if channel == "sms":
        text_key = "|".join(
            [
                str(row.get("message_id") or "").strip(),
                str(row.get("date") or "").strip(),
                str(row.get("from_phone") or "").strip(),
                str(row.get("to_phone") or "").strip(),
                str(row.get("encrypted_aes_text") or row.get("encrypted_text") or "").strip(),
            ]
        )
        explicit = str(row.get("message_id") or row.get("id") or "").strip()
        if explicit:
            return explicit
        return str(uuid5(NAMESPACE_URL, f"dialpad-historical:sms:{text_key}"))
    return str(row.get("call_id") or row.get("id") or "").strip()


def _base_source_metadata(
    *,
    row: Dict[str, Any],
    channel: str,
    source_provenance: Dict[str, Any],
    provider_transcript_text: Optional[str],
    recording_details: List[Dict[str, Any]],
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = dict(row)
    payload["state"] = payload.get("state") or "historical_backfill"
    payload["call_id"] = payload.get("call_id") or source_provenance.get("call_id")
    payload["external_number"] = source_provenance.get("external_number")
    payload["internal_number"] = source_provenance.get("internal_number")
    payload["direction"] = source_provenance.get("direction")
    if provider_transcript_text:
        payload["transcription_text"] = provider_transcript_text
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


async def _archive_sms_row(row: Dict[str, Any], fallback_day: date, office_id: str) -> Dict[str, Any]:
    external_id = _row_external_id("sms", row)
    event_uuid = _deterministic_event_uuid("sms", external_id)
    occurred_at = _occurred_at_for_row("sms", row, fallback_day)
    source_provenance = _source_provenance(row, "sms", external_id, office_id)
    sms_text = str(row.get("encrypted_aes_text") or row.get("encrypted_text") or row.get("text") or "").strip() or None
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
            provider_transcript_text=None,
            recording_details=[],
        ),
        "provider_transcript_text": None,
        "sms_text": sms_text,
        "recording_references": [],
    }
    job = {
        "job_uuid": str(uuid4()),
        "job_type": "ARCHIVE_EXPORT",
        "event_uuid": event_uuid,
        "payload": payload,
    }
    return await ae.handle_archive_export(job)


async def _archive_voicemail_row(row: Dict[str, Any], fallback_day: date, office_id: str) -> Dict[str, Any]:
    external_id = _row_external_id("voicemail", row)
    event_uuid = _deterministic_event_uuid("voicemail", external_id)
    occurred_at = _occurred_at_for_row("voicemail", row, fallback_day)
    source_provenance = _source_provenance(row, "voicemail", external_id, office_id)
    recording_url = str(row.get("recording_url") or "").strip()
    recording_id = str(row.get("recording_id") or "").strip() or _recording_id_from_url(recording_url or "")
    recording_detail = _recording_detail_from_row(
        {
            "recording_url": recording_url,
            "recording_id": recording_id,
            "recording_type": "voicemail",
            "duration": row.get("duration"),
            "date_started": row.get("date") or row.get("date_started"),
        }
    )
    recording_details = [recording_detail] if recording_detail else []
    recording_refs = []
    if recording_detail and recording_url:
        recording_refs.append(
            _recording_reference(
                event_uuid=event_uuid,
                source_provenance=source_provenance,
                url=recording_url,
                reference_type="historical_recording_url",
                recording_id=recording_id or None,
                recording_type="voicemail",
                duration_ms=_duration_ms(row.get("duration")),
                start_time=row.get("date") or row.get("date_started"),
            )
        )
    provider_transcript_text = str(row.get("transcription_text") or "").strip() or None
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
            provider_transcript_text=provider_transcript_text,
            recording_details=recording_details,
            extra={
                "voicemail_link": recording_url or None,
                "voicemail_recording_id": recording_id or None,
            },
        ),
        "provider_transcript_text": provider_transcript_text,
        "sms_text": None,
        "recording_references": recording_refs,
    }
    job = {
        "job_uuid": str(uuid4()),
        "job_type": "ARCHIVE_EXPORT",
        "event_uuid": event_uuid,
        "payload": payload,
    }
    return await ae.handle_archive_export(job)


async def _archive_call_row(
    row: Dict[str, Any],
    fallback_day: date,
    office_id: str,
    recording_rows: List[Dict[str, Any]],
    include_call_transcripts: bool,
) -> Dict[str, Any]:
    external_id = _row_external_id("call", row)
    event_uuid = _deterministic_event_uuid("call", external_id)
    occurred_at = _occurred_at_for_row("call", row, fallback_day)
    source_provenance = _source_provenance(row, "call", external_id, office_id)
    recording_details: List[Dict[str, Any]] = []
    for item in recording_rows:
        detail = _recording_detail_from_row(item)
        if detail is not None:
            recording_details.append(detail)
    recording_refs = []
    for detail in recording_details:
        recording_refs.append(
            _recording_reference(
                event_uuid=event_uuid,
                source_provenance=source_provenance,
                url=str(detail.get("url") or ""),
                reference_type="historical_recording_url",
                recording_id=str(detail.get("id") or "").strip() or None,
                recording_type=str(detail.get("recording_type") or "").strip() or "admincallrecording",
                duration_ms=_duration_ms(detail.get("duration")),
                start_time=detail.get("start_time"),
            )
        )
    provider_transcript_text = None
    if include_call_transcripts and not str(row.get("category") or "").strip().lower().startswith("missed"):
        try:
            provider_transcript_text = await _fetch_call_transcript(external_id)
        except Exception:
            provider_transcript_text = None
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
            provider_transcript_text=provider_transcript_text,
            recording_details=recording_details,
            extra={
                "was_recorded": _boolish(row.get("was_recorded")),
            },
        ),
        "provider_transcript_text": provider_transcript_text,
        "sms_text": None,
        "recording_references": recording_refs,
    }
    job = {
        "job_uuid": str(uuid4()),
        "job_type": "ARCHIVE_EXPORT",
        "event_uuid": event_uuid,
        "payload": payload,
    }
    return await ae.handle_archive_export(job)


async def _fetch_rows_for_chunk(
    *,
    stat_type: str,
    chunk_start: date,
    chunk_end: date,
    target_candidates: List[tuple[str, str]],
) -> List[Dict[str, str]]:
    chunk_rows: List[Dict[str, str]] = []
    last_error: Optional[Exception] = None
    for candidate_type, candidate_id in target_candidates:
        try:
            chunk_rows = await _fetch_csv_rows(
                stat_type=stat_type,
                target_type=candidate_type,
                target_id=candidate_id,
                date_start=chunk_start.isoformat(),
                date_end=chunk_end.isoformat(),
            )
            print(
                f"[DIALPAD_BACKFILL] {stat_type} chunk {chunk_start.isoformat()} -> {chunk_end.isoformat()} "
                f"used target {candidate_type}:{candidate_id}"
            )
            break
        except httpx.HTTPStatusError as exc:
            last_error = exc
            status = exc.response.status_code if exc.response is not None else "unknown"
            print(
                f"[DIALPAD_BACKFILL] {stat_type} target {candidate_type}:{candidate_id} failed "
                f"with HTTP {status}; trying next target"
            )
            continue
    if not chunk_rows and last_error is not None:
        raise last_error

    filtered_rows, dropped_rows = _filter_rows_to_chunk(stat_type, chunk_rows, chunk_start, chunk_end)
    print(
        f"[DIALPAD_BACKFILL] {stat_type} {chunk_start.isoformat()} -> {chunk_end.isoformat()} : "
        f"{len(chunk_rows)} raw rows, {len(filtered_rows)} in-window, {dropped_rows} out-of-window"
    )
    return _sort_rows_for_archive(stat_type, filtered_rows)


async def run_backfill(args: argparse.Namespace) -> Dict[str, Any]:
    office_id = args.office_id or os.environ.get("DIALPAD_OFFICE_ID", DEFAULT_OFFICE_ID).strip()
    target_candidates = _stats_target_candidates(args.target_type, args.target_id, office_id)
    start_day = _parse_date(args.start_date)
    end_day = _parse_date(args.end_date) if args.end_date else datetime.now(ae._archive_timezone()).date()
    chunk_days = max(1, args.chunk_days)
    progress_every = max(1, args.progress_every)
    requested_types = {item.lower() for item in args.types}

    if not (ae.DIALPAD_ACCESS_TOKEN or ae.DIALPAD_REFRESH_TOKEN):
        raise SystemExit("Dialpad OAuth credentials are not configured in the local worker environment.")

    summary: Dict[str, Any] = {
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "target_type": target_candidates[0][0],
        "target_id": target_candidates[0][1],
        "target_candidates": [
            {"target_type": candidate_type, "target_id": candidate_id}
            for candidate_type, candidate_id in target_candidates
        ],
        "chunk_days": chunk_days,
        "progress_every": progress_every,
        "counts": {},
    }

    chunks = list(_iter_date_chunks(start_day, end_day, chunk_days))
    seen_keys: Dict[str, set[str]] = {
        "calls": set(),
        "texts": set(),
        "voicemails": set(),
        "recordings": set(),
    }
    recordings_by_call_id: Dict[str, List[Dict[str, str]]] = defaultdict(list)
    counts = {
        "calls_rows": 0,
        "texts_rows": 0,
        "voicemails_rows": 0,
        "recordings_rows": 0,
        "calls_archived": 0,
        "texts_archived": 0,
        "voicemails_archived": 0,
    }

    for chunk_index, (chunk_start, chunk_end) in enumerate(chunks, start=1):
        print(
            f"[DIALPAD_BACKFILL] chunk {chunk_index}/{len(chunks)} "
            f"{chunk_start.isoformat()} -> {chunk_end.isoformat()} starting"
        )

        if "texts" in requested_types:
            text_rows = await _fetch_rows_for_chunk(
                stat_type="texts",
                chunk_start=chunk_start,
                chunk_end=chunk_end,
                target_candidates=target_candidates,
            )
            new_text_rows: List[Dict[str, str]] = []
            duplicate_texts = 0
            for row in text_rows:
                key = _row_unique_key("texts", row)
                if key in seen_keys["texts"]:
                    duplicate_texts += 1
                    continue
                seen_keys["texts"].add(key)
                new_text_rows.append(row)
            counts["texts_rows"] += len(new_text_rows)
            print(
                f"[DIALPAD_BACKFILL] texts chunk {chunk_index}/{len(chunks)} retained "
                f"{len(new_text_rows)} new rows, skipped {duplicate_texts} duplicates "
                f"({counts['texts_rows']} unique cumulative)"
            )
            for row_index, row in enumerate(new_text_rows, start=1):
                await _archive_sms_row(row, chunk_start, office_id)
                counts["texts_archived"] += 1
                if _should_log_archive_progress(row_index, len(new_text_rows), progress_every):
                    print(
                        f"[DIALPAD_BACKFILL] archive texts chunk {chunk_index}/{len(chunks)} "
                        f"{row_index}/{len(new_text_rows)} complete "
                        f"({counts['texts_archived']} cumulative archived)"
                    )

        if "voicemails" in requested_types:
            voicemail_rows = await _fetch_rows_for_chunk(
                stat_type="voicemails",
                chunk_start=chunk_start,
                chunk_end=chunk_end,
                target_candidates=target_candidates,
            )
            new_voicemail_rows: List[Dict[str, str]] = []
            duplicate_voicemails = 0
            for row in voicemail_rows:
                key = _row_unique_key("voicemails", row)
                if key in seen_keys["voicemails"]:
                    duplicate_voicemails += 1
                    continue
                seen_keys["voicemails"].add(key)
                new_voicemail_rows.append(row)
            counts["voicemails_rows"] += len(new_voicemail_rows)
            print(
                f"[DIALPAD_BACKFILL] voicemails chunk {chunk_index}/{len(chunks)} retained "
                f"{len(new_voicemail_rows)} new rows, skipped {duplicate_voicemails} duplicates "
                f"({counts['voicemails_rows']} unique cumulative)"
            )
            for row_index, row in enumerate(new_voicemail_rows, start=1):
                await _archive_voicemail_row(row, chunk_start, office_id)
                counts["voicemails_archived"] += 1
                if _should_log_archive_progress(row_index, len(new_voicemail_rows), progress_every):
                    print(
                        f"[DIALPAD_BACKFILL] archive voicemails chunk {chunk_index}/{len(chunks)} "
                        f"{row_index}/{len(new_voicemail_rows)} complete "
                        f"({counts['voicemails_archived']} cumulative archived)"
                    )

        if "calls" in requested_types:
            recording_rows = await _fetch_rows_for_chunk(
                stat_type="recordings",
                chunk_start=chunk_start,
                chunk_end=chunk_end,
                target_candidates=target_candidates,
            )
            new_recording_rows: List[Dict[str, str]] = []
            duplicate_recordings = 0
            for row in recording_rows:
                key = _row_unique_key("recordings", row)
                if key in seen_keys["recordings"]:
                    duplicate_recordings += 1
                    continue
                seen_keys["recordings"].add(key)
                new_recording_rows.append(row)
            counts["recordings_rows"] += len(new_recording_rows)
            for row in new_recording_rows:
                call_id = str(row.get("call_id") or row.get("id") or "").strip()
                if call_id:
                    recordings_by_call_id[call_id].append(row)
            print(
                f"[DIALPAD_BACKFILL] recordings chunk {chunk_index}/{len(chunks)} retained "
                f"{len(new_recording_rows)} new rows, skipped {duplicate_recordings} duplicates "
                f"({counts['recordings_rows']} unique cumulative)"
            )

            call_rows = await _fetch_rows_for_chunk(
                stat_type="calls",
                chunk_start=chunk_start,
                chunk_end=chunk_end,
                target_candidates=target_candidates,
            )
            new_call_rows: List[Dict[str, str]] = []
            duplicate_calls = 0
            for row in call_rows:
                key = _row_unique_key("calls", row)
                if key in seen_keys["calls"]:
                    duplicate_calls += 1
                    continue
                seen_keys["calls"].add(key)
                new_call_rows.append(row)
            counts["calls_rows"] += len(new_call_rows)
            print(
                f"[DIALPAD_BACKFILL] calls chunk {chunk_index}/{len(chunks)} retained "
                f"{len(new_call_rows)} new rows, skipped {duplicate_calls} duplicates "
                f"({counts['calls_rows']} unique cumulative)"
            )
            for row_index, row in enumerate(new_call_rows, start=1):
                call_id = str(row.get("call_id") or row.get("id") or "").strip()
                await _archive_call_row(
                    row,
                    chunk_start,
                    office_id,
                    recordings_by_call_id.get(call_id, []),
                    include_call_transcripts=not args.skip_call_transcripts,
                )
                counts["calls_archived"] += 1
                if _should_log_archive_progress(row_index, len(new_call_rows), progress_every):
                    print(
                        f"[DIALPAD_BACKFILL] archive calls chunk {chunk_index}/{len(chunks)} "
                        f"{row_index}/{len(new_call_rows)} complete "
                        f"({counts['calls_archived']} cumulative archived)"
                    )

        print(
            f"[DIALPAD_BACKFILL] chunk {chunk_index}/{len(chunks)} complete: "
            f"calls={counts['calls_archived']} texts={counts['texts_archived']} "
            f"voicemails={counts['voicemails_archived']} recordings={counts['recordings_rows']}"
        )

    summary["counts"] = counts
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill historical Dialpad files into the TALOS archive.")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE, help="Inclusive YYYY-MM-DD start date.")
    parser.add_argument("--end-date", default="", help="Inclusive YYYY-MM-DD end date. Defaults to today.")
    parser.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS, help="Stats export chunk size in days.")
    parser.add_argument(
        "--types",
        nargs="+",
        default=list(DEFAULT_TYPES),
        choices=["calls", "texts", "voicemails"],
        help="Historical record types to fetch.",
    )
    parser.add_argument("--target-type", default="", help="Dialpad stats target type (company, office, user).")
    parser.add_argument("--target-id", default="", help="Dialpad stats target id.")
    parser.add_argument("--office-id", default="", help="Office id used in archived source provenance.")
    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        help="Print archive progress every N items within each chunk.",
    )
    parser.add_argument(
        "--skip-call-transcripts",
        action="store_true",
        help="Skip best-effort GET /transcripts/{call_id} lookups for call AI transcripts.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    summary = asyncio.run(run_backfill(args))
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

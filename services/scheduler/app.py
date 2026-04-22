"""TALOS Scheduler — baseline fallback + dispatch + live-to-deferred."""
from __future__ import annotations
import asyncio, json, logging, os, time
from typing import Optional, Tuple
import asyncpg, httpx
from fastapi import FastAPI

LOG = logging.getLogger("talos.scheduler")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
DATABASE_URL = os.environ["DATABASE_URL"]
LOCAL_WORKER_URL = os.environ.get("LOCAL_WORKER_URL","http://techton:8081")
LOCAL_WORKER_TOKEN = os.environ.get("LOCAL_WORKER_TOKEN","")
WORKER_PING_INTERVAL_SEC = int(os.environ.get("WORKER_PING_INTERVAL_SEC","30"))
DISPATCH_BATCH_SIZE = int(os.environ.get("DISPATCH_BATCH_SIZE","10"))
app = FastAPI(title="talos-scheduler", version="0.1.0")
_pool: Optional[asyncpg.Pool] = None
_worker = {"online":False,"last_seen":None,"last_error":None}

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

@app.on_event("startup")
async def _s():
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    asyncio.create_task(_ping_loop())
    asyncio.create_task(_dispatch_loop())

@app.on_event("shutdown")
async def _d():
    if _pool is not None: await _pool.close()

@app.get("/healthz")
async def healthz(): return {"ok":True,"service":"scheduler","worker":_worker}

async def _ping() -> Tuple[bool, Optional[str]]:
    url = f"{LOCAL_WORKER_URL.rstrip('/')}/healthz"
    h = {"Authorization":f"Bearer {LOCAL_WORKER_TOKEN}"} if LOCAL_WORKER_TOKEN else {}
    try:
        async with httpx.AsyncClient(timeout=5.0) as c:
            r = await c.get(url, headers=h)
            return (200<=r.status_code<300), None
    except Exception as e: return False, str(e)

async def _ping_loop():
    while True:
        was = _worker["online"]
        online, err = await _ping()
        _worker["online"] = online; _worker["last_error"] = err
        if online: _worker["last_seen"] = time.time()
        if was and not online:
            await _emit("WORKER_OFFLINE", {"local_worker_url":LOCAL_WORKER_URL,"error":err})
        await asyncio.sleep(WORKER_PING_INTERVAL_SEC)

async def decide_baseline_for_event(contact_uuid: Optional[str]) -> dict:
    if contact_uuid is None:
        return {"mode":"default_model","provisional":True,"baseline_uuid":None,
                "queued_backfill":False,"queued_refresh":False}
    assert _pool is not None
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT baseline_uuid, state FROM contact_baselines "
            "WHERE contact_uuid=$1 ORDER BY version DESC LIMIT 1",
            contact_uuid)
        if row is None or row["state"] == "NONE":
            q = await _ensure_backfill(conn, contact_uuid)
            return {"mode":"default_model","provisional":True,"baseline_uuid":None,
                    "queued_backfill":q,"queued_refresh":False}
        st = row["state"]; bu = str(row["baseline_uuid"])
        if st == "READY":
            return {"mode":"contact_baseline","provisional":False,"baseline_uuid":bu,
                    "queued_backfill":False,"queued_refresh":False}
        if st == "PARTIAL":
            return {"mode":"partial_baseline","provisional":True,"baseline_uuid":bu,
                    "queued_backfill":False,"queued_refresh":False}
        if st in ("QUEUED","PROCESSING"):
            return {"mode":"default_model","provisional":True,"baseline_uuid":None,
                    "queued_backfill":False,"queued_refresh":False}
        if st == "STALE":
            qr = await _ensure_refresh(conn, contact_uuid, bu)
            return {"mode":"contact_baseline","provisional":False,"baseline_uuid":bu,
                    "queued_backfill":False,"queued_refresh":qr}
    return {"mode":"default_model","provisional":True,"baseline_uuid":None,
            "queued_backfill":False,"queued_refresh":False}

async def _ensure_backfill(conn, contact_uuid: str) -> bool:
    key = f"baseline_backfill:{contact_uuid}"
    r = await conn.fetchrow(
        "INSERT INTO processing_jobs (job_type,state,contact_uuid,payload,idempotency_key) "
        "VALUES ('CONTACT_HISTORY_BACKFILL','QUEUED',$1,'{}'::jsonb,$2) "
        "ON CONFLICT (idempotency_key) DO NOTHING RETURNING job_uuid",
        contact_uuid, key)
    if r is not None:
        await _emit_conn(conn,"BASELINE_BACKFILL_QUEUED",{"contact_uuid":contact_uuid,"idempotency_key":key})
        await _emit_conn(conn,"BASELINE_MISSING",{"contact_uuid":contact_uuid})
        return True
    return False

async def _ensure_refresh(conn, contact_uuid: str, baseline_uuid: str) -> bool:
    key = f"baseline_refresh:{contact_uuid}:{baseline_uuid}"
    r = await conn.fetchrow(
        "INSERT INTO processing_jobs (job_type,state,contact_uuid,payload,idempotency_key) "
        "VALUES ('BASELINE_REFRESH','QUEUED',$1,$2::jsonb,$3) "
        "ON CONFLICT (idempotency_key) DO NOTHING RETURNING job_uuid",
        contact_uuid,
        json.dumps({"baseline_uuid":baseline_uuid}),
        key)
    return r is not None

async def _dispatch_loop():
    while True:
        try: await _tick()
        except Exception: LOG.exception("tick failed")
        await asyncio.sleep(5)

async def _tick():
    if _pool is None: return
    async with _pool.acquire() as conn:
        jobs = await conn.fetch(
            "SELECT job_uuid,job_type,contact_uuid,event_uuid,payload "
            "FROM processing_jobs WHERE state='QUEUED' "
            "ORDER BY priority DESC, created_at ASC LIMIT $1", DISPATCH_BATCH_SIZE)
        if not jobs: return
        if not _worker["online"]:
            for j in jobs:
                await conn.execute("UPDATE processing_jobs SET state='DEFERRED' WHERE job_uuid=$1", j["job_uuid"])
                await _emit_conn(conn,"LIVE_TO_DEFERRED_FALLBACK",{
                    "job_uuid":str(j["job_uuid"]),"job_type":j["job_type"],
                    "contact_uuid":str(j["contact_uuid"]) if j["contact_uuid"] else None,
                    "event_uuid":str(j["event_uuid"]) if j["event_uuid"] else None})
            return
        async with httpx.AsyncClient(timeout=10.0) as c:
            for j in jobs:
                payload = j["payload"]
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except json.JSONDecodeError:
                        LOG.warning("invalid json payload for job %s", j["job_uuid"])
                        payload = {}
                body = {"job_uuid":str(j["job_uuid"]),"job_type":j["job_type"],
                        "contact_uuid":str(j["contact_uuid"]) if j["contact_uuid"] else None,
                        "event_uuid":str(j["event_uuid"]) if j["event_uuid"] else None,
                        "payload":payload}
                h = {"Authorization":f"Bearer {LOCAL_WORKER_TOKEN}"} if LOCAL_WORKER_TOKEN else {}
                try:
                    r = await c.post(f"{LOCAL_WORKER_URL.rstrip('/')}/dispatch", json=body, headers=h)
                    if 200<=r.status_code<300:
                        await conn.execute(
                            "UPDATE processing_jobs SET state='DISPATCHED', dispatched_at=now() WHERE job_uuid=$1",
                            j["job_uuid"])
                    else:
                        LOG.warning("dispatch rejected %s: status=%s body=%s",
                                    j["job_uuid"], r.status_code, r.text)
                except Exception as e:
                    LOG.warning("dispatch failed %s: %s", j["job_uuid"], e)

async def _ensure_outbox(conn): await conn.execute(_OUTBOX_DDL)

async def _emit(event_code: str, payload: dict):
    if _pool is None: return
    async with _pool.acquire() as conn:
        await _ensure_outbox(conn)
        await _emit_conn(conn, event_code, payload)

async def _emit_conn(conn, event_code: str, payload: dict):
    await _ensure_outbox(conn)
    await conn.execute("INSERT INTO bridge_outbox (event_code,payload) VALUES ($1,$2::jsonb)",
                       event_code, json.dumps(payload))

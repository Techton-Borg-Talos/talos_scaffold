"""TALOS Local Worker — Phase 1 scaffold. Single combined service."""
from __future__ import annotations
import logging, os
from typing import Any, Awaitable, Callable, Dict, Optional
from fastapi import FastAPI, Header, HTTPException
import httpx
from pydantic import BaseModel

LOG = logging.getLogger("talos.worker")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
LOCAL_WORKER_TOKEN = os.environ.get("LOCAL_WORKER_TOKEN","")
PRODUCT_WRITEBACK_URL = os.environ.get("PRODUCT_WRITEBACK_URL","").strip()
PRODUCT_WRITEBACK_TOKEN = os.environ.get("PRODUCT_WRITEBACK_TOKEN","").strip()
WORKER_ID = os.environ.get("WORKER_ID", os.environ.get("HOSTNAME", "talos_worker"))
ENABLED_MODULES = {m.strip() for m in os.environ.get("ENABLED_MODULES","").split(",") if m.strip()}
app = FastAPI(title="talos-worker", version="0.1.0")

Handler = Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]
_handlers: Dict[str, tuple[str, Handler]] = {}

def register(job_type: str, module_name: str):
    def _wrap(fn: Handler) -> Handler:
        if job_type in _handlers:
            raise RuntimeError(f"duplicate handler for job_type={job_type}")
        _handlers[job_type] = (module_name, fn)
        return fn
    return _wrap

async def _writeback(job_uuid: str, state: str, result: Optional[Dict[str, Any]] = None,
                     error: Optional[str] = None) -> None:
    if not PRODUCT_WRITEBACK_URL:
        return
    headers = {}
    if PRODUCT_WRITEBACK_TOKEN:
        headers["Authorization"] = f"Bearer {PRODUCT_WRITEBACK_TOKEN}"
    body = {
        "job_uuid": job_uuid,
        "state": state,
        "result": result or {},
        "error": error,
        "worker_id": WORKER_ID,
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(PRODUCT_WRITEBACK_URL, json=body, headers=headers)
            if not (200 <= r.status_code < 300):
                LOG.warning("writeback rejected %s: status=%s body=%s", job_uuid, r.status_code, r.text)
    except Exception as e:
        LOG.warning("writeback failed %s: %s", job_uuid, e)

async def _dispatch(job: Dict[str, Any]) -> Dict[str, Any]:
    jt = job.get("job_type","")
    job_uuid = str(job.get("job_uuid") or "")
    p = _handlers.get(jt)
    if p is None:
        result = {"status":"unhandled","job_type":jt}
        if job_uuid:
            await _writeback(job_uuid, "FAILED", result=result, error="unhandled job type")
        return result
    mn, fn = p
    if mn not in ENABLED_MODULES:
        result = {"status":"skipped","reason":"module_disabled","module":mn}
        if job_uuid:
            await _writeback(job_uuid, "FAILED", result=result, error="module disabled")
        return result
    if job_uuid:
        await _writeback(job_uuid, "PROCESSING", result={"job_type": jt, "module": mn})
    try:
        r = await fn(job)
        result = {"status":"ok","module":mn,"result":r}
        if job_uuid:
            await _writeback(job_uuid, "SUCCEEDED", result=result)
        return result
    except Exception as e:
        LOG.exception("handler failed for %s", jt)
        result = {"status":"error","module":mn,"error":str(e)}
        if job_uuid:
            await _writeback(job_uuid, "FAILED", result=result, error=str(e))
        return result

def _require_token(auth: Optional[str]) -> None:
    if not LOCAL_WORKER_TOKEN: return
    if not auth or not auth.startswith("Bearer "):
        raise HTTPException(401,"missing bearer token")
    if auth[len("Bearer "):].strip() != LOCAL_WORKER_TOKEN:
        raise HTTPException(401,"bad token")

class DispatchRequest(BaseModel):
    job_uuid: str
    job_type: str
    contact_uuid: Optional[str] = None
    event_uuid: Optional[str] = None
    payload: Dict[str, Any] = {}

@app.get("/healthz")
async def healthz():
    return {"ok":True,"service":"worker",
            "enabled_modules":sorted(ENABLED_MODULES),
            "registered_job_types":sorted(_handlers.keys())}

@app.post("/dispatch")
async def dispatch(req: DispatchRequest,
                   authorization: Optional[str] = Header(default=None)):
    _require_token(authorization)
    return await _dispatch(req.model_dump())

from modules import (  # noqa: E402,F401
    transcription, behavior_analysis, contact_engine_local,
    baseline_updates, deferred_backfill, popup_live_assist,
)

LOG.info("worker ready: enabled=%s registered=%s",
         sorted(ENABLED_MODULES), sorted(_handlers.keys()))

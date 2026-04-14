"""TALOS Local Worker — Phase 1 scaffold. Single combined service."""
from __future__ import annotations
import logging, os
from typing import Any, Awaitable, Callable, Dict, Optional
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

LOG = logging.getLogger("talos.worker")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
LOCAL_WORKER_TOKEN = os.environ.get("LOCAL_WORKER_TOKEN","")
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

async def _dispatch(job: Dict[str, Any]) -> Dict[str, Any]:
    jt = job.get("job_type","")
    p = _handlers.get(jt)
    if p is None: return {"status":"unhandled","job_type":jt}
    mn, fn = p
    if mn not in ENABLED_MODULES:
        return {"status":"skipped","reason":"module_disabled","module":mn}
    try:
        r = await fn(job)
        return {"status":"ok","module":mn,"result":r}
    except Exception as e:
        LOG.exception("handler failed for %s", jt)
        return {"status":"error","module":mn,"error":str(e)}

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

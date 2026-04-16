"""TALOS Intake API (scaffold). Archive-first (Rule 1)."""
from __future__ import annotations
import hashlib, hmac, logging, os, uuid
from pathlib import Path
from typing import Dict, Optional
from fastapi import FastAPI, Header, HTTPException, Request

LOG = logging.getLogger("talos.intake")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
RAW = Path(os.environ.get("RAW_ARTIFACT_DIR","/var/lib/talos/raw"))
RAW.mkdir(parents=True, exist_ok=True)
DIALPAD_SECRET = os.environ.get("DIALPAD_WEBHOOK_SECRET", "")
app = FastAPI(title="talos-intake", version="0.1.0")

@app.get("/healthz")
async def healthz(): return {"ok":True,"service":"intake_api"}

def _archive(source: str, raw: bytes, content_type: str) -> Dict:
    au = uuid.uuid4()
    sha = hashlib.sha256(raw).hexdigest()
    sd = RAW / source / sha[:2]; sd.mkdir(parents=True, exist_ok=True)
    p = sd / f"{au}.bin"; p.write_bytes(raw)
    return {"artifact_uuid":str(au),"storage_scheme":"local",
            "storage_uri":str(p),"sha256":sha,"byte_size":len(raw),
            "content_type":content_type}

def _verify_dialpad(raw: bytes, sig: Optional[str]) -> bool:
    if not DIALPAD_SECRET or not sig: return False
    exp = hmac.new(DIALPAD_SECRET.encode(), raw, hashlib.sha256).hexdigest()
    return hmac.compare_digest(exp, sig.strip())

@app.post("/webhooks/dialpad")
async def dialpad(request: Request,
                  x_dialpad_signature: Optional[str] = Header(default=None)):
    raw = await request.body()
    """if not _verify_dialpad(raw, x_dialpad_signature):
        raise HTTPException(401, "bad signature")
    """
    LOG.info("dialpad headers: %s", dict(request.headers))
    LOG.info("dialpad raw body: %s", raw.decode(errors="replace"))

    if DIALPAD_SECRET and x_dialpad_signature:
       if not _verify_dialpad(raw, x_dialpad_signature):
           LOG.warning("dialpad signature failed")


    ptr = _archive("dialpad", raw,
                   request.headers.get("content-type","application/json"))
    # TODO(phase1): insert communication_events + processing_jobs.
    LOG.info("dialpad archived: %s", ptr["artifact_uuid"])
    return {"accepted":True,"artifact_uuid":ptr["artifact_uuid"]}

@app.post("/ingest/gmail")
async def gmail(request: Request):
    raw = await request.body()
    ptr = _archive("gmail", raw,
                   request.headers.get("content-type","application/json"))
    LOG.info("gmail archived: %s", ptr["artifact_uuid"])
    return {"accepted":True,"artifact_uuid":ptr["artifact_uuid"]}

import json as _json

@app.post("/webhooks/dialpad_debug")
async def dialpad_debug(request: Request):
    raw = await request.body()
    LOG.warning("DIALPAD DEBUG headers=%s", dict(request.headers))
    LOG.warning("DIALPAD DEBUG body[:500]=%r", raw[:500])
    return {"ok": True, "len": len(raw)}

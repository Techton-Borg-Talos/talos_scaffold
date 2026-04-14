"""TALOS Contact Engine (backbone) — exact match, candidates, promotion."""
from __future__ import annotations
import json, logging, os
from typing import Optional
from uuid import UUID
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

LOG = logging.getLogger("talos.contact_engine")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
DATABASE_URL = os.environ["DATABASE_URL"]
THRESHOLD = int(os.environ.get("CANDIDATE_PROMOTION_THRESHOLD","3"))
app = FastAPI(title="talos-contact-engine", version="0.1.0")
_pool: Optional[asyncpg.Pool] = None

_OUTBOX_DDL = """
CREATE TABLE IF NOT EXISTS bridge_outbox (
    outbox_uuid   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_code    TEXT NOT NULL,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at  TIMESTAMPTZ,
    delivery_error TEXT
);
"""

@app.on_event("startup")
async def _s():
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with _pool.acquire() as conn: await conn.execute(_OUTBOX_DDL)

@app.on_event("shutdown")
async def _d():
    if _pool is not None: await _pool.close()

@app.get("/healthz")
async def healthz(): return {"ok":True,"service":"contact_engine","threshold":THRESHOLD}

class ResolveReq(BaseModel):
    identity_kind: str = Field(..., examples=["phone_e164","email"])
    identity_value: str
    event_uuid: Optional[UUID] = None
    source_system: Optional[str] = None

class ResolveResp(BaseModel):
    contact_uuid: Optional[UUID] = None
    candidate_uuid: Optional[UUID] = None
    sightings_count: Optional[int] = None
    status: Optional[str] = None
    promotion_proposed: bool = False

class PromoteReq(BaseModel):
    candidate_uuid: UUID
    display_name: str
    given_name: Optional[str] = None
    family_name: Optional[str] = None
    organization: Optional[str] = None
    decided_by: str = "aor"

class MergeReq(BaseModel):
    kept_contact_uuid: UUID
    absorbed_contact_uuid: UUID
    decided_by: str = "aor"
    rationale: Optional[str] = None

class RejectReq(BaseModel):
    candidate_uuid: UUID
    decided_by: str = "aor"
    rationale: Optional[str] = None

@app.post("/resolve_or_track", response_model=ResolveResp)
async def resolve_or_track(req: ResolveReq) -> ResolveResp:
    assert _pool is not None
    async with _pool.acquire() as conn:
        async with conn.transaction():
            c = await conn.fetchrow(
                "SELECT c.contact_uuid FROM contact_identities i "
                "JOIN contacts c ON c.contact_uuid=i.contact_uuid "
                "WHERE i.identity_kind=$1 AND i.identity_value=$2 AND c.deleted_at IS NULL LIMIT 1",
                req.identity_kind, req.identity_value)
            if c:
                await conn.execute(
                    "UPDATE contact_identities SET last_seen_at=now() "
                    "WHERE identity_kind=$1 AND identity_value=$2",
                    req.identity_kind, req.identity_value)
                return ResolveResp(contact_uuid=c["contact_uuid"])
            cand = await conn.fetchrow(
                "INSERT INTO contact_candidates "
                "(primary_identity_kind,primary_identity_value,sightings_count,score) "
                "VALUES ($1,$2,1,1) "
                "ON CONFLICT (primary_identity_kind,primary_identity_value) DO UPDATE SET "
                "sightings_count=contact_candidates.sightings_count+1, "
                "score=contact_candidates.score+1, last_seen_at=now(), updated_at=now() "
                "RETURNING candidate_uuid,sightings_count,status",
                req.identity_kind, req.identity_value)
            cu = cand["candidate_uuid"]; sc = cand["sightings_count"]; st = cand["status"]
            await conn.execute(
                "INSERT INTO candidate_observations "
                "(candidate_uuid,event_uuid,identity_kind,identity_value,source_system) "
                "VALUES ($1,$2,$3,$4,$5::source_system)",
                cu, req.event_uuid, req.identity_kind, req.identity_value, req.source_system)
            proposed = False
            if st == "open" and sc >= THRESHOLD:
                await conn.execute(
                    "UPDATE contact_candidates SET status='proposed',proposed_at=now() "
                    "WHERE candidate_uuid=$1 AND status='open'", cu)
                await conn.execute(
                    "INSERT INTO bridge_outbox (event_code,payload) VALUES ($1,$2::jsonb)",
                    "CONTACT_MATCH_AMBIGUOUS",
                    json.dumps({"candidate_uuid":str(cu),"identity_kind":req.identity_kind,
                                "identity_value":req.identity_value,"sightings_count":sc}))
                proposed = True; st = "proposed"
            return ResolveResp(candidate_uuid=cu, sightings_count=sc, status=st,
                               promotion_proposed=proposed)

@app.post("/promote")
async def promote(req: PromoteReq) -> dict:
    assert _pool is not None
    async with _pool.acquire() as conn:
        async with conn.transaction():
            cand = await conn.fetchrow(
                "SELECT candidate_uuid,primary_identity_kind,primary_identity_value,status "
                "FROM contact_candidates WHERE candidate_uuid=$1", req.candidate_uuid)
            if cand is None: raise HTTPException(404,"candidate not found")
            if cand["status"] in ("promoted","merged","rejected"):
                raise HTTPException(409, f"already {cand['status']}")
            new = await conn.fetchrow(
                "INSERT INTO contacts (display_name,given_name,family_name,organization) "
                "VALUES ($1,$2,$3,$4) RETURNING contact_uuid",
                req.display_name, req.given_name, req.family_name, req.organization)
            cu = new["contact_uuid"]
            if cand["primary_identity_kind"] and cand["primary_identity_value"]:
                await conn.execute(
                    "INSERT INTO contact_identities (contact_uuid,identity_kind,identity_value,is_primary,verified) "
                    "VALUES ($1,$2,$3,TRUE,FALSE) ON CONFLICT (identity_kind,identity_value) DO NOTHING",
                    cu, cand["primary_identity_kind"], cand["primary_identity_value"])
            await conn.execute(
                "UPDATE contact_candidates SET status='promoted',promoted_contact_uuid=$1,resolved_at=now() "
                "WHERE candidate_uuid=$2", cu, req.candidate_uuid)
            if cand["primary_identity_kind"] and cand["primary_identity_value"]:
                await conn.execute(
                    "UPDATE communication_events SET contact_uuid=$1 "
                    "WHERE contact_uuid IS NULL AND remote_identifier=$2",
                    cu, cand["primary_identity_value"])
            return {"contact_uuid":str(cu),"status":"promoted"}

@app.post("/merge")
async def merge(req: MergeReq) -> dict:
    assert _pool is not None
    if req.kept_contact_uuid == req.absorbed_contact_uuid:
        raise HTTPException(400,"kept and absorbed must differ")
    async with _pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE contact_identities SET contact_uuid=$1 WHERE contact_uuid=$2",
                               req.kept_contact_uuid, req.absorbed_contact_uuid)
            await conn.execute("UPDATE communication_events SET contact_uuid=$1 WHERE contact_uuid=$2",
                               req.kept_contact_uuid, req.absorbed_contact_uuid)
            await conn.execute("UPDATE contacts SET deleted_at=now() WHERE contact_uuid=$1",
                               req.absorbed_contact_uuid)
            await conn.execute(
                "INSERT INTO contact_merges (kept_contact_uuid,absorbed_contact_uuid,decided_by,rationale) "
                "VALUES ($1,$2,$3,$4)",
                req.kept_contact_uuid, req.absorbed_contact_uuid, req.decided_by, req.rationale)
            return {"ok":True}

@app.post("/reject")
async def reject(req: RejectReq) -> dict:
    assert _pool is not None
    async with _pool.acquire() as conn:
        r = await conn.execute(
            "UPDATE contact_candidates SET status='rejected',resolved_at=now(),"
            "metadata=metadata||jsonb_build_object('rejected_by',$2::text,'rationale',$3::text) "
            "WHERE candidate_uuid=$1 AND status IN ('open','proposed')",
            req.candidate_uuid, req.decided_by, req.rationale)
        if r.endswith(" 0"):
            raise HTTPException(404,"candidate not found or already resolved")
        return {"ok":True}

@app.get("/candidates")
async def list_cands(status: str = "proposed", limit: int = 50) -> dict:
    assert _pool is not None
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT candidate_uuid,working_label,primary_identity_kind,"
            "primary_identity_value,sightings_count,score,status,first_seen_at,last_seen_at "
            "FROM contact_candidates WHERE status=$1 ORDER BY last_seen_at DESC LIMIT $2",
            status, limit)
        return {"candidates":[dict(r) for r in rows]}

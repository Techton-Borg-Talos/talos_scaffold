"""TALOS Matrix Bridge — thin relay only.

Validates allow-list + required fields, routes to Matrix rooms per config,
writes audit rows into talos_aor.aor_bridge_audit. No reasoning. No
product-state mutation beyond bridge_outbox delivered_at/delivery_error.

TODO(phase-1.1) — bridge_outbox retry / poison-row controls:
  The current bridge_outbox table has no attempt_count, last_attempt_at,
  or terminal_error columns. That means:
    * A row with a permanent `missing_field:X` error will sit in the
      outbox forever and be re-audited on every drain pass.
    * A row whose Matrix delivery transiently fails is retried on the
      next tick indefinitely; there is no backoff and no cap.
    * There is no surface for operators to mark a row terminally dead
      without deleting it.
  Future work (deliberately deferred out of Phase 1): extend bridge_outbox
  with:
    - attempt_count   INT  NOT NULL DEFAULT 0
    - last_attempt_at TIMESTAMPTZ
    - terminal_error  BOOLEAN NOT NULL DEFAULT FALSE
  then:
    - increment attempt_count on every processing pass
    - set terminal_error=TRUE after N attempts OR immediately for
      structural errors (not_in_allow_list, missing_field:*, no_route)
    - skip terminal rows in the drain query
    - expose a small admin endpoint to clear or re-open terminal rows
  Do NOT implement this in Phase 1.
"""
from __future__ import annotations
import asyncio, json, logging, os
from pathlib import Path
from typing import Any, Dict, List, Optional
import asyncpg, httpx
from fastapi import FastAPI

LOG = logging.getLogger("talos.matrix_bridge")
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"))
DATABASE_URL = os.environ["DATABASE_URL"]
AOR_DATABASE_URL = os.environ.get("AOR_DATABASE_URL")
MATRIX_HOMESERVER_URL = os.environ.get("MATRIX_HOMESERVER_URL","")
MATRIX_BRIDGE_USER_TOKEN = os.environ.get("MATRIX_BRIDGE_USER_TOKEN","")
BRIDGE_RULES_PATH = os.environ.get("BRIDGE_RULES_PATH","/etc/talos/aor_bridge_rules.json")
BRIDGE_EVENTS_PATH = os.environ.get("BRIDGE_EVENTS_PATH","/etc/talos/aor_bridge_events.json")
BRIDGE_DRY_RUN = os.environ.get("BRIDGE_DRY_RUN","0") == "1"
POLL_INTERVAL_SEC = int(os.environ.get("POLL_INTERVAL_SEC","5"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE","20"))
app = FastAPI(title="talos-matrix-bridge", version="0.1.0")
_ppool: Optional[asyncpg.Pool] = None
_apool: Optional[asyncpg.Pool] = None
_events_cfg: Dict[str, Any] = {}
_rules_cfg: Dict[str, Any] = {}
_room_id_cache: Dict[str, str] = {}

def _load_cfg():
    global _events_cfg, _rules_cfg
    _events_cfg = json.loads(Path(BRIDGE_EVENTS_PATH).read_text(encoding="utf-8"))
    _rules_cfg = json.loads(Path(BRIDGE_RULES_PATH).read_text(encoding="utf-8"))

def _allowed(code: str) -> bool:
    return code in set(_events_cfg.get("allowed_events_product_to_aor", []))

def _missing_field(code: str, payload: Dict[str, Any]) -> Optional[str]:
    spec = _events_cfg.get("required_payload_fields", {}).get(code)
    if not spec: return None
    for f in spec:
        if f not in payload or payload[f] is None: return f
    return None

def _routes(code: str) -> List[Dict[str, Any]]:
    r = _rules_cfg.get("routes", {}); d = _rules_cfg.get("default_route")
    return r.get(code) or ([d] if d else [])

def _template(code: str, key: Optional[str]) -> str:
    t = _rules_cfg.get("templates", {})
    if key and key in t: return t[key]
    pe = _rules_cfg.get("event_templates", {})
    if code in pe: return pe[code]
    return _rules_cfg.get("default_template","[{event_code}] {payload_json}")

def _render(tpl: str, code: str, payload: Dict[str, Any]) -> str:
    safe = {"event_code":code,"payload_json":json.dumps(payload,sort_keys=True),
            **{f"p.{k}":v for k,v in payload.items()}}
    try: return tpl.format(**safe)
    except Exception:
        return _rules_cfg.get("default_template","[{event_code}] {payload_json}").format(
            event_code=code, payload_json=json.dumps(payload,sort_keys=True))

@app.on_event("startup")
async def _s():
    global _ppool, _apool
    _load_cfg()
    _ppool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=3)
    if AOR_DATABASE_URL:
        _apool = await asyncpg.create_pool(AOR_DATABASE_URL, min_size=1, max_size=3)
    asyncio.create_task(_loop())

@app.on_event("shutdown")
async def _d():
    if _ppool is not None: await _ppool.close()
    if _apool is not None: await _apool.close()

@app.get("/healthz")
async def healthz():
    return {"ok":True,"service":"matrix_bridge","dry_run":BRIDGE_DRY_RUN,
            "configured_events":len(_events_cfg.get("allowed_events_product_to_aor", []))}

@app.get("/stats")
async def stats():
    assert _ppool is not None
    async with _ppool.acquire() as c:
        p = await c.fetchval("SELECT count(*) FROM bridge_outbox WHERE delivered_at IS NULL")
        d = await c.fetchval("SELECT count(*) FROM bridge_outbox WHERE delivered_at IS NOT NULL")
    return {"pending":p,"delivered":d}

@app.post("/admin/flush_once")
async def flush_once():
    return {"processed": await _drain()}

async def _loop():
    while True:
        try: await _drain()
        except Exception: LOG.exception("drain failed")
        await asyncio.sleep(POLL_INTERVAL_SEC)

async def _drain() -> int:
    assert _ppool is not None
    async with _ppool.acquire() as c:
        rows = await c.fetch(
            "SELECT outbox_uuid,event_code,payload,created_at FROM bridge_outbox "
            "WHERE delivered_at IS NULL ORDER BY created_at ASC LIMIT $1", BATCH_SIZE)
    if not rows: return 0
    n = 0
    for r in rows:
        await _process(r); n += 1
    return n

async def _process(r):
    ou = r["outbox_uuid"]; code = r["event_code"]
    payload = r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"])
    if not _allowed(code):
        await _err(ou, f"event_code {code!r} not in allowed_events_product_to_aor")
        await _audit("product_to_aor", code, payload, False, "not_in_allow_list"); return
    miss = _missing_field(code, payload)
    if miss is not None:
        err = f"missing_field:{miss}"
        await _err(ou, err)
        await _audit("product_to_aor", code, payload, False, err); return
    rts = _routes(code)
    if not rts:
        await _err(ou, "no route configured")
        await _audit("product_to_aor", code, payload, False, "no_route"); return
    last: Optional[str] = None; delivered = False
    for rt in rts:
        ra = rt.get("room_alias"); tk = rt.get("template_key"); dl = rt.get("detail_level","normal")
        if not ra: last = "route missing room_alias"; continue
        body = _render(_template(code, tk), code, payload)
        if BRIDGE_DRY_RUN:
            LOG.info("[dry-run] %s (%s): %s", ra, dl, body[:500]); delivered = True; continue
        try:
            await _send(ra, body); delivered = True
        except Exception as e:
            last = f"send failed for {ra}: {e}"; LOG.warning(last)
    if delivered and last is None:
        await _ok(ou); await _audit("product_to_aor", code, payload, True, None)
    elif delivered and last:
        await _ok(ou); await _audit("product_to_aor", code, payload, True, f"partial: {last}")
    else:
        await _err(ou, last or "unknown"); await _audit("product_to_aor", code, payload, False, last)

async def _ok(ou):
    assert _ppool is not None
    async with _ppool.acquire() as c:
        await c.execute("UPDATE bridge_outbox SET delivered_at=now(), delivery_error=NULL WHERE outbox_uuid=$1", ou)

async def _err(ou, err: str):
    assert _ppool is not None
    async with _ppool.acquire() as c:
        await c.execute("UPDATE bridge_outbox SET delivery_error=$2 WHERE outbox_uuid=$1", ou, err)

async def _resolve(alias: str) -> str:
    if alias in _room_id_cache: return _room_id_cache[alias]
    if not MATRIX_HOMESERVER_URL or not MATRIX_BRIDGE_USER_TOKEN:
        raise RuntimeError("matrix not configured")
    import urllib.parse as _u
    q = _u.quote(alias, safe="")
    url = f"{MATRIX_HOMESERVER_URL.rstrip('/')}/_matrix/client/v3/directory/room/{q}"
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.get(url, headers={"Authorization":f"Bearer {MATRIX_BRIDGE_USER_TOKEN}"})
        r.raise_for_status(); rid = r.json()["room_id"]
    _room_id_cache[alias] = rid; return rid

async def _send(alias: str, body: str):
    if not MATRIX_HOMESERVER_URL or not MATRIX_BRIDGE_USER_TOKEN:
        raise RuntimeError("matrix not configured")
    rid = await _resolve(alias)
    import time, uuid
    txn = f"talos-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    url = f"{MATRIX_HOMESERVER_URL.rstrip('/')}/_matrix/client/v3/rooms/{rid}/send/m.room.message/{txn}"
    async with httpx.AsyncClient(timeout=15.0) as c:
        r = await c.put(url,
            headers={"Authorization":f"Bearer {MATRIX_BRIDGE_USER_TOKEN}"},
            json={"msgtype":"m.text","body":body})
        r.raise_for_status()

async def _audit(direction: str, code: str, payload: Dict[str, Any], *, delivered=False, error: Optional[str]=None):
    # tolerate default=False via keyword
    ...
async def _audit(direction, code, payload, delivered, error):
    if _apool is None:
        LOG.info("AUDIT [%s] %s delivered=%s error=%s", direction, code, delivered, error); return
    async with _apool.acquire() as c:
        await c.execute(
            "INSERT INTO aor_bridge_audit (direction,event_code,payload,delivered,delivered_at,error) "
            "VALUES ($1,$2,$3::jsonb,$4,CASE WHEN $4 THEN now() ELSE NULL END,$5)",
            direction, code, json.dumps(payload), delivered, error)

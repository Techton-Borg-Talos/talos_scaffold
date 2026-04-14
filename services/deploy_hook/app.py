"""TALOS Product Deploy Hook."""
from __future__ import annotations
import asyncio, hashlib, hmac, logging, os, subprocess, time
from collections import deque
from typing import Deque, Dict, List, Optional
import httpx
from fastapi import FastAPI, Header, HTTPException, Request

LOG = logging.getLogger("talos.deploy_hook")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

SECRET = os.environ.get("GITEA_WEBHOOK_SECRET", "")
REPO_DIR = os.environ.get("REPO_DIR", "/opt/talos/repo")
COMPOSE_FILE = os.environ.get("COMPOSE_FILE",
    "/opt/talos/repo/deploy/docker/aws/product/docker-compose.yml")
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT", "talos-product")
TRACKED_BRANCH = os.environ.get("TRACKED_BRANCH", "main")
HEALTHCHECK_URLS = [u.strip() for u in os.environ.get("HEALTHCHECK_URLS","").split(",") if u.strip()]
DEPLOY_LOG: Deque[Dict] = deque(maxlen=50)
_lock = asyncio.Lock()
app = FastAPI(title="talos-deploy-hook", version="0.1.0")

def _verify(body: bytes, sig: Optional[str]) -> bool:
    if not SECRET or not sig: return False
    exp = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(exp, sig.strip())

def _run(cmd: List[str], cwd: Optional[str]=None, timeout: int=600) -> Dict:
    LOG.info("exec: %s", " ".join(cmd))
    t0 = time.time()
    try:
        p = subprocess.run(cmd, cwd=cwd, check=False, capture_output=True,
                           text=True, timeout=timeout)
        return {"cmd":cmd,"rc":p.returncode,"stdout_tail":p.stdout[-2000:],
                "stderr_tail":p.stderr[-2000:],"duration_s":round(time.time()-t0,2)}
    except subprocess.TimeoutExpired as e:
        return {"cmd":cmd,"rc":-1,"stdout_tail":"",
                "stderr_tail":f"TIMEOUT {e.timeout}s",
                "duration_s":round(time.time()-t0,2)}

async def _health() -> Dict:
    ok = True; results = []
    async with httpx.AsyncClient(timeout=10.0) as c:
        for u in HEALTHCHECK_URLS:
            try:
                r = await c.get(u); h = 200<=r.status_code<300; ok = ok and h
                results.append({"url":u,"status":r.status_code,"healthy":h})
            except Exception as e:
                ok = False; results.append({"url":u,"error":str(e),"healthy":False})
    return {"ok":ok,"probes":results}

async def _deploy(trigger: Dict) -> Dict:
    async with _lock:
        e: Dict = {"started_at":time.time(),"trigger":trigger,"steps":[],"status":"running"}
        DEPLOY_LOG.appendleft(e)
        e["steps"].append(_run(["git","fetch","--all","--prune"], cwd=REPO_DIR))
        e["steps"].append(_run(["git","reset","--hard",f"origin/{TRACKED_BRANCH}"], cwd=REPO_DIR))
        cd = os.path.dirname(COMPOSE_FILE)
        e["steps"].append(_run(["docker","compose","--project-name",COMPOSE_PROJECT,
                                "-f",COMPOSE_FILE,"build"], cwd=cd, timeout=1800))
        e["steps"].append(_run(["docker","compose","--project-name",COMPOSE_PROJECT,
                                "-f",COMPOSE_FILE,"up","-d"], cwd=cd, timeout=600))
        e["health"] = await _health()
        fail = any(s.get("rc",0)!=0 for s in e["steps"])
        e["status"] = "failed" if (fail or not e["health"]["ok"]) else "ok"
        e["finished_at"] = time.time()
        e["duration_s"] = round(e["finished_at"]-e["started_at"],2)
        return e

@app.get("/healthz")
async def healthz(): return {"ok":True,"service":"deploy_hook"}

@app.get("/recent")
async def recent(): return {"deploys": list(DEPLOY_LOG)}

@app.post("/gitea")
async def gitea(request: Request,
                x_gitea_event: Optional[str] = Header(default=None),
                x_gitea_signature: Optional[str] = Header(default=None)):
    body = await request.body()
    if not _verify(body, x_gitea_signature):
        raise HTTPException(401, "bad signature")
    if x_gitea_event and x_gitea_event.lower() != "push":
        return {"accepted":False,"reason":f"ignored: {x_gitea_event}"}
    try: payload = await request.json()
    except Exception: payload = {}
    ref = payload.get("ref","")
    if ref and not ref.endswith(f"/{TRACKED_BRANCH}"):
        return {"accepted":False,"reason":f"ignored ref: {ref}"}
    trigger = {"event":x_gitea_event,"ref":ref,"after":payload.get("after"),
               "pusher":(payload.get("pusher") or {}).get("username"),
               "repo":(payload.get("repository") or {}).get("full_name")}
    asyncio.create_task(_deploy(trigger))
    return {"accepted":True,"trigger":trigger}

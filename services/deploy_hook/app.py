"""TALOS Product Deploy Hook."""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import subprocess
import time
from collections import deque
from typing import Deque, Dict, List, Optional

import httpx
from fastapi import FastAPI, Header, HTTPException, Request

LOG = logging.getLogger("talos.deploy_hook")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

SECRET = os.environ.get("GITEA_WEBHOOK_SECRET", "")
REPO_DIR = os.environ.get("REPO_DIR", "/opt/talos/repo")
COMPOSE_FILE = os.environ.get(
    "COMPOSE_FILE",
    "/opt/talos/repo/deploy/docker/aws/product/docker-compose.yml",
)
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT", "talos-product")
TRACKED_BRANCH = os.environ.get("TRACKED_BRANCH", "main")
HEALTHCHECK_URLS = [
    u.strip() for u in os.environ.get("HEALTHCHECK_URLS", "").split(",") if u.strip()
]

REQUIRED_SERVICES = [
    s.strip()
    for s in os.environ.get(
        "REQUIRED_SERVICES",
        "postgres_product,deploy_hook,intake_api,scheduler,contact_engine,matrix_bridge,caddy",
    ).split(",")
    if s.strip()
]

DEPLOY_LOG: Deque[Dict] = deque(maxlen=50)
_lock = asyncio.Lock()

app = FastAPI(title="talos-deploy-hook", version="0.2.0")


def _verify(body: bytes, sig: Optional[str]) -> bool:
    if not SECRET or not sig:
        return False
    exp = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(exp, sig.strip())


def _run(cmd: List[str], cwd: Optional[str] = None, timeout: int = 600) -> Dict:
    LOG.info("exec: %s", " ".join(cmd))
    started = time.time()
    try:
        proc = subprocess.run(
            cmd,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return {
            "cmd": cmd,
            "rc": proc.returncode,
            "stdout_tail": proc.stdout[-4000:],
            "stderr_tail": proc.stderr[-4000:],
            "duration_s": round(time.time() - started, 2),
        }
    except subprocess.TimeoutExpired as e:
        return {
            "cmd": cmd,
            "rc": -1,
            "stdout_tail": "",
            "stderr_tail": f"TIMEOUT {e.timeout}s",
            "duration_s": round(time.time() - started, 2),
        }


async def _health() -> Dict:
    if not HEALTHCHECK_URLS:
        return {"ok": True, "probes": []}

    ok = True
    results = []
    async with httpx.AsyncClient(timeout=10.0) as client:
        for url in HEALTHCHECK_URLS:
            try:
                resp = await client.get(url)
                healthy = 200 <= resp.status_code < 300
                ok = ok and healthy
                results.append(
                    {"url": url, "status": resp.status_code, "healthy": healthy}
                )
            except Exception as e:
                ok = False
                results.append({"url": url, "error": str(e), "healthy": False})
    return {"ok": ok, "probes": results}


def _compose_cmd(*args: str) -> List[str]:
    return [
        "docker",
        "compose",
        "--project-name",
        COMPOSE_PROJECT,
        "-f",
        COMPOSE_FILE,
        *args,
    ]


def _parse_ps_output(raw: str) -> List[Dict]:
    lines = [line for line in raw.splitlines() if line.strip()]
    if len(lines) <= 1:
        return []

    services: List[Dict] = []
    for line in lines[1:]:
        parts = line.split()
        if len(parts) < 5:
            continue

        service = parts[3]
        status = " ".join(parts[4:])
        services.append(
            {
                "name": parts[0],
                "service": service,
                "status": status,
                "raw": line,
            }
        )
    return services


def _check_services(ps_rows: List[Dict]) -> Dict:
    by_service = {row["service"]: row for row in ps_rows}

    missing = [svc for svc in REQUIRED_SERVICES if svc not in by_service]
    unhealthy = []
    for svc in REQUIRED_SERVICES:
        row = by_service.get(svc)
        if not row:
            continue
        if not row["status"].startswith("Up"):
            unhealthy.append(
                {
                    "service": svc,
                    "status": row["status"],
                    "raw": row["raw"],
                }
            )

    ok = not missing and not unhealthy
    return {
        "ok": ok,
        "required_services": REQUIRED_SERVICES,
        "missing": missing,
        "unhealthy": unhealthy,
        "rows": ps_rows,
    }


async def _deploy(trigger: Dict) -> Dict:
    async with _lock:
        entry: Dict = {
            "started_at": time.time(),
            "trigger": trigger,
            "steps": [],
            "status": "running",
        }
        DEPLOY_LOG.appendleft(entry)

        compose_dir = os.path.dirname(COMPOSE_FILE)

        entry["steps"].append(
            _run(["git", "fetch", "--all", "--prune"], cwd=REPO_DIR, timeout=600)
        )
        entry["steps"].append(
            _run(
                ["git", "reset", "--hard", f"origin/{TRACKED_BRANCH}"],
                cwd=REPO_DIR,
                timeout=600,
            )
        )

        # One command that builds and starts the stack, matching the manual recovery path.
        entry["steps"].append(
            _run(
                _compose_cmd("up", "-d", "--build"),
                cwd=compose_dir,
                timeout=1800,
            )
        )

        # Give containers a moment to settle, then inspect status.
        await asyncio.sleep(5)

        ps_step = _run(_compose_cmd("ps"), cwd=compose_dir, timeout=120)
        entry["steps"].append(ps_step)
        ps_rows = _parse_ps_output(ps_step.get("stdout_tail", ""))
        entry["service_check"] = _check_services(ps_rows)

        entry["health"] = await _health()

        step_fail = any(step.get("rc", 0) != 0 for step in entry["steps"])
        service_fail = not entry["service_check"]["ok"]
        health_fail = not entry["health"]["ok"]

        entry["status"] = "failed" if (step_fail or service_fail or health_fail) else "ok"
        entry["finished_at"] = time.time()
        entry["duration_s"] = round(entry["finished_at"] - entry["started_at"], 2)

        if entry["status"] == "failed":
            LOG.error(
                "deploy failed: trigger=%s service_check=%s health=%s",
                trigger,
                entry["service_check"],
                entry["health"],
            )
        else:
            LOG.info("deploy ok: trigger=%s", trigger)

        return entry


@app.get("/healthz")
async def healthz():
    return {"ok": True, "service": "deploy_hook"}


@app.get("/recent")
async def recent():
    return {"deploys": list(DEPLOY_LOG)}


@app.post("/gitea")
async def gitea(
    request: Request,
    x_gitea_event: Optional[str] = Header(default=None),
    x_gitea_signature: Optional[str] = Header(default=None),
):
    body = await request.body()

    if not _verify(body, x_gitea_signature):
        raise HTTPException(401, "bad signature")

    if x_gitea_event and x_gitea_event.lower() != "push":
        return {"accepted": False, "reason": f"ignored: {x_gitea_event}"}

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    ref = payload.get("ref", "")
    if ref and not ref.endswith(f"/{TRACKED_BRANCH}"):
        return {"accepted": False, "reason": f"ignored ref: {ref}"}

    trigger = {
        "event": x_gitea_event,
        "ref": ref,
        "after": payload.get("after"),
        "pusher": (payload.get("pusher") or {}).get("username"),
        "repo": (payload.get("repository") or {}).get("full_name"),
    }

    asyncio.create_task(_deploy(trigger))
    return {"accepted": True, "trigger": trigger}
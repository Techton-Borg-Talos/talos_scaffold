"""TALOS Product Deploy Hook.

Receives Gitea push webhooks and redeploys the talos-product stack.
Lives in its own compose project (talos-deployer) so it never
self-terminates during a deploy.

Success criteria (v0.5.0):
  - Tier 1 (critical): git fetch, git reset, docker compose up -d --build
    Any nonzero rc here → deploy failed.
  - Tier 2 (authoritative): HTTP health probes with retries.
    All probes must pass → deploy failed if not.
  - Tier 3 (informational): docker compose ps inspection.
    Captured for /recent diagnostics. Never gates pass/fail.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SECRET = os.environ.get("GITEA_WEBHOOK_SECRET", "")
REPO_DIR = os.environ.get("REPO_DIR", "/opt/talos/repo")
COMPOSE_FILE = os.environ.get(
    "COMPOSE_FILE",
    "/opt/talos/repo/deploy/docker/aws/product/docker-compose.yml",
)
COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT", "talos-product")
TRACKED_BRANCH = os.environ.get("TRACKED_BRANCH", "main")

HEALTHCHECK_URLS = [
    u.strip()
    for u in os.environ.get("HEALTHCHECK_URLS", "").split(",")
    if u.strip()
]

REQUIRED_SERVICES = [
    s.strip()
    for s in os.environ.get(
        "REQUIRED_SERVICES",
        "postgres_product,intake_api,scheduler,contact_engine,matrix_bridge,caddy",
    ).split(",")
    if s.strip()
]

DEPLOY_SERVICES = [
    s.strip()
    for s in os.environ.get(
        "DEPLOY_SERVICES",
        "postgres_product,intake_api,scheduler,contact_engine,matrix_bridge,caddy",
    ).split(",")
    if s.strip()
]

# Health probe tuning
HEALTH_INITIAL_DELAY_S = int(os.environ.get("HEALTH_INITIAL_DELAY_S", "10"))
HEALTH_RETRY_COUNT = int(os.environ.get("HEALTH_RETRY_COUNT", "3"))
HEALTH_RETRY_DELAY_S = int(os.environ.get("HEALTH_RETRY_DELAY_S", "5"))
HEALTH_TIMEOUT_S = float(os.environ.get("HEALTH_TIMEOUT_S", "10"))

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
DEPLOY_LOG: Deque[Dict] = deque(maxlen=50)
_lock = asyncio.Lock()

app = FastAPI(title="talos-deploy-hook", version="0.5.0")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _verify(body: bytes, sig: Optional[str]) -> bool:
    if not SECRET or not sig:
        return False
    expected = hmac.new(SECRET.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig.strip())


def _run(
    cmd: List[str],
    cwd: Optional[str] = None,
    timeout: int = 600,
    label: Optional[str] = None,
) -> Dict:
    LOG.info("exec [%s]: %s", label or "step", " ".join(cmd))
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
            "label": label,
            "cmd": cmd,
            "rc": proc.returncode,
            "stdout_tail": proc.stdout[-12000:],
            "stderr_tail": proc.stderr[-12000:],
            "duration_s": round(time.time() - started, 2),
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "label": label,
            "cmd": cmd,
            "rc": -1,
            "stdout_tail": "",
            "stderr_tail": f"TIMEOUT {exc.timeout}s",
            "duration_s": round(time.time() - started, 2),
        }


def _compose_cmd(*args: str) -> List[str]:
    return [
        "docker", "compose",
        "--project-name", COMPOSE_PROJECT,
        "-f", COMPOSE_FILE,
        *args,
    ]


def _get_head_sha(repo_dir: str) -> Optional[str]:
    """Return the current HEAD commit SHA, or None on failure."""
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_dir,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if proc.returncode == 0:
            return proc.stdout.strip()
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Health probes (with retries)
# ---------------------------------------------------------------------------
async def _health_once() -> Dict:
    """Single pass of health probes."""
    if not HEALTHCHECK_URLS:
        return {"ok": True, "probes": []}

    all_ok = True
    results = []
    async with httpx.AsyncClient(timeout=HEALTH_TIMEOUT_S) as client:
        for url in HEALTHCHECK_URLS:
            try:
                resp = await client.get(url)
                healthy = 200 <= resp.status_code < 300
                all_ok = all_ok and healthy
                results.append(
                    {"url": url, "status": resp.status_code, "healthy": healthy}
                )
            except Exception as exc:
                all_ok = False
                results.append({"url": url, "error": str(exc), "healthy": False})
    return {"ok": all_ok, "probes": results}


async def _health_with_retries() -> Dict:
    """Probe health with initial delay and retries."""
    await asyncio.sleep(HEALTH_INITIAL_DELAY_S)

    last_result: Dict = {"ok": False, "probes": []}
    for attempt in range(1, HEALTH_RETRY_COUNT + 1):
        last_result = await _health_once()
        if last_result["ok"]:
            last_result["attempts"] = attempt
            return last_result
        if attempt < HEALTH_RETRY_COUNT:
            LOG.warning(
                "health probe attempt %d/%d failed, retrying in %ds",
                attempt, HEALTH_RETRY_COUNT, HEALTH_RETRY_DELAY_S,
            )
            await asyncio.sleep(HEALTH_RETRY_DELAY_S)

    last_result["attempts"] = HEALTH_RETRY_COUNT
    return last_result


# ---------------------------------------------------------------------------
# Service inspection (informational only — never gates pass/fail)
# ---------------------------------------------------------------------------
def _parse_ps_output(raw: str) -> List[Dict]:
    raw = raw.strip()
    if not raw:
        return []

    rows: List[Dict] = []

    # Try JSON array first
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    rows.append({
                        "name": item.get("Name"),
                        "service": item.get("Service"),
                        "status": str(item.get("State", "")).lower(),
                        "raw": item,
                    })
            return rows
    except json.JSONDecodeError:
        pass

    # Fall back to NDJSON (one object per line)
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            LOG.debug("skipping unparseable ps line: %r", line[:200])
            continue
        if isinstance(item, dict):
            rows.append({
                "name": item.get("Name"),
                "service": item.get("Service"),
                "status": str(item.get("State", "")).lower(),
                "raw": item,
            })

    return rows


def _check_services(ps_rows: List[Dict]) -> Dict:
    by_service = {row["service"]: row for row in ps_rows if row.get("service")}
    missing = [svc for svc in REQUIRED_SERVICES if svc not in by_service]
    unhealthy = []
    for svc in REQUIRED_SERVICES:
        row = by_service.get(svc)
        if row and row["status"] != "running":
            unhealthy.append({
                "service": svc,
                "status": row["status"],
                "raw": row["raw"],
            })

    return {
        "ok": not missing and not unhealthy,
        "required_services": REQUIRED_SERVICES,
        "missing": missing,
        "unhealthy": unhealthy,
        "rows": ps_rows,
    }


# ---------------------------------------------------------------------------
# Deploy execution
# ---------------------------------------------------------------------------
async def _deploy(trigger: Dict) -> Dict:
    async with _lock:
        entry: Dict = {
            "started_at": time.time(),
            "trigger": trigger,
            "critical_steps": [],
            "status": "running",
        }
        DEPLOY_LOG.appendleft(entry)

        compose_dir = os.path.dirname(COMPOSE_FILE)

        # ---- Tier 1: Critical steps ----
        step_fetch = _run(
            ["git", "fetch", "--all", "--prune"],
            cwd=REPO_DIR,
            timeout=600,
            label="git_fetch",
        )
        entry["critical_steps"].append(step_fetch)

        step_reset = _run(
            ["git", "reset", "--hard", f"origin/{TRACKED_BRANCH}"],
            cwd=REPO_DIR,
            timeout=600,
            label="git_reset",
        )
        entry["critical_steps"].append(step_reset)

        # Record deployed commit
        entry["commit_sha"] = _get_head_sha(REPO_DIR)

        step_up = _run(
            _compose_cmd("up", "-d", "--build", *DEPLOY_SERVICES),
            cwd=compose_dir,
            timeout=1800,
            label="compose_up",
        )
        entry["critical_steps"].append(step_up)

        critical_fail = any(
            step.get("rc", 0) != 0 for step in entry["critical_steps"]
        )

        # ---- Tier 2: Health probes with retries ----
        entry["health"] = await _health_with_retries()
        health_fail = not entry["health"]["ok"]

        # ---- Tier 3: Informational service inspection ----
        ps_step = _run(
            _compose_cmd("ps", "--format", "json"),
            cwd=compose_dir,
            timeout=120,
            label="compose_ps",
        )
        entry["ps_step"] = ps_step
        ps_rows = _parse_ps_output(ps_step.get("stdout_tail", ""))
        entry["service_check"] = _check_services(ps_rows)
        # NOTE: service_check does NOT influence pass/fail

        # ---- Final verdict ----
        entry["status"] = "failed" if (critical_fail or health_fail) else "ok"
        entry["finished_at"] = time.time()
        entry["duration_s"] = round(entry["finished_at"] - entry["started_at"], 2)

        if entry["status"] == "failed":
            failed_steps = [
                s["label"] for s in entry["critical_steps"] if s.get("rc", 0) != 0
            ]
            LOG.error(
                "deploy FAILED: commit=%s critical_failures=%s health_ok=%s trigger=%s",
                entry.get("commit_sha"),
                failed_steps or "none",
                entry["health"]["ok"],
                trigger,
            )
        else:
            LOG.info(
                "deploy OK: commit=%s duration=%.1fs trigger=%s",
                entry.get("commit_sha"),
                entry["duration_s"],
                trigger,
            )

        return entry


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
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

    # Fast-path: if a deploy is already running, skip rather than queue
    if _lock.locked():
        LOG.info("deploy already in progress, skipping webhook")
        return {"accepted": False, "reason": "deploy already in progress"}

    trigger = {
        "event": x_gitea_event,
        "ref": ref,
        "after": payload.get("after"),
        "pusher": (payload.get("pusher") or {}).get("username"),
        "repo": (payload.get("repository") or {}).get("full_name"),
    }

    asyncio.create_task(_deploy(trigger))
    return {"accepted": True, "trigger": trigger}

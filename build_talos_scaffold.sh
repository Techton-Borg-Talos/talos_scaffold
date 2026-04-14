#!/usr/bin/env bash
# Builds the TALOS Phase 1 scaffold in ./talos-scaffold
set -euo pipefail

ROOT="talos-scaffold"
rm -rf "$ROOT"
mkdir -p "$ROOT"
cd "$ROOT"

mkdir -p deploy/docker/aws/product/caddy deploy/docker/aws/product/scripts
mkdir -p deploy/docker/aws/aor/scripts
mkdir -p deploy/k8s/base deploy/k8s/overlays/dev deploy/k8s/overlays/prod deploy/k8s/argocd
mkdir -p services/deploy_hook services/intake_api services/scheduler services/contact_engine services/matrix_bridge
mkdir -p infra/sql infra/cloudflare infra/aws
mkdir -p docs config
mkdir -p worker/local/app/modules

# ===========================================================================
# README.md
# ===========================================================================
cat > README.md <<'TALOS_EOF'
# TALOS — Phase 1 Scaffold

**Product:** Universal Translator for the Neurodivergent
**Internal governance:** AoR (Architecture of Responsibility)

This repo hosts both the commercial Translator product and the internal AoR
governance system. They share a host in Phase 1 but are **separate compose
projects with separate databases**.

## Two stacks

| Stack | Purpose | Lives on | Public subdomains |
|-------|---------|----------|-------------------|
| Product / Patent | Intake, contact registry, events, dispatch, **plus a single host-wide Caddy** fronting all three public vhosts | EC2 (AWS) | `intake.taloscognitive.com` |
| AoR / Internal | Matrix/Synapse + Element + consultant rooms (fronted by the product-stack Caddy via a shared network) | EC2 (AWS) | `matrix.` / `chat.taloscognitive.com` |
| Local worker | Heavy AI: transcription, behavior baselines, contact engine | Windows laptop `techton` (Docker Desktop + Tailscale) | Tailscale-only |

The single-Caddy model means the product compose project owns the host's
`:80` and `:443` and reverse-proxies `intake.*`, `matrix.*`, and `chat.*`
by attaching to the AoR project's network. AoR and Product remain
separate compose projects with separate databases — only the single Caddy
crosses the network boundary to reach Synapse and Element.

## Hard rules

1. **Archive first** — every inbound event is received → verified → stored → normalized → queued **before** any live processing.
2. **AWS is not the heavy AI box** — heavy inference runs on the laptop.
3. **Matrix is not source of truth** — Postgres + artifact storage are.
4. **AoR and Translator stay separate** — same EC2 OK, same compose project NOT OK, same DB NOT OK.
5. **Product users interact with the Translator**, not AoR. AoR supervises.
6. **Never block the user waiting for baseline** — use default model + queue backfill.

## Bring-up order (strict)

1. EC2 base host + Docker + Tailscale
2. AoR Postgres init via `infra/sql/000_aor_databases.sql` + `002_init_aor.sql`
3. AoR stack up (`deploy/docker/aws/aor`) — creates `aor_net` the product Caddy attaches to
4. Product Postgres init via `infra/sql/001_init_product.sql` + `003_contact_registry.sql`
5. Product intake stack up (`deploy/docker/aws/product`) — single Caddy starts fronting all three vhosts
6. Deploy hook wired to Gitea webhook
7. Local worker over Tailscale (`worker/local`)

## What's intentionally deferred

Argo CD, Kubernetes, heavy AI on AWS, merged schemas, raw chain-of-thought
logging, wearable pipeline, full popup service, automated Matrix
bridge-user provisioning, fuzzy contact matching, AoR→product inbound
consumer, bridge_outbox retry/attempt/poison-row controls (see
`services/matrix_bridge/app.py` TODO).
TALOS_EOF

# ===========================================================================
# deploy/docker/aws/product/docker-compose.yml
# ===========================================================================
cat > deploy/docker/aws/product/docker-compose.yml <<'TALOS_EOF'
# TALOS Product Stack (Universal Translator)
#
# Compose project name: talos-product
# This stack is the commercializable product side. The `caddy` service here
# is the SINGLE host-wide Caddy that fronts all three public vhosts:
#   intake.taloscognitive.com -> this stack (intake_api + deploy_hook)
#   matrix.taloscognitive.com -> talos-aor synapse (via aor_net)
#   chat.taloscognitive.com   -> talos-aor element (via aor_net)
#
# Only the single Caddy crosses the network boundary to reach AoR.
# AoR and Product otherwise stay fully separate (different compose project,
# different databases).

name: talos-product

networks:
  product_net:
    driver: bridge
  aor_net:
    external: true
    name: talos-aor_aor_net

volumes:
  postgres_product_data:
  caddy_product_data:
  caddy_product_config:
  raw_artifacts:

services:
  postgres_product:
    image: postgres:16-alpine
    container_name: talos_postgres_product
    restart: unless-stopped
    environment:
      POSTGRES_DB: ${PRODUCT_DB_NAME:-talos_product}
      POSTGRES_USER: ${PRODUCT_DB_USER:-talos_product}
      POSTGRES_PASSWORD: ${PRODUCT_DB_PASSWORD}
    volumes:
      - postgres_product_data:/var/lib/postgresql/data
      - ../../../../infra/sql/001_init_product.sql:/docker-entrypoint-initdb.d/001_init_product.sql:ro
      - ../../../../infra/sql/003_contact_registry.sql:/docker-entrypoint-initdb.d/003_contact_registry.sql:ro
    networks: [product_net]
    ports:
      - "127.0.0.1:5432:5432"

  intake_api:
    build:
      context: ../../../../services/intake_api
    container_name: talos_intake_api
    restart: unless-stopped
    environment:
      DATABASE_URL: postgresql://${PRODUCT_DB_USER:-talos_product}:${PRODUCT_DB_PASSWORD}@postgres_product:5432/${PRODUCT_DB_NAME:-talos_product}
      RAW_ARTIFACT_DIR: /var/lib/talos/raw
      DIALPAD_WEBHOOK_SECRET: ${DIALPAD_WEBHOOK_SECRET}
      GMAIL_OAUTH_CLIENT_ID: ${GMAIL_OAUTH_CLIENT_ID}
      GMAIL_OAUTH_CLIENT_SECRET: ${GMAIL_OAUTH_CLIENT_SECRET}
      LOG_LEVEL: ${LOG_LEVEL:-INFO}
    volumes:
      - raw_artifacts:/var/lib/talos/raw
    depends_on:
      - postgres_product
    networks: [product_net]

  scheduler:
    build:
      context: ../../../../services/scheduler
    container_name: talos_scheduler
    restart: unless-stopped
    environment:
      DATABASE_URL: postgresql://${PRODUCT_DB_USER:-talos_product}:${PRODUCT_DB_PASSWORD}@postgres_product:5432/${PRODUCT_DB_NAME:-talos_product}
      LOCAL_WORKER_URL: ${LOCAL_WORKER_URL}
      LOCAL_WORKER_TOKEN: ${LOCAL_WORKER_TOKEN}
      WORKER_PING_INTERVAL_SEC: ${WORKER_PING_INTERVAL_SEC:-30}
    depends_on:
      - postgres_product
    networks: [product_net]

  contact_engine:
    build:
      context: ../../../../services/contact_engine
    container_name: talos_contact_engine
    restart: unless-stopped
    environment:
      DATABASE_URL: postgresql://${PRODUCT_DB_USER:-talos_product}:${PRODUCT_DB_PASSWORD}@postgres_product:5432/${PRODUCT_DB_NAME:-talos_product}
      CANDIDATE_PROMOTION_THRESHOLD: ${CANDIDATE_PROMOTION_THRESHOLD:-3}
    depends_on:
      - postgres_product
    networks: [product_net]

  matrix_bridge:
    build:
      context: ../../../../services/matrix_bridge
    container_name: talos_matrix_bridge
    restart: unless-stopped
    environment:
      DATABASE_URL: postgresql://${PRODUCT_DB_USER:-talos_product}:${PRODUCT_DB_PASSWORD}@postgres_product:5432/${PRODUCT_DB_NAME:-talos_product}
      AOR_DATABASE_URL: ${AOR_DATABASE_URL}
      MATRIX_HOMESERVER_URL: ${MATRIX_HOMESERVER_URL}
      MATRIX_BRIDGE_USER_TOKEN: ${MATRIX_BRIDGE_USER_TOKEN}
      BRIDGE_RULES_PATH: /etc/talos/aor_bridge_rules.json
      BRIDGE_EVENTS_PATH: /etc/talos/aor_bridge_events.json
      BRIDGE_DRY_RUN: ${BRIDGE_DRY_RUN:-0}
    volumes:
      - ../../../../config/aor_bridge_rules.example.json:/etc/talos/aor_bridge_rules.json:ro
      - ../../../../config/aor_bridge_events.example.json:/etc/talos/aor_bridge_events.json:ro
    depends_on:
      - postgres_product
    networks: [product_net, aor_net]

  deploy_hook:
    build:
      context: ../../../../services/deploy_hook
    container_name: talos_deploy_hook
    restart: unless-stopped
    environment:
      GITEA_WEBHOOK_SECRET: ${GITEA_WEBHOOK_SECRET}
      REPO_DIR: /opt/talos/repo
      COMPOSE_PROJECT: talos-product
      COMPOSE_FILE: /opt/talos/repo/deploy/docker/aws/product/docker-compose.yml
      HEALTHCHECK_URLS: ${HEALTHCHECK_URLS:-http://intake_api:8080/healthz}
    volumes:
      - ${REPO_HOST_DIR:-/opt/talos/repo}:/opt/talos/repo
      - /var/run/docker.sock:/var/run/docker.sock
    networks: [product_net]

  caddy:
    image: caddy:2-alpine
    container_name: talos_caddy_product
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./caddy/Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy_product_data:/data
      - caddy_product_config:/config
    depends_on:
      - intake_api
      - deploy_hook
    # Single-Caddy model: fronts intake.*, matrix.*, and chat.* by attaching
    # to both product_net and aor_net.
    networks: [product_net, aor_net]
TALOS_EOF

# ===========================================================================
# deploy/docker/aws/product/.env.example
# ===========================================================================
cat > deploy/docker/aws/product/.env.example <<'TALOS_EOF'
# TALOS Product stack env (AWS EC2)
# Copy to .env and fill in secrets. Do NOT commit .env.

PRODUCT_DB_NAME=talos_product
PRODUCT_DB_USER=talos_product
PRODUCT_DB_PASSWORD=CHANGE_ME_STRONG_PASSWORD

DIALPAD_WEBHOOK_SECRET=CHANGE_ME_DIALPAD_SECRET

GMAIL_OAUTH_CLIENT_ID=
GMAIL_OAUTH_CLIENT_SECRET=

# Tailscale hostname of the Windows laptop running the combined worker.
LOCAL_WORKER_URL=http://techton:8081
LOCAL_WORKER_TOKEN=CHANGE_ME_WORKER_TOKEN
WORKER_PING_INTERVAL_SEC=30

CANDIDATE_PROMOTION_THRESHOLD=3

MATRIX_HOMESERVER_URL=https://matrix.taloscognitive.com
MATRIX_BRIDGE_USER_TOKEN=CHANGE_ME_MATRIX_TOKEN
# AoR DB connection string for audit-row writes. Reached over aor_net by
# hostname `postgres_aor`. Port is the internal container port 5432.
AOR_DATABASE_URL=postgresql://synapse:CHANGE_ME_SYNAPSE_DB_PASSWORD@postgres_aor:5432/talos_aor
BRIDGE_DRY_RUN=0

GITEA_WEBHOOK_SECRET=CHANGE_ME_GITEA_SECRET
GITEA_REPO_URL=https://gitea.example.com/talos/talos.git
TRACKED_BRANCH=main
REPO_HOST_DIR=/opt/talos/repo
HEALTHCHECK_URLS=http://intake_api:8080/healthz

LOG_LEVEL=INFO
TALOS_EOF

# ===========================================================================
# deploy/docker/aws/product/caddy/Caddyfile
# ===========================================================================
cat > deploy/docker/aws/product/caddy/Caddyfile <<'TALOS_EOF'
# TALOS :: Single-Caddy model (Phase 1)
#
# One Caddy runs on the host's :80/:443 and fronts BOTH stacks by attaching
# to product_net AND aor_net.
#
# Public vhosts:
#   intake.taloscognitive.com  -> product intake API + deploy hook
#   matrix.taloscognitive.com  -> AoR Synapse
#   chat.taloscognitive.com    -> AoR Element Web

{
    email admin@taloscognitive.com
}

intake.taloscognitive.com {
    encode gzip
    handle_path /_deploy/* {
        reverse_proxy deploy_hook:8090
    }
    handle {
        reverse_proxy intake_api:8080
    }
    log {
        output stdout
        format console
    }
}

matrix.taloscognitive.com {
    encode gzip
    reverse_proxy synapse:8008
    log {
        output stdout
        format console
    }
}

chat.taloscognitive.com {
    encode gzip
    reverse_proxy element:80
    log {
        output stdout
        format console
    }
}
TALOS_EOF

# ===========================================================================
# deploy/docker/aws/product/scripts/bootstrap.sh
# ===========================================================================
cat > deploy/docker/aws/product/scripts/bootstrap.sh <<'TALOS_EOF'
#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
cd "$HERE"

if [[ ! -f .env ]]; then
  echo "ERROR: .env not found at $HERE/.env" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1091
. ./.env
set +a

REPO_HOST_DIR="${REPO_HOST_DIR:-/opt/talos/repo}"
TRACKED_BRANCH="${TRACKED_BRANCH:-main}"

if [[ ! -d "$REPO_HOST_DIR/.git" ]]; then
  if [[ -z "${GITEA_REPO_URL:-}" ]]; then
    echo "ERROR: $REPO_HOST_DIR is not a git checkout and GITEA_REPO_URL is not set." >&2
    exit 1
  fi
  echo "[talos-product] Cloning $GITEA_REPO_URL into $REPO_HOST_DIR ..."
  sudo mkdir -p "$(dirname "$REPO_HOST_DIR")"
  sudo chown "$USER":"$USER" "$(dirname "$REPO_HOST_DIR")"
  git clone --branch "$TRACKED_BRANCH" "$GITEA_REPO_URL" "$REPO_HOST_DIR"
else
  echo "[talos-product] Repo present; updating..."
  git -C "$REPO_HOST_DIR" fetch --all --prune
  git -C "$REPO_HOST_DIR" reset --hard "origin/$TRACKED_BRANCH"
fi

AOR_DIR="$REPO_HOST_DIR/deploy/docker/aws/aor"
if ! docker network inspect talos-aor_aor_net >/dev/null 2>&1; then
  echo "[talos-product] aor_net not found; bringing up talos-aor first..."
  if [[ ! -f "$AOR_DIR/.env" ]]; then
    echo "ERROR: $AOR_DIR/.env missing." >&2
    exit 1
  fi
  ( cd "$AOR_DIR" && docker compose --project-name talos-aor up -d )
fi

docker compose --project-name talos-product pull || true
docker compose --project-name talos-product build
docker compose --project-name talos-product up -d
docker compose --project-name talos-product ps
TALOS_EOF
chmod +x deploy/docker/aws/product/scripts/bootstrap.sh

# ===========================================================================
# deploy/docker/aws/aor/docker-compose.yml
# ===========================================================================
cat > deploy/docker/aws/aor/docker-compose.yml <<'TALOS_EOF'
# TALOS AoR / Internal Stack (single-Caddy model)
#
# Bring up BEFORE the product stack's caddy. Product Caddy attaches to
# talos-aor_aor_net as an external network.
#
# The Postgres container here holds two databases in one instance:
#   - synapse    : Matrix homeserver DB
#   - talos_aor  : AoR governance DB (policies, escalations, bridge audit)
# Both separate from talos_product.
#
# No Caddy in this stack; product Caddy reverse-proxies matrix.* and chat.*
# via aor_net.

name: talos-aor

networks:
  aor_net:
    driver: bridge

volumes:
  postgres_aor_data:
  synapse_data:

services:
  postgres_aor:
    image: postgres:16-alpine
    container_name: talos_postgres_aor
    restart: unless-stopped
    environment:
      POSTGRES_DB: ${SYNAPSE_DB_NAME:-synapse}
      POSTGRES_USER: ${SYNAPSE_DB_USER:-synapse}
      POSTGRES_PASSWORD: ${SYNAPSE_DB_PASSWORD}
      POSTGRES_INITDB_ARGS: "--encoding=UTF-8 --lc-collate=C --lc-ctype=C"
    volumes:
      - postgres_aor_data:/var/lib/postgresql/data
      - ../../../../infra/sql/000_aor_databases.sql:/docker-entrypoint-initdb.d/000_aor_databases.sql:ro
      - ../../../../infra/sql/002_init_aor.sql:/docker-entrypoint-initdb.d/002_init_aor.sql:ro
    networks: [aor_net]
    ports:
      - "127.0.0.1:5433:5432"

  synapse:
    image: matrixdotorg/synapse:latest
    container_name: talos_synapse
    restart: unless-stopped
    environment:
      SYNAPSE_SERVER_NAME: ${SYNAPSE_SERVER_NAME:-taloscognitive.com}
      SYNAPSE_REPORT_STATS: "no"
    volumes:
      - synapse_data:/data
    depends_on:
      - postgres_aor
    networks: [aor_net]

  element:
    image: vectorim/element-web:latest
    container_name: talos_element
    restart: unless-stopped
    volumes:
      - ./element-config.json:/app/config.json:ro
    networks: [aor_net]
TALOS_EOF

# ===========================================================================
# deploy/docker/aws/aor/.env.example
# ===========================================================================
cat > deploy/docker/aws/aor/.env.example <<'TALOS_EOF'
SYNAPSE_DB_NAME=synapse
SYNAPSE_DB_USER=synapse
SYNAPSE_DB_PASSWORD=CHANGE_ME_SYNAPSE_DB_PASSWORD
SYNAPSE_SERVER_NAME=taloscognitive.com
SYNAPSE_PUBLIC_BASEURL=https://matrix.taloscognitive.com
ELEMENT_DEFAULT_SERVER_URL=https://matrix.taloscognitive.com
TALOS_EOF

# ===========================================================================
# deploy/docker/aws/aor/element-config.json
# ===========================================================================
cat > deploy/docker/aws/aor/element-config.json <<'TALOS_EOF'
{
  "default_server_config": {
    "m.homeserver": {
      "base_url": "https://matrix.taloscognitive.com",
      "server_name": "taloscognitive.com"
    }
  },
  "brand": "TALOS AoR",
  "default_country_code": "US",
  "disable_guests": true,
  "disable_custom_urls": true,
  "features": {},
  "room_directory": {
    "servers": ["taloscognitive.com"]
  }
}
TALOS_EOF

# ===========================================================================
# deploy/docker/aws/aor/scripts/bootstrap.sh
# ===========================================================================
cat > deploy/docker/aws/aor/scripts/bootstrap.sh <<'TALOS_EOF'
#!/usr/bin/env bash
set -euo pipefail
HERE="$(cd "$(dirname "$0")/.." && pwd)"
cd "$HERE"
if [[ ! -f .env ]]; then
  echo "ERROR: .env not found at $HERE/.env" >&2
  exit 1
fi
docker compose --project-name talos-aor pull || true
docker compose --project-name talos-aor up -d
docker compose --project-name talos-aor ps
TALOS_EOF
chmod +x deploy/docker/aws/aor/scripts/bootstrap.sh

# ===========================================================================
# deploy/k8s placeholders
# ===========================================================================
cat > deploy/k8s/README.md <<'TALOS_EOF'
# TALOS — Kubernetes (placeholder)

**Phase 1 does not use Kubernetes.** This directory exists only to pin
the future layout so the repo structure stays GitOps-ready.

Current deployment path: Docker Compose on one EC2 host
(`deploy/docker/aws/product`, `deploy/docker/aws/aor`) plus the local
worker (`worker/local`). See `infra/aws/README.md`.

## Intended future layout

- base/        shared manifests for product + aor
- overlays/dev single-node / laptop cluster overlay
- overlays/prod production EKS (or self-hosted) overlay
- argocd/      Argo CD Application manifests (future)

Do not add real manifests in Phase 1.
TALOS_EOF

cat > deploy/k8s/base/kustomization.yaml <<'TALOS_EOF'
# PLACEHOLDER ONLY. Phase 1 uses Docker Compose.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: []
TALOS_EOF

cat > deploy/k8s/overlays/dev/kustomization.yaml <<'TALOS_EOF'
# PLACEHOLDER ONLY.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
TALOS_EOF

cat > deploy/k8s/overlays/prod/kustomization.yaml <<'TALOS_EOF'
# PLACEHOLDER ONLY.
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - ../../base
TALOS_EOF

cat > deploy/k8s/argocd/README.md <<'TALOS_EOF'
# Argo CD (placeholder)

Not used in Phase 1. The deploy path in Phase 1 is `services/deploy_hook`
responding to Gitea push webhooks. Do not populate until the k8s
migration starts (see `../README.md`).
TALOS_EOF

# ===========================================================================
# infra/sql/000_aor_databases.sql
# ===========================================================================
cat > infra/sql/000_aor_databases.sql <<'TALOS_EOF'
-- Bootstrap helper for the talos-aor Postgres container.
-- Runs BEFORE 002_init_aor.sql alphabetically.
-- The container's POSTGRES_DB creates `synapse`. This file adds `talos_aor`.
CREATE DATABASE talos_aor;
GRANT ALL PRIVILEGES ON DATABASE talos_aor TO CURRENT_USER;
TALOS_EOF

# ===========================================================================
# infra/sql/001_init_product.sql  (with corrected event_contact_links)
# ===========================================================================
cat > infra/sql/001_init_product.sql <<'TALOS_EOF'
-- =============================================================================
-- TALOS :: talos_product :: 001_init_product.sql
-- =============================================================================
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DO $$ BEGIN
    CREATE TYPE source_system AS ENUM (
        'dialpad','gmail','sms','google_voice','manual','other'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE event_channel AS ENUM (
        'email','sms','voicemail','call','meeting','chat','other'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE event_direction AS ENUM ('inbound','outbound','internal','unknown');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE job_state AS ENUM (
        'QUEUED','DISPATCHED','PROCESSING','DEFERRED','SUCCEEDED','FAILED','CANCELLED'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE baseline_state AS ENUM (
        'NONE','QUEUED','PROCESSING','PARTIAL','READY','STALE'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE model_provenance AS ENUM (
        'default_model','partial_baseline','contact_baseline','override'
    );
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    CREATE TYPE detail_level AS ENUM ('quiet','normal','verbose');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE TABLE IF NOT EXISTS communication_events (
    event_uuid          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system       source_system NOT NULL,
    channel             event_channel NOT NULL,
    direction           event_direction NOT NULL DEFAULT 'unknown',
    external_id         TEXT,
    remote_identifier   TEXT,
    local_identifier    TEXT,
    occurred_at         TIMESTAMPTZ NOT NULL,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    contact_uuid        UUID,
    subject             TEXT,
    source_metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,
    verified            BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_system, external_id)
);
CREATE INDEX IF NOT EXISTS idx_events_contact ON communication_events (contact_uuid);
CREATE INDEX IF NOT EXISTS idx_events_occurred_at ON communication_events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_remote_identifier ON communication_events (remote_identifier);
CREATE INDEX IF NOT EXISTS idx_events_unresolved
    ON communication_events (received_at DESC) WHERE contact_uuid IS NULL;

CREATE TABLE IF NOT EXISTS event_artifacts (
    artifact_uuid   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_uuid      UUID NOT NULL REFERENCES communication_events(event_uuid) ON DELETE CASCADE,
    kind            TEXT NOT NULL,
    storage_scheme  TEXT NOT NULL,
    storage_uri     TEXT NOT NULL,
    sha256          TEXT,
    byte_size       BIGINT,
    content_type    TEXT,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_artifacts_event ON event_artifacts (event_uuid);
CREATE INDEX IF NOT EXISTS idx_artifacts_sha ON event_artifacts (sha256);

-- event_contact_links: surrogate PK + partial unique indexes.
-- Postgres cannot use COALESCE-keyed uniqueness in a PRIMARY KEY.
CREATE TABLE IF NOT EXISTS event_contact_links (
    link_uuid       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_uuid      UUID NOT NULL REFERENCES communication_events(event_uuid) ON DELETE CASCADE,
    contact_uuid    UUID,
    candidate_uuid  UUID,
    role            TEXT NOT NULL,
    confidence      NUMERIC(4,3),
    resolver        TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT event_contact_links_target_present CHECK (
        contact_uuid IS NOT NULL OR candidate_uuid IS NOT NULL
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_event_contact_links_contact
    ON event_contact_links (event_uuid, role, contact_uuid)
    WHERE contact_uuid IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_event_contact_links_candidate
    ON event_contact_links (event_uuid, role, candidate_uuid)
    WHERE candidate_uuid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_event_contact_links_event
    ON event_contact_links (event_uuid);
CREATE INDEX IF NOT EXISTS idx_event_contact_links_contact
    ON event_contact_links (contact_uuid) WHERE contact_uuid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_event_contact_links_candidate
    ON event_contact_links (candidate_uuid) WHERE candidate_uuid IS NOT NULL;

CREATE TABLE IF NOT EXISTS processing_jobs (
    job_uuid        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type        TEXT NOT NULL,
    state           job_state NOT NULL DEFAULT 'QUEUED',
    priority        SMALLINT NOT NULL DEFAULT 100,
    event_uuid      UUID REFERENCES communication_events(event_uuid) ON DELETE SET NULL,
    contact_uuid    UUID,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    result          JSONB,
    idempotency_key TEXT UNIQUE,
    dispatched_at   TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_jobs_state_priority
    ON processing_jobs (state, priority DESC, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_jobs_contact
    ON processing_jobs (contact_uuid) WHERE contact_uuid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_event
    ON processing_jobs (event_uuid) WHERE event_uuid IS NOT NULL;

CREATE TABLE IF NOT EXISTS processing_runs (
    run_uuid        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_uuid        UUID NOT NULL REFERENCES processing_jobs(job_uuid) ON DELETE CASCADE,
    worker_id       TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    outcome         TEXT,
    logs_uri        TEXT,
    stats           JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_runs_job ON processing_runs (job_uuid);

CREATE TABLE IF NOT EXISTS transcripts (
    transcript_uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_uuid      UUID NOT NULL REFERENCES communication_events(event_uuid) ON DELETE CASCADE,
    engine          TEXT NOT NULL,
    language        TEXT,
    diarized        BOOLEAN NOT NULL DEFAULT FALSE,
    text            TEXT,
    confidence      NUMERIC(4,3),
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_transcripts_event ON transcripts (event_uuid);

CREATE TABLE IF NOT EXISTS utterances (
    utterance_uuid  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transcript_uuid UUID NOT NULL REFERENCES transcripts(transcript_uuid) ON DELETE CASCADE,
    sequence        INTEGER NOT NULL,
    start_ms        INTEGER,
    end_ms          INTEGER,
    speaker_label   TEXT,
    contact_uuid    UUID,
    text            TEXT NOT NULL,
    confidence      NUMERIC(4,3),
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_utterances_transcript ON utterances (transcript_uuid, sequence);
CREATE INDEX IF NOT EXISTS idx_utterances_contact ON utterances (contact_uuid);

CREATE TABLE IF NOT EXISTS behavior_features (
    feature_uuid    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contact_uuid    UUID,
    event_uuid      UUID REFERENCES communication_events(event_uuid) ON DELETE CASCADE,
    utterance_uuid  UUID REFERENCES utterances(utterance_uuid) ON DELETE CASCADE,
    feature_set     TEXT NOT NULL,
    features        JSONB NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_features_contact ON behavior_features (contact_uuid);
CREATE INDEX IF NOT EXISTS idx_features_event ON behavior_features (event_uuid);

CREATE TABLE IF NOT EXISTS contact_baselines (
    baseline_uuid       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contact_uuid        UUID NOT NULL,
    state               baseline_state NOT NULL DEFAULT 'NONE',
    version             INTEGER NOT NULL DEFAULT 1,
    events_observed     INTEGER NOT NULL DEFAULT 0,
    utterances_observed INTEGER NOT NULL DEFAULT 0,
    coverage_score      NUMERIC(4,3),
    payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
    stale_after         TIMESTAMPTZ,
    last_refresh_job    UUID REFERENCES processing_jobs(job_uuid) ON DELETE SET NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (contact_uuid, version)
);
CREATE INDEX IF NOT EXISTS idx_baselines_contact_state
    ON contact_baselines (contact_uuid, state);

CREATE TABLE IF NOT EXISTS advisory_outputs (
    advisory_uuid       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_uuid          UUID REFERENCES communication_events(event_uuid) ON DELETE SET NULL,
    contact_uuid        UUID,
    baseline_uuid       UUID REFERENCES contact_baselines(baseline_uuid) ON DELETE SET NULL,
    provenance          model_provenance NOT NULL,
    provisional         BOOLEAN NOT NULL DEFAULT FALSE,
    summary             TEXT NOT NULL,
    detail              TEXT,
    confidence          NUMERIC(4,3),
    risk_level          TEXT,
    signals             JSONB NOT NULL DEFAULT '{}'::jsonb,
    surfaced_at_level   detail_level NOT NULL DEFAULT 'normal',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_advisory_event ON advisory_outputs (event_uuid);
CREATE INDEX IF NOT EXISTS idx_advisory_contact ON advisory_outputs (contact_uuid);

CREATE TABLE IF NOT EXISTS delete_index (
    delete_uuid     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scope           TEXT NOT NULL,
    target_uuid     UUID,
    source_system   source_system,
    requested_by    TEXT NOT NULL,
    reason          TEXT,
    requested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'pending',
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_delete_status ON delete_index (status);

CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_events_touch ON communication_events;
CREATE TRIGGER trg_events_touch BEFORE UPDATE ON communication_events
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

DROP TRIGGER IF EXISTS trg_jobs_touch ON processing_jobs;
CREATE TRIGGER trg_jobs_touch BEFORE UPDATE ON processing_jobs
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

DROP TRIGGER IF EXISTS trg_baselines_touch ON contact_baselines;
CREATE TRIGGER trg_baselines_touch BEFORE UPDATE ON contact_baselines
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
TALOS_EOF

# ===========================================================================
# infra/sql/002_init_aor.sql
# ===========================================================================
cat > infra/sql/002_init_aor.sql <<'TALOS_EOF'
-- TALOS :: talos_aor :: 002_init_aor.sql
\connect talos_aor

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS aor_policies (
    policy_uuid     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_key      TEXT NOT NULL,
    policy_value    JSONB NOT NULL,
    scope           TEXT NOT NULL DEFAULT 'global',
    author          TEXT NOT NULL,
    effective_from  TIMESTAMPTZ NOT NULL DEFAULT now(),
    effective_to    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_policies_key_scope
    ON aor_policies (policy_key, scope, effective_from DESC);

CREATE TABLE IF NOT EXISTS aor_escalations (
    escalation_uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    subject_kind    TEXT NOT NULL,
    subject_uuid    UUID NOT NULL,
    reason_code     TEXT NOT NULL,
    severity        TEXT NOT NULL DEFAULT 'normal',
    opened_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at       TIMESTAMPTZ,
    decision        TEXT,
    decided_by      TEXT,
    notes           TEXT
);
CREATE INDEX IF NOT EXISTS idx_escalations_open
    ON aor_escalations (opened_at DESC) WHERE closed_at IS NULL;

CREATE TABLE IF NOT EXISTS aor_contact_reviews (
    review_uuid     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_uuid  UUID,
    contact_uuid    UUID,
    action          TEXT NOT NULL,
    rationale       TEXT,
    decided_by      TEXT NOT NULL,
    decided_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS aor_bridge_audit (
    audit_uuid      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    direction       TEXT NOT NULL,
    event_code      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    delivered       BOOLEAN NOT NULL DEFAULT FALSE,
    delivered_at    TIMESTAMPTZ,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_bridge_created ON aor_bridge_audit (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_bridge_code ON aor_bridge_audit (event_code);
TALOS_EOF

# ===========================================================================
# infra/sql/003_contact_registry.sql
# ===========================================================================
cat > infra/sql/003_contact_registry.sql <<'TALOS_EOF'
-- TALOS :: talos_product :: 003_contact_registry.sql
CREATE TABLE IF NOT EXISTS contacts (
    contact_uuid    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    display_name    TEXT NOT NULL,
    given_name      TEXT,
    family_name     TEXT,
    organization    TEXT,
    notes           TEXT,
    tags            TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    deleted_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_contacts_display_name ON contacts (display_name);
CREATE INDEX IF NOT EXISTS idx_contacts_tags ON contacts USING GIN (tags);

CREATE TABLE IF NOT EXISTS contact_identities (
    identity_uuid   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contact_uuid    UUID NOT NULL REFERENCES contacts(contact_uuid) ON DELETE CASCADE,
    identity_kind   TEXT NOT NULL,
    identity_value  TEXT NOT NULL,
    source_system   source_system,
    is_primary      BOOLEAN NOT NULL DEFAULT FALSE,
    verified        BOOLEAN NOT NULL DEFAULT FALSE,
    first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (identity_kind, identity_value)
);
CREATE INDEX IF NOT EXISTS idx_identities_contact ON contact_identities (contact_uuid);
CREATE INDEX IF NOT EXISTS idx_identities_value ON contact_identities (identity_kind, identity_value);

CREATE TABLE IF NOT EXISTS contact_candidates (
    candidate_uuid      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    working_label       TEXT,
    primary_identity_kind   TEXT,
    primary_identity_value  TEXT,
    sightings_count     INTEGER NOT NULL DEFAULT 0,
    score               NUMERIC(5,3) NOT NULL DEFAULT 0,
    status              TEXT NOT NULL DEFAULT 'open',
    promoted_contact_uuid   UUID REFERENCES contacts(contact_uuid) ON DELETE SET NULL,
    proposed_at         TIMESTAMPTZ,
    resolved_at         TIMESTAMPTZ,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (primary_identity_kind, primary_identity_value)
);
CREATE INDEX IF NOT EXISTS idx_candidates_status
    ON contact_candidates (status, last_seen_at DESC);

CREATE TABLE IF NOT EXISTS candidate_observations (
    observation_uuid    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    candidate_uuid      UUID NOT NULL REFERENCES contact_candidates(candidate_uuid) ON DELETE CASCADE,
    event_uuid          UUID REFERENCES communication_events(event_uuid) ON DELETE SET NULL,
    identity_kind       TEXT NOT NULL,
    identity_value      TEXT NOT NULL,
    source_system       source_system,
    observed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_cand_obs_candidate
    ON candidate_observations (candidate_uuid, observed_at DESC);
CREATE INDEX IF NOT EXISTS idx_cand_obs_identity
    ON candidate_observations (identity_kind, identity_value);

CREATE TABLE IF NOT EXISTS contact_merges (
    merge_uuid          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    kept_contact_uuid   UUID NOT NULL REFERENCES contacts(contact_uuid) ON DELETE CASCADE,
    absorbed_contact_uuid UUID NOT NULL,
    decided_by          TEXT NOT NULL,
    rationale           TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_contacts_touch ON contacts;
CREATE TRIGGER trg_contacts_touch BEFORE UPDATE ON contacts
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

DROP TRIGGER IF EXISTS trg_candidates_touch ON contact_candidates;
CREATE TRIGGER trg_candidates_touch BEFORE UPDATE ON contact_candidates
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
TALOS_EOF

echo "Wrote $(pwd) part 1 of scaffold (deploy + infra/sql + k8s + README)."
echo "Continuing with part 2 of the build script below..."
# ===========================================================================
# infra/cloudflare/README.md
# ===========================================================================
cat > infra/cloudflare/README.md <<'TALOS_EOF'
# Cloudflare — TALOS DNS setup

## 1. Records to create

All three records point at the same EC2 Elastic IP. Replace `203.0.113.10`
with your actual Elastic IP.

| Type | Name     | Content        | Proxy status | TTL  |
|------|----------|----------------|--------------|------|
| A    | intake   | 203.0.113.10   | DNS only     | Auto |
| A    | matrix   | 203.0.113.10   | DNS only     | Auto |
| A    | chat     | 203.0.113.10   | DNS only     | Auto |

## 2. Proxy guidance

Set DNS-only (grey cloud) for all three records in Phase 1:
- Matrix federation behaves poorly behind the CF proxy by default.
- Dialpad webhooks sign bodies; CF transformations can break signature verify.
- Gitea webhooks use HMAC over the raw body — same concern.

`chat.taloscognitive.com` can be orange-clouded if you want static caching.

## 3. TLS

Caddy handles Let's Encrypt via HTTP-01 on port 80. Nothing to configure in CF.

## 4. Tailscale is separate

MagicDNS names like `techton` and `aws-talos` never appear in Cloudflare.
The three records above are the complete public DNS surface.

## 5. Verification

    dig +short intake.taloscognitive.com
    dig +short matrix.taloscognitive.com
    dig +short chat.taloscognitive.com

After bootstrap:

    curl -I https://intake.taloscognitive.com/healthz
    curl -I https://matrix.taloscognitive.com/_matrix/client/versions
    curl -I https://chat.taloscognitive.com/
TALOS_EOF

# ===========================================================================
# infra/aws/README.md
# ===========================================================================
cat > infra/aws/README.md <<'TALOS_EOF'
# AWS — TALOS EC2 bootstrap

One EC2 instance. One Elastic IP. Both stacks and the single host-wide
Caddy run on the same host. Heavy AI stays on `techton` over Tailscale.

## 1. Pre-reqs
- AWS account; budget ~ $20/month.
- Tailscale account + auth key.
- Gitea on Synology with the repo.
- Cloudflare DNS records already set.

## 2. Provision EC2
- Ubuntu 24.04 LTS (arm64 preferred).
- `t4g.small` or `t3.small` (~2 GB RAM minimum).
- 30 GB gp3 root.
- Elastic IP allocated and associated.
- SSH key pair created (temporary; replaced by Tailscale SSH).

## 3. Security group (hard rule)

Exactly these inbound rules:

| Port | Proto | Source           | Purpose                              |
|------|-------|------------------|--------------------------------------|
| 80   | TCP   | 0.0.0.0/0        | Caddy HTTP + ACME                    |
| 443  | TCP   | 0.0.0.0/0        | Caddy HTTPS (intake/matrix/chat)     |
| 22   | TCP   | your current IP  | TEMPORARY; remove after Tailscale up |

Nothing else is public. Postgres is bound to 127.0.0.1. Internal services
are reached from Docker networks or via Tailscale. Admin SSH is Tailscale.

## 4. Host setup

    sudo apt-get update && sudo apt-get -y upgrade
    curl -fsSL https://get.docker.com | sudo sh
    sudo usermod -aG docker "$USER"
    newgrp docker
    sudo apt-get install -y git

## 5. Tailscale

    curl -fsSL https://tailscale.com/install.sh | sh
    sudo tailscale up --authkey=tskey-XXXX --hostname=aws-talos --ssh

Now delete the public port-22 rule from the security group. Admin is
`tailscale ssh ubuntu@aws-talos` from here on.

## 6. Clone repo and fill envs

    sudo mkdir -p /opt/talos
    sudo chown $USER:$USER /opt/talos
    git clone https://gitea.example.com/talos/talos.git /opt/talos/repo
    cd /opt/talos/repo
    cp deploy/docker/aws/product/.env.example deploy/docker/aws/product/.env
    cp deploy/docker/aws/aor/.env.example     deploy/docker/aws/aor/.env
    $EDITOR deploy/docker/aws/product/.env
    $EDITOR deploy/docker/aws/aor/.env

## 7. Bring-up order (strict)

    cd /opt/talos/repo

    # 7.1 One-time Synapse config generation
    docker volume create talos-aor_synapse_data
    docker run -it --rm \
        -v talos-aor_synapse_data:/data \
        -e SYNAPSE_SERVER_NAME=taloscognitive.com \
        -e SYNAPSE_REPORT_STATS=no \
        matrixdotorg/synapse:latest generate
    # Then edit homeserver.yaml inside the volume to point at postgres_aor,
    # set public_baseurl=https://matrix.taloscognitive.com, etc.

    # 7.2 AoR first (creates aor_net)
    bash deploy/docker/aws/aor/scripts/bootstrap.sh

    # 7.3 Product stack (single Caddy fronts all three vhosts)
    bash deploy/docker/aws/product/scripts/bootstrap.sh

    # 7.4 Verify
    curl -I https://intake.taloscognitive.com/healthz
    curl -I https://matrix.taloscognitive.com/_matrix/client/versions
    curl -I https://chat.taloscognitive.com/

## 8. Bridge user (manual, one-time)

    docker exec -it talos_synapse \
        register_new_matrix_user \
            -u talos-bridge -p <pw> \
            -c /data/homeserver.yaml --no-admin \
            http://localhost:8008

Log in via Element to get the access token; invite the bridge user to all
rooms listed in docs/aor_translator_boundary.md §7.1. Put the token into
`MATRIX_BRIDGE_USER_TOKEN`, set `BRIDGE_DRY_RUN=0`, restart matrix_bridge.

## 9. Local worker on techton

    cd worker\local
    copy .env.example .env
    # edit PRODUCT_DATABASE_URL + LOCAL_WORKER_TOKEN
    docker compose --project-name talos-worker up -d --build

## 10. Gitea webhook

URL: `https://intake.taloscognitive.com/_deploy/gitea`
Secret: value of `GITEA_WEBHOOK_SECRET` in product .env
Trigger: push, branch=main.

## 11. Admin rules
- SSH via Tailscale only.
- Internal services never public.
- intake/matrix/chat public via single Caddy.
- Secrets live in the three env files only.
TALOS_EOF

# ===========================================================================
# docs/aor_translator_boundary.md  (abridged but faithful)
# ===========================================================================
cat > docs/aor_translator_boundary.md <<'TALOS_EOF'
# AoR / Universal Translator Boundary

Governing artifact. Source of truth. Companion JSON:
`aor_translator_boundary.json`. If the two disagree, this markdown wins
and the JSON must be updated.

## 1. Definitions

**AoR (Architecture of Responsibility)** — internal governance + orchestration.
Surfaced through Matrix/Synapse. Not user-facing. Not marketed.

**Universal Translator** — the commercial, user-facing product. Ingests
historical + live comms, resolves contacts, builds contact-specific
behavioral baselines, falls back to default/partial/provisional when
history is unavailable, emits explicit assistive guidance.

## 2. Separation principles

1. Different identities. Translator public; AoR internal.
2. Different data stores. `talos_product` vs `talos_aor`. No cross-DB FKs.
3. Different compose projects. `talos-product` vs `talos-aor`.
4. Different channels. Translator → user; AoR → operators in Matrix.
5. Mediated bridge only (§4).
6. Matrix is not the system of record — Postgres + artifact storage are.

## 3. Baseline fallback rule (MANDATORY)

| State       | Behavior                                                      |
|-------------|---------------------------------------------------------------|
| NONE        | use default model NOW + queue CONTACT_HISTORY_BACKFILL        |
| QUEUED      | use default model NOW; do NOT duplicate backfill              |
| PROCESSING  | use default model NOW; do NOT duplicate backfill              |
| PARTIAL     | blended mode; output flagged provisional                      |
| READY       | use contact baseline; not provisional                         |
| STALE       | use existing baseline; queue one BASELINE_REFRESH             |

User is never blocked. Provisional/default output is clearly marked.

## 4. Allowed bridge events

Product → AoR:
PROCESSING_FAILURE, BASELINE_MISSING, BASELINE_BACKFILL_QUEUED,
CONTACT_MATCH_AMBIGUOUS, ADVISORY_HIGH_UNCERTAINTY, ADVISORY_HIGH_RISK,
WORKER_OFFLINE, LIVE_TO_DEFERRED_FALLBACK, DEPLOY_FAILURE,
SYSTEM_HEALTH_WARNING.

AoR → Product (consumer not yet built in Phase 1):
POLICY_UPDATE, THRESHOLD_UPDATE, DETAIL_LEVEL_UPDATE,
APPROVED_RESPONSE_TEMPLATE, ESCALATION_DECISION, CONTACT_REVIEW_DECISION,
FEATURE_FLAG_UPDATE.

## 5. Forbidden flows

- Raw deliberation → product.
- Raw chain-of-thought anywhere.
- Unrestricted consultant notes → product.
- Private/family Matrix content → product.
- Wholesale Matrix mirror → product.
- Raw artifacts (audio/email bodies/transcripts) → Matrix rooms.
- Product user messages routed to AoR as AoR input.
- matrix_bridge writing anywhere in talos_product other than
  bridge_outbox (delivered_at / delivery_error only).

## 6. Escalation

Escalation writes one row to bridge_outbox. Fire-and-forget. AoR
operators handle inline, open `aor_escalations`, or push an allow-listed
decision back.

## 7. Rooms on day one

- #aor-front-desk:taloscognitive.com
- #aor-deliberation:taloscognitive.com
- #consultant-council:taloscognitive.com
- #system-alerts:taloscognitive.com
- #system-verbose:taloscognitive.com
- #infrastructure:taloscognitive.com
- #consultant-rac:taloscognitive.com
- #consultant-mks:taloscognitive.com
- #consultant-hfc:taloscognitive.com

Detail levels: `quiet`, `normal`, `verbose`. Routing is config-only. No
code branching on event semantics in the bridge. Deliberation uses the
structured summary template (Task / Relevant facts / Assumptions /
Analysis / Risks / Recommendation / Confidence / Escalate). No raw
chain-of-thought logging.

## 8. Bridge-user provisioning (manual, Phase 1)

1. Bring up AoR.
2. Generate Synapse config.
3. `register_new_matrix_user -u talos-bridge ...`.
4. Log in once to get access token.
5. Invite to all §7 rooms.
6. Paste token into product `.env` `MATRIX_BRIDGE_USER_TOKEN`.
7. Restart `matrix_bridge`.

Until step 6, run with `BRIDGE_DRY_RUN=1`.

## 9. Future, out of scope for Phase 1

- AoR → product inbound consumer.
- Popup/live-assist channel.
- Wearable/glasses/multimodal delivery.
- Automated bridge-user lifecycle.
- Argo CD / GitOps / Kubernetes.
- Advanced contact engine on the worker.
- bridge_outbox retry/poison-row controls (see matrix_bridge TODO).

## 10. Change control

PR must update markdown AND JSON. Example configs must still validate.
Summary note in `#aor-deliberation`.
TALOS_EOF

# ===========================================================================
# docs/aor_translator_boundary.json
# ===========================================================================
cat > docs/aor_translator_boundary.json <<'TALOS_EOF'
{
  "$schema_version": "1.0.0",
  "source_of_truth_doc": "docs/aor_translator_boundary.md",
  "systems": {
    "translator": {
      "id": "universal_translator",
      "display_name": "Universal Translator for the Neurodivergent",
      "role": "commercializable_user_facing_product",
      "databases": ["talos_product"],
      "compose_project": "talos-product",
      "user_visibility": "public"
    },
    "aor": {
      "id": "aor",
      "display_name": "Architecture of Responsibility",
      "role": "internal_governance_and_orchestration",
      "databases": ["talos_aor"],
      "compose_project": "talos-aor",
      "user_visibility": "internal_only"
    }
  },
  "data_boundaries": {
    "cross_database_foreign_keys_allowed": false,
    "cross_reference_style": "opaque_uuid",
    "shared_host_allowed": true,
    "shared_compose_project_allowed": false,
    "shared_database_allowed": false
  },
  "allowed_events_product_to_aor": [
    "PROCESSING_FAILURE","BASELINE_MISSING","BASELINE_BACKFILL_QUEUED",
    "CONTACT_MATCH_AMBIGUOUS","ADVISORY_HIGH_UNCERTAINTY","ADVISORY_HIGH_RISK",
    "WORKER_OFFLINE","LIVE_TO_DEFERRED_FALLBACK","DEPLOY_FAILURE",
    "SYSTEM_HEALTH_WARNING"
  ],
  "allowed_events_aor_to_product": [
    "POLICY_UPDATE","THRESHOLD_UPDATE","DETAIL_LEVEL_UPDATE",
    "APPROVED_RESPONSE_TEMPLATE","ESCALATION_DECISION",
    "CONTACT_REVIEW_DECISION","FEATURE_FLAG_UPDATE"
  ],
  "forbidden_flows": [
    {"id":"no_raw_deliberation_to_product","description":"AoR internal deliberation logs must not reach the product."},
    {"id":"no_chain_of_thought","description":"Raw chain-of-thought logging is forbidden in either direction."},
    {"id":"no_consultant_notes_to_product","description":"Unrestricted consultant notes must not cross into the product."},
    {"id":"no_family_chat_to_product","description":"Unrelated private/family Matrix content must not reach the product."},
    {"id":"no_matrix_log_mirror_to_product","description":"Matrix rooms must not be mirrored wholesale into product tables."},
    {"id":"no_raw_artifacts_to_matrix","description":"Raw audio, email bodies, and transcripts must not be posted into Matrix rooms."},
    {"id":"no_user_messages_as_aor_input","description":"Product user messages must not be routed to AoR as AoR input."},
    {"id":"bridge_cannot_mutate_product_state","description":"matrix_bridge may only update bridge_outbox (delivered_at / delivery_error)."}
  ],
  "baseline_states": {
    "NONE":      {"use_model":"default_model","queue_backfill":true,"provisional":true},
    "QUEUED":    {"use_model":"default_model","queue_backfill":false,"provisional":true},
    "PROCESSING":{"use_model":"default_model","queue_backfill":false,"provisional":true},
    "PARTIAL":   {"use_model":"partial_baseline","queue_backfill":false,"provisional":true},
    "READY":     {"use_model":"contact_baseline","queue_backfill":false,"provisional":false},
    "STALE":     {"use_model":"contact_baseline","queue_refresh":true,"provisional":false}
  },
  "escalation_conditions": [
    {"condition":"advisory.confidence < policy.min_confidence","emit":"ADVISORY_HIGH_UNCERTAINTY"},
    {"condition":"advisory.risk_level == 'high'","emit":"ADVISORY_HIGH_RISK"},
    {"condition":"contact_candidate.sightings_count >= policy.promotion_threshold AND status == 'open'","emit":"CONTACT_MATCH_AMBIGUOUS"},
    {"condition":"processing_job.state == 'FAILED'","emit":"PROCESSING_FAILURE"},
    {"condition":"local_worker.healthy == false","emit":"WORKER_OFFLINE"},
    {"condition":"scheduler dispatched to worker while worker.online == false","emit":"LIVE_TO_DEFERRED_FALLBACK"},
    {"condition":"deploy_hook run finished with status == 'failed'","emit":"DEPLOY_FAILURE"},
    {"condition":"any service health probe returns non-2xx for N consecutive intervals","emit":"SYSTEM_HEALTH_WARNING"},
    {"condition":"contact_baseline.state == 'NONE' and event arrives","emit":"BASELINE_MISSING"},
    {"condition":"scheduler enqueues CONTACT_HISTORY_BACKFILL","emit":"BASELINE_BACKFILL_QUEUED"}
  ],
  "default_visibility_rules": {
    "front_desk_room_alias": "#aor-front-desk:taloscognitive.com",
    "deliberation_room_alias": "#aor-deliberation:taloscognitive.com",
    "alerts_room_alias": "#system-alerts:taloscognitive.com",
    "verbose_room_alias": "#system-verbose:taloscognitive.com",
    "infrastructure_room_alias": "#infrastructure:taloscognitive.com",
    "detail_levels": ["quiet","normal","verbose"],
    "default_detail_level": "normal",
    "user_facing_detail_level": "quiet"
  },
  "phase1_constraints": {
    "aor_to_product_consumer_implemented": false,
    "bridge_user_provisioning": "manual",
    "matrix_bridge_is_thin_relay_only": true,
    "matrix_bridge_does_not_do_reasoning": true,
    "local_worker_is_single_combined_service": true,
    "kubernetes_used": false,
    "argocd_used": false,
    "bridge_outbox_has_retry_attempt_tracking": false
  }
}
TALOS_EOF

# ===========================================================================
# docs/aor_bridge_events.md
# ===========================================================================
cat > docs/aor_bridge_events.md <<'TALOS_EOF'
# AoR Bridge Events

Per-event contracts. Allow-list lives in `aor_translator_boundary.json`.
Routing lives in `config/aor_bridge_rules.example.json`. Required payload
fields live in `config/aor_bridge_events.example.json`.

The bridge is a thin relay: it validates against these contracts and
routes. It does not infer meaning.

## Envelope

Every row in `bridge_outbox`:
- outbox_uuid, event_code, payload (jsonb), created_at,
  delivered_at (nullable), delivery_error (nullable).

Audit rows in `talos_aor.aor_bridge_audit` mirror these plus `direction`.

## Product → AoR events

- PROCESSING_FAILURE — job failed. {job_uuid, job_type, error}.
  Route: #system-alerts, #system-verbose.
- BASELINE_MISSING — baseline state NONE for contact. {contact_uuid}.
  Route: #system-verbose.
- BASELINE_BACKFILL_QUEUED — backfill job enqueued.
  {contact_uuid, idempotency_key}. Route: #system-verbose.
- CONTACT_MATCH_AMBIGUOUS — candidate hit promotion threshold.
  {candidate_uuid, identity_kind, identity_value, sightings_count}.
  Route: #aor-deliberation.
- ADVISORY_HIGH_UNCERTAINTY — low confidence advisory.
  {advisory_uuid, confidence, provenance}. Route: #aor-deliberation.
- ADVISORY_HIGH_RISK — high risk advisory.
  {advisory_uuid, risk_level, summary}. Route: #aor-front-desk, #aor-deliberation.
- WORKER_OFFLINE — scheduler lost worker.
  {local_worker_url, error}. Route: #system-alerts, #infrastructure.
- LIVE_TO_DEFERRED_FALLBACK — job deferred because worker offline.
  {job_uuid, job_type}. Route: #system-verbose.
- DEPLOY_FAILURE — deploy hook failure.
  {trigger, failed_step}. Route: #system-alerts, #infrastructure.
- SYSTEM_HEALTH_WARNING — generic health warning.
  {service, severity, detail}. Route: #system-alerts.

## AoR → Product events (Phase 1: allow-listed, consumer not yet built)

- POLICY_UPDATE, THRESHOLD_UPDATE, DETAIL_LEVEL_UPDATE,
  APPROVED_RESPONSE_TEMPLATE, ESCALATION_DECISION,
  CONTACT_REVIEW_DECISION, FEATURE_FLAG_UPDATE.

## Validation behavior

- event_code not in allow-list → delivery_error = "not_in_allow_list".
- required field missing → delivery_error = "missing_field:<name>".
- allowed but no route → delivery_error = "no_route".
- Matrix delivery failure → row left undelivered for retry.
- Partial delivery (one room fails) → row marked delivered with partial error.

## Known Phase 1 limitation

`bridge_outbox` currently has no attempt_count / last_attempt_at /
terminal_error columns. A poison row (e.g. missing_field that is never
fixed) will sit there forever and the bridge will re-audit it on every
drain. Future pass will add retry/poison-row controls; see
`services/matrix_bridge/app.py` TODO block.
TALOS_EOF

# ===========================================================================
# config/aor_bridge_rules.example.json
# ===========================================================================
cat > config/aor_bridge_rules.example.json <<'TALOS_EOF'
{
  "$schema_version": "1.0.0",
  "default_template": "[{event_code}] {payload_json}",
  "templates": {
    "generic_alert": "\u26a0 [{event_code}] {payload_json}",
    "generic_verbose": "\u2022 {event_code} :: {payload_json}",
    "quiet_user_facing": "A high-risk signal was flagged. See #aor-deliberation for context."
  },
  "event_templates": {
    "BASELINE_MISSING":          "\u00b7 baseline missing for contact {p.contact_uuid}; default model engaged",
    "BASELINE_BACKFILL_QUEUED":  "\u00b7 backfill queued for contact {p.contact_uuid} (key={p.idempotency_key})",
    "CONTACT_MATCH_AMBIGUOUS":   "\ud83d\uddc2 Candidate {p.candidate_uuid} ({p.identity_kind}={p.identity_value}) hit threshold with {p.sightings_count} sightings.",
    "ADVISORY_HIGH_UNCERTAINTY": "\ud83d\uddc2 Advisory {p.advisory_uuid} low-confidence ({p.confidence}) via {p.provenance}.",
    "ADVISORY_HIGH_RISK":        "\u26a0 High-risk advisory {p.advisory_uuid}: {p.summary}",
    "WORKER_OFFLINE":            "\u26a0 Local worker unreachable at {p.local_worker_url} ({p.error})",
    "LIVE_TO_DEFERRED_FALLBACK": "\u00b7 live\u2192deferred: job {p.job_uuid} ({p.job_type})",
    "DEPLOY_FAILURE":            "\u26a0 Deploy failed at step '{p.failed_step}'.",
    "PROCESSING_FAILURE":        "\u26a0 Processing failed: job {p.job_uuid} ({p.job_type}) \u2014 {p.error}",
    "SYSTEM_HEALTH_WARNING":     "\u26a0 {p.service}: {p.severity} \u2014 {p.detail}"
  },
  "default_route": {
    "room_alias": "#system-verbose:taloscognitive.com",
    "detail_level": "verbose",
    "template_key": "generic_verbose"
  },
  "routes": {
    "PROCESSING_FAILURE": [
      {"room_alias":"#system-alerts:taloscognitive.com","detail_level":"normal"},
      {"room_alias":"#system-verbose:taloscognitive.com","detail_level":"verbose"}
    ],
    "BASELINE_MISSING": [
      {"room_alias":"#system-verbose:taloscognitive.com","detail_level":"verbose"}
    ],
    "BASELINE_BACKFILL_QUEUED": [
      {"room_alias":"#system-verbose:taloscognitive.com","detail_level":"verbose"}
    ],
    "CONTACT_MATCH_AMBIGUOUS": [
      {"room_alias":"#aor-deliberation:taloscognitive.com","detail_level":"normal"}
    ],
    "ADVISORY_HIGH_UNCERTAINTY": [
      {"room_alias":"#aor-deliberation:taloscognitive.com","detail_level":"normal"}
    ],
    "ADVISORY_HIGH_RISK": [
      {"room_alias":"#aor-front-desk:taloscognitive.com","detail_level":"quiet","template_key":"quiet_user_facing"},
      {"room_alias":"#aor-deliberation:taloscognitive.com","detail_level":"normal"}
    ],
    "WORKER_OFFLINE": [
      {"room_alias":"#system-alerts:taloscognitive.com","detail_level":"normal"},
      {"room_alias":"#infrastructure:taloscognitive.com","detail_level":"verbose"}
    ],
    "LIVE_TO_DEFERRED_FALLBACK": [
      {"room_alias":"#system-verbose:taloscognitive.com","detail_level":"verbose"}
    ],
    "DEPLOY_FAILURE": [
      {"room_alias":"#system-alerts:taloscognitive.com","detail_level":"normal"},
      {"room_alias":"#infrastructure:taloscognitive.com","detail_level":"verbose"}
    ],
    "SYSTEM_HEALTH_WARNING": [
      {"room_alias":"#system-alerts:taloscognitive.com","detail_level":"normal"}
    ]
  }
}
TALOS_EOF

# ===========================================================================
# config/aor_bridge_events.example.json
# ===========================================================================
cat > config/aor_bridge_events.example.json <<'TALOS_EOF'
{
  "$schema_version": "1.0.0",
  "allowed_events_product_to_aor": [
    "PROCESSING_FAILURE","BASELINE_MISSING","BASELINE_BACKFILL_QUEUED",
    "CONTACT_MATCH_AMBIGUOUS","ADVISORY_HIGH_UNCERTAINTY","ADVISORY_HIGH_RISK",
    "WORKER_OFFLINE","LIVE_TO_DEFERRED_FALLBACK","DEPLOY_FAILURE",
    "SYSTEM_HEALTH_WARNING"
  ],
  "allowed_events_aor_to_product": [
    "POLICY_UPDATE","THRESHOLD_UPDATE","DETAIL_LEVEL_UPDATE",
    "APPROVED_RESPONSE_TEMPLATE","ESCALATION_DECISION",
    "CONTACT_REVIEW_DECISION","FEATURE_FLAG_UPDATE"
  ],
  "required_payload_fields": {
    "PROCESSING_FAILURE":        ["job_uuid","job_type","error"],
    "BASELINE_MISSING":          ["contact_uuid"],
    "BASELINE_BACKFILL_QUEUED":  ["contact_uuid","idempotency_key"],
    "CONTACT_MATCH_AMBIGUOUS":   ["candidate_uuid","identity_kind","identity_value","sightings_count"],
    "ADVISORY_HIGH_UNCERTAINTY": ["advisory_uuid","confidence","provenance"],
    "ADVISORY_HIGH_RISK":        ["advisory_uuid","risk_level","summary"],
    "WORKER_OFFLINE":            ["local_worker_url","error"],
    "LIVE_TO_DEFERRED_FALLBACK": ["job_uuid","job_type"],
    "DEPLOY_FAILURE":            ["trigger","failed_step"],
    "SYSTEM_HEALTH_WARNING":     ["service","severity","detail"]
  }
}
TALOS_EOF

echo "Wrote part 2 (infra READMEs + docs + config)."
echo "Continuing with part 3..."
# ===========================================================================
# services/deploy_hook/*
# ===========================================================================
cat > services/deploy_hook/requirements.txt <<'TALOS_EOF'
fastapi==0.115.0
uvicorn[standard]==0.30.6
httpx==0.27.2
TALOS_EOF

cat > services/deploy_hook/Dockerfile <<'TALOS_EOF'
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends \
        git ca-certificates curl gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" \
        > /etc/apt/sources.list.d/docker.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends docker-ce-cli docker-compose-plugin \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
EXPOSE 8090
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8090"]
TALOS_EOF

cat > services/deploy_hook/README.md <<'TALOS_EOF'
# deploy_hook

Receives Gitea push webhooks and redeploys the product stack.

Webhook URL: https://intake.taloscognitive.com/_deploy/gitea
Secret: GITEA_WEBHOOK_SECRET in deploy/docker/aws/product/.env
Trigger: Push events, branch=main.

What it does:
1. git fetch --all --prune
2. git reset --hard origin/main
3. docker compose build
4. docker compose up -d
5. Probe HEALTHCHECK_URLS
6. Record result in ring buffer visible at /_deploy/recent

Product stack only. AoR stack is redeployed manually in Phase 1.
TALOS_EOF

cat > services/deploy_hook/app.py <<'TALOS_EOF'
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
TALOS_EOF

# ===========================================================================
# services/intake_api/*
# ===========================================================================
cat > services/intake_api/requirements.txt <<'TALOS_EOF'
fastapi==0.115.0
uvicorn[standard]==0.30.6
httpx==0.27.2
TALOS_EOF

cat > services/intake_api/Dockerfile <<'TALOS_EOF'
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
RUN mkdir -p /var/lib/talos/raw
EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
TALOS_EOF

cat > services/intake_api/app.py <<'TALOS_EOF'
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
DIALPAD_SECRET = os.environ.get("DIALPAD_WEBHOOK_SECRET","")
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
    if not _verify_dialpad(raw, x_dialpad_signature):
        raise HTTPException(401, "bad signature")
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
TALOS_EOF

# ===========================================================================
# services/scheduler/*
# ===========================================================================
cat > services/scheduler/requirements.txt <<'TALOS_EOF'
fastapi==0.115.0
uvicorn[standard]==0.30.6
httpx==0.27.2
asyncpg==0.29.0
TALOS_EOF

cat > services/scheduler/Dockerfile <<'TALOS_EOF'
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
EXPOSE 8082
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8082"]
TALOS_EOF

cat > services/scheduler/app.py <<'TALOS_EOF'
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
                body = {"job_uuid":str(j["job_uuid"]),"job_type":j["job_type"],
                        "contact_uuid":str(j["contact_uuid"]) if j["contact_uuid"] else None,
                        "event_uuid":str(j["event_uuid"]) if j["event_uuid"] else None,
                        "payload":j["payload"]}
                h = {"Authorization":f"Bearer {LOCAL_WORKER_TOKEN}"} if LOCAL_WORKER_TOKEN else {}
                try:
                    r = await c.post(f"{LOCAL_WORKER_URL.rstrip('/')}/dispatch", json=body, headers=h)
                    if 200<=r.status_code<300:
                        await conn.execute(
                            "UPDATE processing_jobs SET state='DISPATCHED', dispatched_at=now() WHERE job_uuid=$1",
                            j["job_uuid"])
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
TALOS_EOF

# ===========================================================================
# services/contact_engine/*
# ===========================================================================
cat > services/contact_engine/requirements.txt <<'TALOS_EOF'
fastapi==0.115.0
uvicorn[standard]==0.30.6
asyncpg==0.29.0
pydantic==2.9.2
TALOS_EOF

cat > services/contact_engine/Dockerfile <<'TALOS_EOF'
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
EXPOSE 8083
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8083"]
TALOS_EOF

cat > services/contact_engine/app.py <<'TALOS_EOF'
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
TALOS_EOF

# ===========================================================================
# services/matrix_bridge/*  (WITH poison-row/retry TODO added)
# ===========================================================================
cat > services/matrix_bridge/requirements.txt <<'TALOS_EOF'
fastapi==0.115.0
uvicorn[standard]==0.30.6
httpx==0.27.2
asyncpg==0.29.0
TALOS_EOF

cat > services/matrix_bridge/Dockerfile <<'TALOS_EOF'
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py .
EXPOSE 8084
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8084"]
TALOS_EOF

cat > services/matrix_bridge/app.py <<'TALOS_EOF'
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
TALOS_EOF

# ===========================================================================
# worker/local
# ===========================================================================
cat > worker/local/docker-compose.yml <<'TALOS_EOF'
# TALOS Local Worker (Phase 1). Single combined worker service.
name: talos-worker
networks:
  worker_net:
    driver: bridge
volumes:
  worker_work:
  worker_cache:
services:
  worker:
    build:
      context: ./app
    container_name: talos_worker
    restart: unless-stopped
    environment:
      PRODUCT_DATABASE_URL: ${PRODUCT_DATABASE_URL}
      LOCAL_WORKER_TOKEN: ${LOCAL_WORKER_TOKEN}
      ARTIFACT_ARCHIVE_DIR: /mnt/archive
      LOG_LEVEL: ${LOG_LEVEL:-INFO}
      ENABLED_MODULES: ${ENABLED_MODULES:-transcription,behavior_analysis,contact_engine_local,baseline_updates,deferred_backfill}
    volumes:
      - worker_work:/var/lib/talos/work
      - worker_cache:/root/.cache
      # - //diskstation/talos-archive:/mnt/archive
    ports:
      - "8081:8081"
    networks: [worker_net]
TALOS_EOF

cat > worker/local/.env.example <<'TALOS_EOF'
# TALOS Local Worker env (Phase 1).
#
# NOTE ON PRODUCT_DATABASE_URL:
# This is a Phase 1 PLACEHOLDER. Direct laptop -> EC2 Postgres connectivity
# is NOT assumed to be solved here. EC2 Postgres is bound to 127.0.0.1 on
# the host for safety, so the worker cannot simply point at the Tailscale
# name of the EC2 box and connect.
#
# Reaching the product DB from the worker requires intentional choice,
# which Phase 1 does NOT dictate:
#   (a) Tailscale-forwarded port (tailscale serve / tsnet sidecar) into the
#       EC2 container network, OR
#   (b) A thin product-side HTTP API the worker uses instead of direct SQL, OR
#   (c) Rebinding Postgres to the Tailscale interface on EC2 (only if the
#       operator accepts the blast radius).
#
# Until one of those is explicitly configured, leave this value as a
# placeholder and treat direct worker-to-DB access as DEFERRED.
PRODUCT_DATABASE_URL=postgresql://talos_product:CHANGE_ME_STRONG_PASSWORD@PLACEHOLDER_UNTIL_TAILSCALE_PATH_CHOSEN:5432/talos_product

LOCAL_WORKER_TOKEN=CHANGE_ME_WORKER_TOKEN
ENABLED_MODULES=transcription,behavior_analysis,contact_engine_local,baseline_updates,deferred_backfill
LOG_LEVEL=INFO
TALOS_EOF

cat > worker/local/app/Dockerfile <<'TALOS_EOF'
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p /var/lib/talos/work /mnt/archive
EXPOSE 8081
CMD ["uvicorn", "worker_main:app", "--host", "0.0.0.0", "--port", "8081"]
TALOS_EOF

cat > worker/local/app/requirements.txt <<'TALOS_EOF'
fastapi==0.115.0
uvicorn[standard]==0.30.6
httpx==0.27.2
asyncpg==0.29.0
pydantic==2.9.2
TALOS_EOF

cat > worker/local/app/worker_main.py <<'TALOS_EOF'
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
TALOS_EOF

cat > worker/local/app/modules/__init__.py <<'TALOS_EOF'
"""TALOS worker module registry."""
TALOS_EOF

cat > worker/local/app/modules/transcription.py <<'TALOS_EOF'
"""modules/transcription — Whisper/pyannote transcription + speaker ID."""
from __future__ import annotations
from worker_main import register

@register("TRANSCRIBE", "transcription")
async def handle_transcribe(job: dict) -> dict:
    # TODO(phase1): wire into existing comm_analysis pipeline.
    return {"todo":"transcription not yet implemented",
            "job_uuid":job.get("job_uuid"),
            "event_uuid":job.get("event_uuid")}
TALOS_EOF

cat > worker/local/app/modules/behavior_analysis.py <<'TALOS_EOF'
"""modules/behavior_analysis — feature extraction + advisory generation."""
from __future__ import annotations
from worker_main import register

@register("FEATURE_EXTRACT", "behavior_analysis")
async def handle_fe(job: dict) -> dict:
    return {"todo":"feature extraction not yet implemented",
            "job_uuid":job.get("job_uuid"),
            "event_uuid":job.get("event_uuid")}

@register("ADVISORY_GENERATE", "behavior_analysis")
async def handle_adv(job: dict) -> dict:
    return {"todo":"advisory generation not yet implemented",
            "job_uuid":job.get("job_uuid"),
            "contact_uuid":job.get("contact_uuid")}
TALOS_EOF

cat > worker/local/app/modules/contact_engine_local.py <<'TALOS_EOF'
"""modules/contact_engine_local — fuzzy/probabilistic matching (deferred)."""
from __future__ import annotations
from worker_main import register

@register("CONTACT_MATCH_FUZZY", "contact_engine_local")
async def handle_fuzzy(job: dict) -> dict:
    return {"todo":"fuzzy contact matching deliberately deferred past Phase 1",
            "job_uuid":job.get("job_uuid")}

@register("CONTACT_MERGE_RECOMMEND", "contact_engine_local")
async def handle_merge(job: dict) -> dict:
    return {"todo":"merge recommendation deliberately deferred past Phase 1",
            "job_uuid":job.get("job_uuid")}
TALOS_EOF

cat > worker/local/app/modules/baseline_updates.py <<'TALOS_EOF'
"""modules/baseline_updates — per-contact baseline backfill + refresh."""
from __future__ import annotations
from worker_main import register

@register("CONTACT_HISTORY_BACKFILL", "baseline_updates")
async def handle_backfill(job: dict) -> dict:
    return {"todo":"baseline backfill not yet implemented",
            "job_uuid":job.get("job_uuid"),
            "contact_uuid":job.get("contact_uuid")}

@register("BASELINE_REFRESH", "baseline_updates")
async def handle_refresh(job: dict) -> dict:
    return {"todo":"baseline refresh not yet implemented",
            "job_uuid":job.get("job_uuid"),
            "contact_uuid":job.get("contact_uuid")}
TALOS_EOF

cat > worker/local/app/modules/deferred_backfill.py <<'TALOS_EOF'
"""modules/deferred_backfill — drains DEFERRED jobs when worker is online."""
from __future__ import annotations
from worker_main import register

@register("DEFERRED_DRAIN", "deferred_backfill")
async def handle_drain(job: dict) -> dict:
    return {"todo":"deferred drain not yet implemented",
            "job_uuid":job.get("job_uuid")}
TALOS_EOF

cat > worker/local/app/modules/popup_live_assist.py <<'TALOS_EOF'
"""modules/popup_live_assist — future real-time assist surface."""
from __future__ import annotations
# No @register(...) in Phase 1. Placeholder only.
TALOS_EOF

cat > worker/local/app/modules/README.md <<'TALOS_EOF'
# TALOS Worker Modules

Each file in this directory registers one or more job handlers via
`@worker_main.register("<JOB_TYPE>", "<module_name>")`. Disabled modules
(via `ENABLED_MODULES`) no-op cleanly.

## Contract

    async def handle(job: dict) -> dict:
        # job = {job_uuid, job_type, contact_uuid?, event_uuid?, payload}
        return {...}  # JSON-serializable

Exceptions become `{"status":"error", "module":..., "error":...}`.

## Job type catalog (Phase 1)

- TRANSCRIBE                → transcription
- FEATURE_EXTRACT           → behavior_analysis
- ADVISORY_GENERATE         → behavior_analysis
- CONTACT_MATCH_FUZZY       → contact_engine_local
- CONTACT_MERGE_RECOMMEND   → contact_engine_local
- CONTACT_HISTORY_BACKFILL  → baseline_updates
- BASELINE_REFRESH          → baseline_updates
- DEFERRED_DRAIN            → deferred_backfill
- (none yet)                → popup_live_assist

## Must not

- Cross the AoR/Translator boundary.
- Mutate talos_aor.
- Do synchronous blocking I/O inside a handler.
- Do heavy work at import time.
TALOS_EOF

echo ""
echo "DONE. Scaffold written under $(pwd)"
echo "Total files:"
find . -type f | wc -l
echo ""
echo "Manifest:"
find . -type f | sort
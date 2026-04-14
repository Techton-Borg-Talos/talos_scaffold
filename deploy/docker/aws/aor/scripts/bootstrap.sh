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

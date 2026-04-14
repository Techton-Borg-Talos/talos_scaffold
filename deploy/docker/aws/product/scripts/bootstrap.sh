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

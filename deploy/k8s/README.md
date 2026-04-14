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

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

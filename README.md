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

## Operational Notes

This section records the known-good deployment behavior and the key troubleshooting outcomes from the first successful AWS Phase 1 bring-up.

### Deployment Order

For first deployment, use this sequence:

1. Bring up the AoR stack first.
2. Generate Synapse configuration on first run.
3. Bring up the product stack second.
4. Create public DNS records for:

   * `intake.taloscognitive.com`
   * `matrix.taloscognitive.com`
   * `chat.taloscognitive.com`
5. Wait for public DNS resolution before expecting automatic certificate issuance to succeed.

If DNS is not live yet, Caddy may report ACME failures such as `NXDOMAIN`. This is expected until the records exist and resolve publicly.

### Environment Changes and Container Lifecycle

Changes to `.env` values are **not** applied by a plain container restart when the service depends on environment variables loaded at container creation time.

Use container recreation instead:

```bash
docker compose --project-name talos-product \
  -f deploy/docker/aws/product/docker-compose.yml \
  up -d --no-deps --force-recreate <service_name>
```

This was required for services such as:

* `matrix_bridge`
* `deploy_hook`

Operational rule:

* `restart` reuses the existing container and its original environment
* `--force-recreate` creates a new container and loads the current `.env` values

### Public Validation

When the stack is healthy, the following endpoints should return `200 OK`:

```bash
curl https://intake.taloscognitive.com/healthz
curl https://matrix.taloscognitive.com/_matrix/client/versions
curl https://chat.taloscognitive.com/
```

### Internal Health Checks

Some services are only reachable on the Docker network and are not published to the host. Host-level checks against unpublished ports may fail even when the service is healthy.

Example of an expected host failure:

```bash
curl http://127.0.0.1:8084/healthz
```

Use container-local or Docker-network checks instead.

Example:

```bash
docker exec talos_matrix_bridge python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8084/healthz').read().decode())"
```

### Matrix Bridge Requirements

For `matrix_bridge` to deliver messages successfully, all of the following must be true:

1. The target room alias exists exactly as configured.
2. The bridge user is invited and has joined the room.
3. `MATRIX_BRIDGE_USER_TOKEN` is a valid access token for `@talos-bridge:taloscognitive.com`.
4. `BRIDGE_DRY_RUN=0` is set.
5. The `matrix_bridge` container is recreated after token or dry-run changes.

### Matrix Bridge Error Interpretation

Common bridge failure patterns:

* `404 Not Found` during room directory lookup
  The configured room alias does not exist or does not match the actual room alias.

* `401 Unauthorized` during `send/m.room.message`
  The running container is using an invalid, stale, or not-yet-reloaded token.

* `M_FORBIDDEN ... not in room`
  The bridge user has not joined the target room.

### Matrix Bridge Verification Commands

Check the bridge token:

```bash
TOKEN=$(grep '^MATRIX_BRIDGE_USER_TOKEN=' deploy/docker/aws/product/.env | cut -d= -f2-)
curl -sS -H "Authorization: Bearer $TOKEN" https://matrix.taloscognitive.com/_matrix/client/v3/account/whoami
```

Check room membership:

```bash
ROOM_ID='!REPLACE_WITH_ROOM_ID:taloscognitive.com'
TOKEN=$(grep '^MATRIX_BRIDGE_USER_TOKEN=' deploy/docker/aws/product/.env | cut -d= -f2-)
curl -sS -H "Authorization: Bearer $TOKEN" "https://matrix.taloscognitive.com/_matrix/client/v3/rooms/$ROOM_ID/joined_members"
```

Manual send test:

```bash
ROOM_ID='!REPLACE_WITH_ROOM_ID:taloscognitive.com'
TOKEN=$(grep '^MATRIX_BRIDGE_USER_TOKEN=' deploy/docker/aws/product/.env | cut -d= -f2-)
TXN="manualtest-$(date +%s)"
curl -i -X PUT \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"msgtype":"m.text","body":"manual bridge token test"}' \
  "https://matrix.taloscognitive.com/_matrix/client/v3/rooms/$ROOM_ID/send/m.room.message/$TXN"
```

### Gitea Webhook Configuration

Deploy hook endpoint:

```text
https://intake.taloscognitive.com/_deploy/gitea
```

Recommended webhook settings:

* Method: `POST`
* Content type: `application/json`
* Trigger: `Push Events`
* Branch filter: `main`

Branch filters are case-sensitive. Use `main`, not `Main`.

If `GITEA_WEBHOOK_SECRET` changes, recreate the `deploy_hook` container:

```bash
docker compose --project-name talos-product \
  -f deploy/docker/aws/product/docker-compose.yml \
  up -d --no-deps --force-recreate deploy_hook
```

### Established Phase 1 Baseline

The following Phase 1 components have been validated together:

* AWS host deployment
* AoR stack
* Product stack
* Caddy public edge
* Synapse / Matrix
* Element
* Matrix bridge delivery
* Gitea deploy webhook


AWS auto-deploy notes:
- AWS repo path: /opt/talos/repo
- Product stack: deploy/docker/aws/product/docker-compose.yml
- Deployer stack: deploy/docker/aws/deployer/docker-compose.yml
- Deployer build context: services/deploy_hook
- Gitea remote uses SSH
- deploy hook container must include openssh-client for git fetch
- deploy hook is exposed on product_net to Caddy, not published to host localhost

Post-reboot verification:
cd /opt/talos/repo
git rev-parse HEAD
sudo docker ps --format "table {{.Names}}\t{{.Status}}"

Deploy hook live logs:
sudo docker logs -f talos_deploy_hook

Force deployer rebuild:
sudo docker compose --project-name talos-deployer \
  -f /opt/talos/repo/deploy/docker/aws/deployer/docker-compose.yml \
  build --no-cache
sudo docker compose --project-name talos-deployer \
  -f /opt/talos/repo/deploy/docker/aws/deployer/docker-compose.yml \
  up -d

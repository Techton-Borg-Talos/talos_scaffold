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

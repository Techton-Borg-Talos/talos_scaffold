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

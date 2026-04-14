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

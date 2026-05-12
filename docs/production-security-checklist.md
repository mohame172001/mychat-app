# Production Security Checklist

Use this checklist before public launch, Meta review updates, and billing rollout. Do not paste secret values into this file, tickets, screenshots, or chat.

## Canonical URLs

- Frontend: `https://frontend-production-6eb2.up.railway.app`
- Backend: `https://backend-production-a1a3.up.railway.app`
- Backend health: `GET https://backend-production-a1a3.up.railway.app/api/`
- Instagram OAuth redirect: `https://backend-production-a1a3.up.railway.app/api/instagram/callback`
- Instagram webhook callback: `https://backend-production-a1a3.up.railway.app/api/instagram/webhook`
- Meta data deletion callback: `https://backend-production-a1a3.up.railway.app/api/meta/data-deletion`

## Required Environment Posture

- `APP_ENV=production` or equivalent production flag set.
- `DEBUG=false` or unset.
- `JWT_SECRET` set to a strong random value. Never reuse development values.
- `META_WEBHOOK_VERIFY_TOKEN` set. Do not print the value.
- `META_WEBHOOK_APP_SECRET` or `META_APP_SECRET` set when HMAC enforcement is required.
- `META_WEBHOOK_HMAC_ENFORCE=true` for production webhook signature enforcement.
- `ENABLE_ADMIN_REPAIR_TOOLS=false` or unset except during an explicitly approved repair window.
- `ADMIN_EMAILS` contains at least one owner escape-hatch email.
- Google Sign-In only needs public client IDs in frontend/backend env; no Google client secret should be in frontend env.
- Login and signup rate limits enabled for IP and normalized identifier/email-hash buckets.
- `ADMIN_EMAILS` reviewed: it should contain only intended owner escape-hatch emails, and removing a bootstrap owner fully requires removing the env email as well as disabling/removing the member row.

## API and Browser Security

- FastAPI docs, ReDoc, and OpenAPI are disabled by default in production.
- CORS allowlist must not be `*` when credentials are enabled.
- Security headers are enabled on backend responses:
  - `X-Content-Type-Options: nosniff`
  - `X-Frame-Options: DENY`
  - `Referrer-Policy: strict-origin-when-cross-origin`
  - `Permissions-Policy` denies camera, microphone, geolocation, and payment.
  - Production API CSP defaults to `default-src 'none'; frame-ancestors 'none'; base-uri 'none'` unless overridden.
  - HSTS is sent only for HTTPS production requests.
- Frontend source maps policy: production source maps may exist only if Railway/static hosting access is acceptable; otherwise disable source map publishing before public scale.

## Meta / Graph API Version

- Current documented Facebook Graph API version: `v21.0`.
- Instagram Graph calls use `graph.instagram.com`; diagnostics may probe versioned endpoints when needed, but production Meta dashboard settings should be kept aligned with the documented version.
- Expected webhook subscription fields: `comments`, `messages`.
- Keep unused webhook fields disabled until product support exists: `live_comments`, `mentions`, `message_edit`, `message_reactions`, `messaging_handover`, `messaging_postbacks`, `messaging_referral`, `messaging_seen`, `standby`.

## Operational Flags

- System Health may show booleans such as HMAC configured and repair tools enabled, but never secret values.
- Admin repair/debug endpoints are unavailable to normal users and audited when enabled.
- Webhook logs store safe metadata only: counts, status, hashes, and timestamps. No raw webhook body.
- Admin role changes are DB-backed and should take effect without waiting for JWT expiry.
- Suspended/deleted users with an old JWT should be blocked from normal app APIs with `account_suspended` or `account_deleted`.
- Logout, login, signup, Google login, and `401` auth resets should clear user-specific frontend API cache.

## Manual Checks

1. Confirm backend health returns `200`.
2. Confirm unauthenticated protected routes return `403`.
3. Confirm `/privacy`, `/terms`, and `/data-deletion` load publicly.
4. Confirm a bad webhook verify token returns `403`.
5. Confirm production logs do not contain access tokens, raw comments, raw replies, raw DMs, raw Graph bodies, Google credentials, Authorization headers, cookies, or raw webhook bodies.
6. Confirm Mongo backups and restore process are configured outside the app before billing launch.
7. Confirm password signup/email login casing behavior: `Test@Example.com` and `test@example.com` resolve to the same account identity.
8. Confirm no Google client secret or password reset token is present in frontend env, build output, logs, docs, or screenshots.

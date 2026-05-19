# Security Policy

## Reporting a vulnerability

If you discover a security issue in mychat, **please do not open a
public GitHub issue.** Email the details to
**mm.mohame172000@gmail.com** with the subject line *"Security report:
mychat"*. We aim to acknowledge new reports within 72 hours and
ship a fix or a documented mitigation within 30 days.

When reporting, include:

- A description of the vulnerability and its impact
- Steps to reproduce (a minimal proof-of-concept is appreciated)
- The affected endpoint, page, or commit SHA if you can identify it

We will credit you in the changelog once a fix lands, unless you ask
to stay anonymous.

## Supported versions

Only the `master` branch (currently deployed at Railway) receives
security patches. Forks and older deploys are out of scope.

## What's already in place

| Area | Control |
|---|---|
| Auth | JWT (HS256) with 30-day TTL, `JWT_SECRET ≥ 32 chars` enforced in production, bcrypt password hashing via passlib, 8-128 char password length enforced both client- and server-side |
| Tokens | `secrets.token_urlsafe(32)` for email-verification and password-reset tokens, stored as HMAC-SHA256 hashes so the raw value never sits in the database |
| Webhooks | `X-Hub-Signature-256` HMAC verification required in production; the server refuses to boot if `META_WEBHOOK_APP_SECRET` is missing. `hmac.compare_digest` for every signature compare. |
| Headers | `Strict-Transport-Security`, `Content-Security-Policy frame-ancestors 'none'`, `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Referrer-Policy: strict-origin-when-cross-origin`, `Permissions-Policy: camera=(), microphone=(), geolocation=(), payment=()` |
| Caching | `Cache-Control: no-store, private` on every authenticated endpoint |
| CORS | Explicit allowlist in production (`CORS_ALLOWED_ORIGINS` + `FRONTEND_URL` + `RAILWAY_PUBLIC_DOMAINS`); no wildcards, no localhost |
| Rate limiting | Per-IP + per-account buckets on `/auth/login`, `/auth/signup`, `/auth/forgot-password`, `/instagram/connect`, `/automations/poll-now`, `/meta/data-deletion`, and admin heavy actions |
| Body size | Hard 2 MB cap (`REQUEST_BODY_MAX_BYTES`, tunable) — DoS-by-multi-GB-POST is blocked before any handler runs |
| Input validation | Every Pydantic input model has explicit `Field(min_length, max_length, ge, le)` — usernames ≤32, emails ≤254, passwords 8-128, audience sizes 0-10M |
| Logging | `X-Request-Id` on every response; sensitive headers/bodies (tokens, passwords, message content) never logged |
| Outbound HTTP | All `httpx` callouts use fixed env-configured URLs (Meta Graph + email webhook). No user-controlled URL ever reaches the HTTP client — SSRF is not reachable. |
| Frontend XSS | No `dangerouslySetInnerHTML` anywhere in the source. No `eval` / `new Function`. URL params are passed through `encodeURIComponent`. `target=_blank` links carry `rel="noopener noreferrer"`. |
| 401 handling | Frontend clears `localStorage` and redirects to `/login` on every 401 so a revoked token can't linger in cache |

## What you should still review yourself before going to prod

- Run `npm audit` (or your dependency scanner of choice) on a fresh
  install — we don't commit `package-lock.json` to the repo today
- Rotate `JWT_SECRET`, `META_APP_SECRET`, and `CRON_SECRET` on a
  schedule and after any suspected breach
- Restrict the MongoDB instance to private networking (no public IPs)
- Enable audit logging on your Railway / Mongo Atlas accounts
- If you fork: change every default placeholder in `.env.example`
  before deploying. A few of them (`META_VERIFY_TOKEN`,
  `change_me_to_random_*`) are intentionally weak so you notice them.

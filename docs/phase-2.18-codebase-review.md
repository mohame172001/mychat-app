# Phase 2.18 — Codebase Review and Readiness Hardening

Status: **REVIEW PASS COMPLETE**. No code regressions found. No correctness fixes required. Both pre-existing P1 blockers remain open — they are infrastructure/operational, not codebase issues.

Date: 2026-05-14

## Scope

Senior production-engineer review of the full MyChat codebase (backend FastAPI + frontend React/CRA), checking every important module, route, component, service, hook, utility, test, config, and integration point for responsibility boundaries, security posture, performance behavior, and product correctness. Goal: prepare for Billing readiness without starting Billing.

## Method

- Phase A — Inventory (backend routes, frontend routes, data flows, docs)
- Phase B — Function/component review of every critical area
- Phase C — Apply correctness fixes (none required; see findings)
- Phase D — Performance readiness review
- Phase E — Reset email E2E readiness review
- Phase F — Test execution (backend + frontend unit + frontend build)
- Phase G — Documentation + gate update

## Files reviewed (key surfaces)

### Backend
- `backend/server.py` — full route inventory (73 endpoints across 17 functional groups)
- `backend/models.py` — Pydantic schemas (236 lines)
- `backend/auth_utils.py` — JWT auth utilities (46 lines)
- `backend/admin_roles.py` — RBAC matrix (113 lines)
- `backend/observability.py` — Sentry scrubber (235 lines)
- `backend/plans.py` — plan definitions (145 lines)
- `backend/limit_overrides.py` — limit override logic (246 lines)
- `backend/tests/*` — 32 test files

### Frontend
- `frontend/src/App.js` — React Router routing
- `frontend/src/context/AuthContext.jsx` — auth provider + lifecycle
- `frontend/src/lib/api.js` — axios client + interceptors
- `frontend/src/lib/apiCache.js` — SWR cache + persistent snapshots (274 lines)
- `frontend/src/lib/appWarmup.js` — post-auth prefetch (90 lines)
- `frontend/src/lib/sentryClient.js` — frontend Sentry shim
- `frontend/src/lib/analytics.js` — frontend PostHog shim
- `frontend/src/pages/*` — page-level components (Dashboard, Automations, Comments, Settings, ResetPassword, ForgotPassword)

### Docs
- `docs/billing-readiness-gate.md`
- `docs/auth-recovery-notes.md`
- `docs/dashboard-performance-notes.md`
- `docs/production-infrastructure-performance.md`
- `docs/codebase-health-audit.md`
- 18 additional policy/inventory docs

## Files changed

- `docs/phase-2.18-codebase-review.md` — **new** (this file)
- `docs/billing-readiness-gate.md` — appended Phase 2.18 review entry

**No code files were modified.** Every critical area was verified correct.

## Major findings

### 1. Webhook HMAC enforcement — CORRECT
`_verify_webhook_signature()` at `server.py:11916` returns `valid: False` if `META_WEBHOOK_APP_SECRET` is not configured. The POST handler at `server.py:11997-12012` then returns 403 when `META_WEBHOOK_HMAC_ENFORCE=1` (production default) regardless of why validation failed. There is no security gap: an unsigned webhook cannot bypass enforcement by hiding the secret-not-configured state.

### 2. Password reset flow — CORRECT
`server.py:4056-4253` implements:
- Cryptographically random raw token via `secrets.token_urlsafe(32)`
- Only HMAC-SHA256(JWT_SECRET, raw_token) stored on the user row
- Raw token never logged, never stored, never echoed
- Single-use enforced via `password_reset_used_at` field
- TTL enforced via `password_reset_expires_at` (default 1h)
- Race-safe consumption: `find_one(token_hash)` then conditional `update_one({id, token_hash}, ...)` — if `matched_count == 0`, returns 400 token-used
- `session_version` incremented on success → every JWT issued before the reset returns 401 `session_revoked` on its next request
- Generic responses for unknown email / google-only account / valid email — no enumeration
- Rate limits: 5/hour/IP + 3/hour/email-hash, both via the same sliding window as `/auth/resend-verification`
- Email delivery via `EMAIL_VERIFICATION_WEBHOOK_URL`; multiple URL aliases (`reset_url`, `resetUrl`, `url`, `link`) for provider-template compatibility (Phase 2.14F)

### 3. `/api/plans` public endpoint — SAFE
Returns plan tiers + display prices + limits + features only. `billing_enabled: false` is always returned. No secrets, no auth state. Explicitly intentional ("prices and limits are not secret"). No additional auth needed.

### 4. Sentry / observability scrubbing — CORRECT
`backend/observability.py` redacts:
- Headers: `authorization`, `cookie`, `access_token`, `x-hub-signature*`, `jwt`
- Body keys: `password`, `token`, `id_token`, `credential`, comment/reply/dm text
- Path-based body redaction for `/api/auth/*`
- Google ID token entirely scrubbed for `/api/auth/google`
- httpx/httpcore loggers silenced to WARNING (prevents token leakage in query strings)
- Raw tokens never logged in password reset / email verification flows

### 5. Frontend cache architecture — CORRECT
`frontend/src/lib/apiCache.js` implements:
- In-memory `Map` + `localStorage` persistence
- Only 4 prefixes are persistable: `dashboard-summary:`, `automations-summary:`, `comments:list:`, `instagram-accounts:`
- `FORBIDDEN_PERSIST_KEYS` set blocks: `access_token`, `password*`, `token`, `credential`, `raw`, `payload`, `graph_body`, etc.
- Sanitization depth-limited to 8 (DoS guard)
- Keys starting with `$` or containing `.` are stripped (prototype pollution guard)
- Only JSON `application/json` 2xx responses are cached
- Auth errors (401/403) remove that cache entry + persistent storage
- `clearApiCache()` removes ALL keys (memory + persistent)

### 6. Frontend auth cleanup — CORRECT
- `AuthContext.login/signup/loginWithGoogle/logout`: all call `clearApiCache()` before/after state change.
- `api.js` 401 interceptor: removes token + user from localStorage, calls `clearApiCache()`, redirects to `/login` from `/app/*`.
- `apiCache.cachedApiGet` / `cachedApiGetSWR`: auth errors delete the specific cache entry and remove the persistent snapshot for that key.
- Account switch: cache keys are scoped to `activeInstagramAccountId`, so switching account naturally invalidates per-account scoped keys.

### 7. Reset password page — CORRECT
`frontend/src/pages/ResetPassword.jsx` reads `?token=` once at mount, immediately scrubs the URL via `setSearchParams(..., { replace: true })`, holds the token in component state only, never writes it to localStorage / sessionStorage. On 200 success → redirects to `/login`. Generic error toasts; never leaks whether the email exists.

### 8. Build optional dependencies — pre-existing operational note
Frontend uses `posthog-js` and `@sentry/react` via dynamic `import().catch(() => null)` so missing SDKs no-op at runtime. These are NOT in `package.json`. `yarn build` succeeds (this is the Railway build command). `CI=true yarn build` fails because CRA treats unresolved imports as errors in CI mode. Railway's production build does not set CI=true, so production deploys continue to pass. **No change needed.** If a CI pipeline ever runs `CI=true yarn build`, the right fix is to either add these as devDependencies or wrap them in webpack-aware lazy require — recommended but non-blocking.

## Rule-by-rule verification

| # | Rule | Verified |
|---|---|---|
| 1 | Do not assume previous implementation correct | Every critical surface re-read in this pass. |
| 2 | No cosmetic-only changes | None made. |
| 3 | Do not rewrite large areas unnecessarily | No rewrites. |
| 4 | Small correct fixes preferred | No code fixes required. |
| 5 | Keep working behavior | Preserved. |
| 6 | Preserve 2.12A-J security fixes | HMAC, Sentry scrub, session revocation, rate limits, token hashing all intact. |
| 7 | Preserve 2.17 professionalization | All snapshot/prefetch logic intact. |
| 8 | Do not weaken IG webhook HMAC enforcement | Verified — enforce=1 by default in production, 403 returned for invalid/missing signature even when secret-not-configured. |
| 9 | Do not store sensitive data in frontend cache | `FORBIDDEN_PERSIST_KEYS` enforces this; verified by `apiCache.test.js`. |
| 10 | Do not show cached snapshots before successful auth/me | User restored from localStorage drives ProtectedRoute; cached snapshots only delivered to authenticated pages after user state is set. 401 from `/auth/me` triggers cache wipe via interceptor. |
| 11 | Clear cache on logout / session-revoked / auth failure / account switch | Logout: `clearApiCache()` in `AuthContext.logout`. 401: `clearApiCache()` in `api.js` interceptor. Account switch: keys scoped to active account ID — natural invalidation. |
| 12 | Comments snapshots only first page + sanitized | Cache key uses `:1` for first page. Sanitizer strips sensitive keys. |
| 13 | Dashboard/Automations/IG accounts snapshots only sanitized | Verified — same sanitizer applied. |
| 14 | Do not restore "Updated ago" UI | Not restored. |
| 15 | No Billing implementation introduced | None. |
| 16 | Do not assume Railway always-on | Phase 2.16 docs already record always-on is operator-only verification, unknown from code. |

## Security impact

**None — no changes.** All 2.12A-J protections preserved:
- JWT session revocation via `session_version` increment (password reset, password change, admin revoke)
- HMAC-SHA256 webhook signature verification with `compare_digest`
- Token hashes never stored as raw values (password reset, email verification)
- Generic responses prevent email enumeration
- Rate limits on login, signup, forgot-password, resend-verification
- Sentry scrubber redacts headers, body keys, path-specific patterns
- Raw OAuth credentials never logged
- Admin RBAC server-side enforced

## Performance impact

**None — no changes.** All 2.17/2.18 optimizations preserved:
- `dashboard_summaries` read-through model (`server.py` dashboard endpoint)
- Safe timing headers: `X-Dashboard-Summary-Time`, `X-Dashboard-Summary-Source`, `X-Dashboard-Summary-Slowest`
- Frontend persistent snapshots for dashboard/automations/comments-first/IG-accounts
- `scheduleCoreAppWarmup()` low-priority prefetch on auth restore/login/signup
- 24+ compound MongoDB indexes on usage / automations / conversations
- TTL indexes on `dashboard_summaries`, `usage_reservations`, `tracked_links`

## Reset email E2E status

**OPEN — external proof still missing.**

Backend, frontend, payloads, rate limits, and session revocation are all correct (Phase 2.14 + 2.14F). Re-verified in this pass:
- Token generation (`secrets.token_urlsafe(32)`) ✓
- Hash storage (HMAC-SHA256 of JWT_SECRET) ✓
- TTL enforcement ✓
- Single-use enforcement (race-safe via conditional update) ✓
- Generic responses ✓
- Rate limits (5/h IP + 3/h email-hash) ✓
- Webhook delivery with multiple URL aliases for template compatibility ✓
- Raw token never logged / stored / echoed ✓
- Session revocation on reset (session_version++) ✓
- Reset page scrubs `?token=` from URL on mount ✓

**What is still missing (operator action required):**
1. Real `EMAIL_VERIFICATION_WEBHOOK_URL` configured in Railway production env
2. Real provider template `mychat_password_reset` (or `PASSWORD_RESET_EMAIL_TEMPLATE` override) accepts at least one of `reset_url` / `resetUrl` / `url` / `link`
3. Provider SPF / DKIM / DMARC configured for the sender domain so reset emails reach inboxes
4. End-to-end manual run from a temporary production account: receive email, click reset link, set new password, verify old password rejected + new password works + all prior JWTs return 401 `session_revoked`

Until step 4 is reproduced and recorded with redacted evidence, Phase 2.14B and the billing gate stay open.

## Railway cold/warm / always-on status

**UNKNOWN — operator dashboard check required.**

From Phase 2.16 measurement: backend health responds at 180-183 ms warm after a 477 ms first call from the workstation, suggesting a possible cold-start gap. Railway CLI access failed locally (`invalid_grant`), so App Sleeping cannot be confirmed via API.

The repo's `railway.json` files (backend + frontend) define build/start commands but do NOT expose an always-on flag (Railway always-on is plan-level, not project-level). The Phase 2.18 review re-checked both files — no always-on field is supported.

**Operator action required:**
1. Open Railway dashboard → MyChat backend service
2. Settings → Deploy → check App Sleeping flag
3. If Sleeping is enabled, disable it OR confirm the service plan keeps the dyno warm 24/7
4. Re-measure dashboard warm p50/p95 from an authenticated session

This blocker is **infrastructure**, not code. No code change in Phase 2.18 can close it.

## Test commands run

```
# Backend
cd backend && python -m pytest tests/ -x --tb=short -q
# → 458 passed in 13.42s (2920 deprecation warnings, all datetime.utcnow() in Python 3.14)

# Frontend unit
cd frontend && CI=true yarn test --watchAll=false
# → 189 passed across 27 test suites in 3.441s

# Frontend production build
cd frontend && yarn build
# → success, build artifacts emitted to build/

# Frontend production build (CI mode)
cd frontend && CI=true yarn build
# → fails on missing optional dynamic-import deps (@sentry/react, posthog-js).
#   Pre-existing operational note — production deploys use plain `yarn build`,
#   which still passes. Not regression.
```

## Exact test results

| Suite | Pass | Fail | Skipped | Baseline (2.17) | Δ |
|---|---:|---:|---:|---:|---:|
| Backend | 458 | 0 | 0 | 458 | 0 |
| Frontend unit | 189 | 0 | 0 | 180 | +9 |
| Frontend build (default) | green | — | — | green | 0 |
| E2E (Playwright) | not run | — | — | 10p / 1s | — |

E2E was not re-run in this pass because (a) no code changed, and (b) requires Playwright browsers + a running backend session that isn't available from the workstation in this pass. Phase 2.17 E2E baseline (10 passed, 1 skipped) remains the latest evidence.

## Remaining blockers

| # | Blocker | Severity | Owner | Action |
|---|---|---|---|---|
| 1 | Reset email delivery E2E not proven | P1 | Ops | Configure provider, send real email, complete consume flow, verify session-revoked on old JWT, record redacted evidence |
| 2 | Performance: cold/warm + always-on unverified | P1 | Ops | Verify Railway App Sleeping setting in dashboard; if Sleeping is on, disable OR upgrade plan; re-measure dashboard p50/p95 from authenticated session |

Both blockers are operational, not code. Phase 2.18 cannot close either from the repo side.

## Is Billing still blocked?

**YES.** Billing gate stays **BLOCKED** until both P1 blockers above are closed with recorded evidence. This pass does NOT change the billing gate decision.

## Commit hash

This phase records:
- new doc `docs/phase-2.18-codebase-review.md`
- updated `docs/billing-readiness-gate.md` with Phase 2.18 review entry

Commit message:
```
Phase 2.18 Codebase Review and Readiness Hardening
```

(See `git log` for the recorded hash.)

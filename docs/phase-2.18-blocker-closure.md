# Phase 2.18 — Blocker Evidence and Build Hardening

Date: 2026-05-14
Builds on: `2eff184` (Phase 2.18 Codebase Review)

## Executive summary

This pass closes two operational tasks and produces evidence for the two open P1 blockers:

| Item | Before | After |
|---|---|---|
| `CI=true yarn build` | failed on missing optional SDKs | **PASSES** (craco IgnorePlugin gates absent deps) |
| Default `yarn build` | passed | still passes |
| E2E suite | not re-run in `2eff184` | **10 passed, 1 skipped** (matches 2.17 baseline) |
| Backend pytest | 458/458 | 458/458 |
| Frontend jest | 189/189 | 189/189 |
| Reset email E2E (P1 #1) | OPEN | **STILL OPEN** (external email + operator account required) |
| Performance proof (P1 #2) | OPEN | **STILL OPEN** (operator session + dashboard verification required), but backend `X-Response-Time: 0` measured — no observable cold start |

Billing remains **BLOCKED**. Both P1 blockers require operator/external action that cannot be performed from the codebase.

## Files changed in this pass

1. `frontend/craco.config.js` — Phase 2.18B IgnorePlugin for missing optional SDKs (build hardening)
2. `backend/scripts/measure_production_timings.py` — **new** production-safe timing probe (no secrets, env-driven, redacts tokens)
3. `docs/phase-2.18-blocker-closure.md` — **new** (this file)
4. `docs/billing-readiness-gate.md` — appended Phase 2.18 evidence entry

## A. E2E result

```
cd frontend && yarn test:e2e --project=chromium --reporter=line
```

Result: **10 passed, 1 skipped (16.9s)**

- 4 / 4 `auth-smoke` (login, signup, forgot+reset, data-deletion)
- 5 / 5 `product-smoke` (dashboard, automations, comments, billing-placeholder, settings password fields)
- 1 / 1 `admin-smoke` (overview, users, detail, admins, metrics)
- 1 skipped: `operator-auth-smoke` — requires `E2E_BASE_URL` + `E2E_EMAIL` + `E2E_PASSWORD` env vars (deliberate production smoke gate; never commits operator credentials)

Playwright browsers were already installed in `%LOCALAPPDATA%\ms-playwright`. The CRA dev server was auto-started by Playwright `webServer` config (port 3100). Mocked API responses cover all non-operator paths so no real credentials were required for these 10 tests.

## B. `CI=true yarn build` fix

### Root cause

`src/lib/sentryClient.js` and `src/lib/analytics.js` use dynamic `import('@sentry/react' | 'posthog-js').catch(() => null)` so the runtime is no-op if the SDKs are absent. Both SDKs are intentional optional dependencies and are NOT in `package.json` so production bundles stay small when no DSN/key is configured.

However, webpack still tries to statically resolve the import strings at build time. CRA's `react-scripts` honors `CI=true` by treating any `Module not found` warning as an error, so `CI=true yarn build` failed:

```
Module not found: Error: Can't resolve 'posthog-js'
Module not found: Error: Can't resolve '@sentry/react'
```

The default `yarn build` (without `CI=true`) only emitted warnings and produced a working bundle. Railway's deploy uses the default build, so production was unaffected.

### Fix

`frontend/craco.config.js` now resolves each optional dep against `node_modules` at config time. If the dep is missing, an `webpack.IgnorePlugin` is added only for those missing resources, so:

- **If the SDK is installed**: webpack bundles it as a lazy chunk; runtime `import()` resolves normally.
- **If the SDK is missing**: webpack ignores the import; runtime `import()` rejects; the existing `.catch(() => null)` keeps the app working with no-op analytics/Sentry.

A one-line `[craco] optional runtime deps not installed; webpack will ignore: ...` warning is printed to build logs so the decision is observable.

### Verification

```
cd frontend && CI=true yarn build      # ✅ PASS (was failing)
cd frontend && yarn build              # ✅ PASS (unchanged)
cd frontend && CI=true yarn test --watchAll=false   # ✅ 189/189 PASS
```

This fix does NOT hide a real dependency error: if a NON-optional dep goes missing, it will still fail because only the `OPTIONAL_RUNTIME_DEPS` list is gated.

## C. Reset email delivery E2E — STILL OPEN

### What is verified (code side)

- `POST /api/auth/forgot-password` is live in production; probed from this workstation:
  ```
  POST https://backend-production-a1a3.up.railway.app/api/auth/forgot-password
  body: {"email":"phase218-noexist-probe@example.com"}
  → 200 {"ok":true,"status":"sent_if_account_exists"}  (920 ms)
  ```
  Generic response shape preserved — no enumeration leak. This was a non-existent address (no real user created or contacted).
- Token primitive verified in `Phase 2.18 Codebase Review` doc: cryptographically random raw, only HMAC-SHA256 hash stored, single-use, TTL-bounded, never echoed or logged.
- Webhook payload includes 4 link aliases (`reset_url`, `resetUrl`, `url`, `link`) for provider-template compatibility (Phase 2.14F).
- Session revocation on reset (session_version++) was verified by code review in Phase 2.18.

### What is NOT verified (external, blocks gate closure)

A real reset email must arrive at a real inbox, be consumed, and a full state-change must be observed end-to-end. This pass cannot perform that test from inside the repository because:

| Missing piece | Why this pass cannot supply it |
|---|---|
| Real email-provider account with mailbox access | Repo cannot hold operator email credentials; no public test mailbox exists |
| `EMAIL_VERIFICATION_WEBHOOK_URL` set on Railway production env | Operator dashboard action; not in repo |
| `PASSWORD_RESET_EMAIL_TEMPLATE` (or default `mychat_password_reset`) configured at provider | Provider-side template; not in repo |
| Provider SPF / DKIM / DMARC | DNS-level; not in repo |
| A temporary test user on production that can receive and discard reset emails | Operator must create; storing such an account in repo is forbidden |

### Exact manual verification checklist (operator action)

1. In Railway → backend service → Variables, confirm: `EMAIL_VERIFICATION_WEBHOOK_URL`, `EMAIL_VERIFICATION_WEBHOOK_TOKEN` (optional), `PASSWORD_RESET_EMAIL_TEMPLATE` (optional), `PASSWORD_RESET_TOKEN_TTL_HOURS` (optional), `FRONTEND_URL` are all set with real values.
2. From a real inbox the operator controls, sign up for a temporary production account with a known password A.
3. Sign out and submit the forgot-password form with that account's email.
4. Verify a real reset email is received (check spam, headers should show SPF/DKIM pass).
5. Click the reset link → set new password B → submit.
6. Verify the reset page redirects to `/login`.
7. Log in with old password A → expect 401 invalid credentials.
8. Log in with new password B → expect success.
9. Take any JWT issued before the reset and call `/api/auth/me` with it → expect 401 `session_revoked`.
10. Replay the same reset token → expect 400 `password_reset_token_used`.
11. Record redacted evidence in `docs/auth-recovery-notes.md` (timestamp, status codes, no raw tokens / passwords / email bodies).

Until step 4 is recorded with redacted evidence, this blocker stays **OPEN**.

## D. Performance proof — STILL OPEN (with evidence collected)

### Backend cold/warm probe — production

```
MYCHAT_BACKEND_URL=https://backend-production-a1a3.up.railway.app \
python backend/scripts/measure_production_timings.py
```

(Run 2026-05-14T13:21:34Z from the workstation. No auth token; authenticated probes were skipped.)

| Probe | Samples | Status | min | p50 | p95 | max |
|---|---:|---:|---:|---:|---:|---:|
| health_first_call_cold | 1 | 200 | 481 | 481 | 481 | 481 |
| health_warm | 8 | 200 | 240 | 394 | 405 | 407 |
| plans_public_warm | 5 | 200 | 254 | 396 | 660 | 1033 |
| auth_google_config_warm | 5 | 200 | 271 | 399 | 400 | 416 |
| auth_me_warm | 0 | skipped | — | — | — | — |
| dashboard_summary_warm | 0 | skipped | — | — | — | — |
| automations_summary_warm | 0 | skipped | — | — | — | — |
| instagram_accounts_warm | 0 | skipped | — | — | — | — |

### Server-side timing

A `curl -v https://backend-production-a1a3.up.railway.app/api/` from the same workstation returns header:

```
X-Response-Time: 0
X-Railway-Edge: railway/europe-west4-drams3a
```

**Server-side processing rounds to <1 ms.** The 400-500 ms client-side total is therefore network latency + TLS handshake + (observed) one TLS renegotiation, not backend processing time. There is **no observable cold-start** in this 5-call probe window (cold 481 ms vs warm p50 394 ms is within the network noise band).

### What this proves

- Backend is not running into Railway sleep within the timing window of this probe.
- Authenticated dashboard timing is the actual gate metric and was **not measured** because that requires an operator session token.
- Public endpoints all responded in p95 ≤ 660 ms client-side from a transatlantic workstation. One 1033 ms outlier on `/api/plans` is within the documented p95 ≤ 1200 ms budget but should be checked under an operator-authenticated probe.

### What is needed to close

| Missing piece | Action |
|---|---|
| Operator JWT for production | Operator generates a short-lived token in a private session, exports `MYCHAT_AUTH_TOKEN` in the shell, runs `python backend/scripts/measure_production_timings.py`, copies the resulting table into `docs/dashboard-performance-notes.md`. **Never commit the token.** |
| p50 ≤ 500 ms / p95 ≤ 1200 ms target | Determined by the same probe with `dashboard_summary_warm`, `automations_summary_warm`, `instagram_accounts_warm`, `auth_me_warm` lines populated. |
| Cold-start re-check | Operator forces a deploy or service restart, immediately re-runs the probe. If `health_first_call_cold` jumps significantly above warm, App Sleeping is likely on. |

### Script properties (security)

- Reads `MYCHAT_BACKEND_URL` and `MYCHAT_AUTH_TOKEN` from env vars only.
- Never echoes the token to stdout or stderr — only `<set, len=N>` is logged.
- Never writes credentials to a file.
- Outputs a markdown timing table + a JSON dump for CI ingestion.
- 15 ms inter-call spacing so it does not stress the host.

## E. Railway always-on / sleeping — UNKNOWN (operator dashboard action required)

### What the repo says

- `backend/railway.json`: defines `healthcheckPath: /api/`, `restartPolicyType: ON_FAILURE`, max 3 retries. **No always-on / sleep flag** — Railway does not expose one in the railway.json schema; sleep is a service-level dashboard setting.
- `frontend/railway.json`: defines build + start commands only.
- `backend/Procfile`: `web: uvicorn server:app --host 0.0.0.0 --port ${PORT:-8001}` (no sleep config).
- `frontend/Procfile`: `web: npx serve -s build -l ${PORT:-3000}`.
- `frontend/nixpacks.toml`: install/build/start commands; no sleep config.

No file in the repo can turn always-on on or off. **It is a Railway dashboard setting**.

### From the timing probe

Cold (first call) 481 ms vs warm p50 394 ms is an 87 ms delta. With `X-Response-Time: 0` on the server, this 87 ms is well within network noise (TLS renegotiation alone took noticeable time in the curl verbose trace). This is **consistent with always-on, not with sleeping**. But: a single 5-call probe is not strong evidence — the service could have been warmed by an earlier user or healthcheck.

### Operator action required to close

1. Open Railway dashboard → MyChat backend service.
2. Settings → Deploy → look for `App Sleeping` toggle (or in the Service Settings).
3. If the toggle is visible and ON: turn it OFF (or upgrade to a plan that does not sleep) and redeploy.
4. If the toggle is not exposed at all: document that the current plan does not surface always-on, and note that warm timings should still be measured authenticated.
5. Record finding in `docs/production-infrastructure-performance.md`.

This blocker stays **UNKNOWN** until a screenshot or written confirmation of the Sleep setting is recorded.

## F. Tests run in this pass — full results

| Suite | Command | Result |
|---|---|---|
| Backend pytest | `cd backend && python -m pytest tests/ --tb=short -q` | **458 passed** in 12.73s |
| Frontend unit | `cd frontend && CI=true yarn test --watchAll=false` | **189 passed** across 27 suites in 3.40s |
| Frontend default build | `cd frontend && yarn build` | **PASS** in 8.19s |
| Frontend CI build | `cd frontend && CI=true yarn build` | **PASS** in 8.51s (was failing before B fix) |
| Frontend E2E | `cd frontend && yarn test:e2e --project=chromium --reporter=line` | **10 passed, 1 skipped** in 16.9s (skipped = operator-auth-smoke, requires production credentials) |
| Production health probe | `python backend/scripts/measure_production_timings.py` | health: cold 481 ms, warm p50 394 ms, p95 405 ms |

## G. Remaining blockers

| # | Blocker | Severity | Owner | Action |
|---|---|---|---|---|
| 1 | Reset email delivery E2E not proven | P1 | Operator | Run the 11-step manual verification checklist in section C; record redacted evidence in `docs/auth-recovery-notes.md` |
| 2 | Authenticated dashboard p50/p95 unmeasured | P1 | Operator | Export operator token to env, run `backend/scripts/measure_production_timings.py`, paste result in `docs/dashboard-performance-notes.md` |
| 3 | Railway App Sleeping setting unverified | P1 (related to #2) | Operator | Check Railway dashboard, record in `docs/production-infrastructure-performance.md` |

## H. Billing status

**STILL BLOCKED.** This pass:
- closes one operational dev task (CI=true build now passes)
- re-verifies E2E baseline (10 passed, 1 skipped)
- ships a production-safe timing script ready for operator use
- collects unauthenticated production-timing evidence that is consistent with a non-sleeping backend, but cannot prove it

The billing gate requires **operator action** to:
1. Reproduce the full reset-email flow on a real production account, and
2. Run the timing probe with an operator token from a typical user location.

No code change in this pass moves Billing closer to ready beyond the build-hardening fix.

## I. Push and deploy status

This pass adds two commits-worth of changes (build fix + scripts + docs) on top of `2eff184` (Phase 2.18 Codebase Review). Both Phase 2.18 commits live **only on the local `master` branch** at the time of writing:

- Local HEAD: `2eff184a4a3c1a173daab1fbc491911e2c21cefa` before this pass
- Remote `origin/master`: `cd067f2222654bf0383b6f4a10f4b17a3273dc99` (Phase 2.17)
- Local ahead by 1 commit before this pass; after committing this pass, local will be ahead by 2 commits

**Nothing has been pushed.** Railway production therefore reflects only `cd067f2` (Phase 2.17). The CI=true build fix and the timing script are NOT live until an operator pushes `master` to `origin`. Operator decision: push when ready; deploy will then pick up the new commits automatically.

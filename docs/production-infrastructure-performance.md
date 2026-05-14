# Production Infrastructure Performance

Generated: 2026-05-14

Phase 2.16 treats production performance as a measured gate, not a visual polish task. The backend can only meet authenticated-dashboard targets consistently if the production service is warm when users arrive.

## Always-On Requirement

MyChat should run the production backend as an always-on Railway service before Billing. Railway App Sleeping or a sleeping-equivalent plan can add first-request latency that code-level dashboard optimization cannot fully remove.

Required operator check:

1. Open Railway.
2. Open the MyChat backend service.
3. Confirm the production environment and service attached to `https://backend-production-a1a3.up.railway.app`.
4. Check Settings / Deploy / App Sleeping.
5. Disable App Sleeping if it is enabled.
6. Confirm the service plan keeps the backend running without sleeping.
7. Confirm healthcheck path is `/api/`.
8. Redeploy the backend.
9. Measure first `GET /api/`, immediate second `GET /api/`, and authenticated `/api/dashboard/summary`.

Cron or external ping keep-warm jobs are temporary mitigation only. They are not the final production answer for a billing SaaS.

## Phase 2.16 Evidence

Unauthenticated backend health checks from the workstation:

| Attempt | Endpoint | Status | Duration | X-Response-Time |
|---:|---|---:|---:|---:|
| 1 | `GET /api/` | 200 | 477 ms | 0 ms |
| 2 | `GET /api/` | 200 | 180 ms | 0 ms |
| 3 | `GET /api/` | 200 | 183 ms | 0 ms |

This shows the backend can respond quickly once warm. It does not prove App Sleeping is disabled, because Railway dashboard access was unavailable from the local CLI (`invalid_grant`).

## Dashboard Measurement Contract

`GET /api/dashboard/summary` now exposes safe timing diagnostics and read-through snapshot source:

- `X-Dashboard-Summary-Time`: total dashboard summary duration in milliseconds.
- `X-Dashboard-Summary-Slowest`: slowest measured backend section name.
- `X-Dashboard-Summary-Source`: `rebuilt`, `read_model`, `stale_read_model`, `stale_fallback`, or `live_fallback`.

Safe backend log event:

- `dashboard_summary_breakdown`
- includes section timings and bounded document counts.
- excludes tokens, passwords, emails, raw comments, raw DMs, raw provider bodies, and raw webhook bodies.

Frontend safe perf log:

```text
[perf] api GET /dashboard/summary client=<ms> backend=<ms> dashboard=<ms> source=<source> slowest=<section> status=<status>
```

This log intentionally contains route and timing metadata only.

## Phase 2.18 Frontend Snapshot Layer

The frontend now has a browser-persistent, opt-in snapshot layer for safe app data. It is designed to remove the refresh/new-tab penalty caused by memory-only cache:

- dashboard summary: 60s fresh TTL, 5m max stale.
- automations summary/list: 90s fresh TTL, 5m max stale.
- comments first page/filter: 20s fresh TTL, 2m max stale.
- Instagram accounts: 180s fresh TTL, 10m max stale.

The cache persists only successful JSON responses for allowlisted user/account-scoped keys. It clears on logout and matching invalidations, and it does not persist tokens, passwords, reset tokens, secrets, raw provider payloads, 401/403 responses, 5xx responses, or HTML error pages.

This improves perceived first-load UX after browser Refresh, but it does not replace the always-on backend requirement. If the backend container sleeps, background refresh and any uncached route still pay cold-start latency.

## Production Measurement Procedure

After deploy:

1. Log in with a temporary production test account.
2. Hard reload `/app`.
3. Capture the first dashboard console perf line.
4. Refresh or navigate away/back and capture two warm dashboard perf lines.
5. Record `client`, `backend`, `dashboard`, `source`, and `slowest`.
6. Confirm the second/warm dashboard request uses `read_model` or stays within the documented backend budget.
7. If dashboard warm is within budget but first request after idle is slow, disable App Sleeping / move backend to always-on.

## Decision Rule

- Warm dashboard within budget and cold request slow: infrastructure action required.
- Warm dashboard slow: dashboard read-model work required.
- Backend fast but route navigation slow: frontend render/query work required.

Billing remains blocked until the auth recovery email E2E is closed and the production performance gate is either measured green or explicitly accepted with an always-on infrastructure action.

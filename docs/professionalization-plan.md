# Phase 2.17 Professionalization Plan

Generated: 2026-05-14

## Current State

- Branch: `master`
- Starting commit: `ce728fe44c46b7d179619553d50130a2a330e7e4`
- Billing: blocked
- Security 2.12A-J: closed
- Password change E2E: passed
- Password reset email E2E: blocked until a real reset email is received and consumed
- Performance: dashboard snapshot/read-through cache implemented locally; production proof still required after deploy

## Agent Assignments

- Agent A: baseline, integration, tests, docs, final report.
- Agent B: frontend cache, routes, fake-success, password visibility.
- Agent C: backend dashboard read model, invalidation, Mongo/Railway performance.
- Agent D: auth recovery, email delivery, password/session security.
- Agent E: endpoint security, RBAC, logging, webhook hardening.
- Agent F: tests, E2E, production verification, billing gate.

## Findings Summary

| ID | Severity | Area | Status |
|---|---|---|---|
| P1-AUTH-EMAIL | P1 | Reset email delivery E2E not complete | open, billing blocker |
| P1-SENTRY-PASSWORD | P1 | Password-bearing auth bodies not fully scrubbed from optional Sentry events | fixed |
| P1-DASH-PROJECTION | P1 | Dashboard projections omitted account fields needed for active-account filtering | fixed |
| P1-WEBHOOK-HMAC | P1 | Instagram webhook HMAC enforcement defaulted to warn-only | fixed |
| P1-ADMIN-HARD-DELETE | P1 | Legacy admin delete route was ordinary-user accessible | fixed |
| P1-DASH-REFRESH | P1 | Dashboard manual refresh failure could throw due scoped error helper | fixed |
| P1-PERF-PROOF | P1 | Production authenticated performance proof still pending | open, billing blocker |

## Fixes Implemented

- Dashboard read-through snapshots use `dashboard_summaries` with fresh/stale windows and safe timing headers.
- Dashboard projections preserve account identifiers so active-account filtering remains correct.
- Usage events invalidate dashboard summaries for the affected user/account/month.
- Frontend SWR cache no longer falls back to expired stale cache for foreground refresh failures.
- Dashboard, Automations, and Comments keep cached data visible during background refresh.
- Comments status filter no longer fragments the API cache for identical backend requests.
- Optional Sentry scrubber now drops password-bearing auth request bodies and password keys.
- Settings password change now enforces the same backend minimum password length as reset.
- Instagram webhook HMAC enforcement is on by default unless explicitly disabled for local testing.
- Legacy `/api/admin/users/{email}` hard delete is admin-gated and limited to disposable test accounts.

## Deferred Items

| Item | Risk | Billing blocker |
|---|---|---:|
| Real reset email delivery E2E | P1 account recovery | yes |
| Railway always-on confirmation | P1 first-load performance | yes until measured/accepted |
| Authenticated dashboard timing after deploy | P1 performance proof | yes |
| Canonical ID field migration | P2 data model consistency | no, if legacy fallback remains |
| Full broad-exception cleanup | P2 diagnostics | no |

## Production Verification Status

Not closed in this document. Production must be verified after commit/push/deploy using browser-render checks, backend health, protected endpoint checks, live JS chunk search, and authenticated dashboard timing if credentials are available.

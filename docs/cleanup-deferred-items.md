# Cleanup Deferred Items

Generated: 2026-05-14

These items were intentionally deferred from Phase 2.17 because they are not safe to rush into the performance/security patch set.

| Item | Reason deferred | Risk | Billing blocker |
|---|---|---|---:|
| Canonical field migration | Requires diagnostics, backfill, and observation before removing legacy fallbacks | P2 query complexity | no |
| Full broad-exception cleanup | Needs endpoint-by-endpoint behavior review to avoid changing public error contracts | P2 diagnostics | no |
| Diagnostic DM log redaction | Some user-scoped diagnostic endpoints still expose raw message previews; needs product decision on support visibility | P2 privacy surface | no, if not public/admin-leaked |
| Provider-send reservation release audit | Existing tests cover major release paths, but all provider failure branches should be reviewed | P2 temporary false limits | no |
| Authenticated performance E2E thresholds | Requires production operator credentials/session | P1 performance proof | yes |
| Real password reset email E2E | Requires provider deliverability and inbox/reset-link consumption | P1 account recovery | yes |

# Known Good Versions

Use this file before rollback or comparison work. A known-good version is not always perfect; it is a verified reference point.

| Commit SHA | What was working | What was not working | Why considered safe | Verification evidence | Rollback notes |
|---|---|---|---|---|---|
| `d30f04e155828c61c85713142c10a6e7888a90d8` | Backend health, OAuth reused-code protection, frontend cache behavior for media failures, diagnostics UI still absent. | Instagram automation needs fresh live verification after deploy; Meta token issues may still require reconnect if Graph returns code 190. | Latest deployed commit with full backend/frontend tests passing and production build_sha verified. | `/api/version` returned `d30f04e15582`; `/api/` returned ok; backend 622 passed; frontend 184 passed; build passed. | Prefer forward fix unless this commit is directly tied to a regression. |
| `8ebe8c6b6b40b631bd4c1a7e0678589a4c5201a5` | Backend health, OAuth idempotency guard, nonblocking startup bootstrap. | Did not handle Meta returning "authorization code has been used" outside local consumed-code guard; media picker could show internal cache error after `ok=false`. | Deployed healthy state before `d30f04e`; no intended automation flow change. | `/api/version` previously returned `8ebe8c6b6b40`; backend tests reported 621 passed. | Safe comparison point if `d30f04e` suspected, but it has known OAuth/media UX gaps. |
| `d547203` | Restored event-scoped token context and media cache guard. | Did not include later OAuth idempotency and startup bootstrap hardening. | Important recovery point after risky stored-token preflight in `3500dcd`. | Backend 614 passed; frontend cache 17 passed; build passed. | Use as token-flow comparison baseline. |
| `23e9f68` | Webhook secret source resolution hardened and local dirty work had been reverted. | Later dedupe/token/media fixes absent. | Historical structural baseline after unsafe dirty diff was removed. | Production build_sha was verified at the time; backend health ok. | Use only if later Instagram flow changes need forensic comparison. |

## Rollback Reminder

Prefer `git revert <bad_commit>` over resetting history. Never rollback production without checking whether the target commit lacks security or data-integrity fixes.

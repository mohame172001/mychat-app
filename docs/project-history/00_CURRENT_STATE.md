# Current State

Last updated by: Claude Code  
Last updated date: 2026-05-24

## Project

Project name: MyChat

Stack:

- Backend: FastAPI, MongoDB
- Frontend: React, CRA, CRACO
- Hosting: Railway
- Integrations: Instagram Business Login, Instagram Graph API, webhooks, Sentry

## Production URLs

- Frontend: https://frontend-production-6eb2.up.railway.app
- Backend: https://backend-production-a1a3.up.railway.app
- Backend health: https://backend-production-a1a3.up.railway.app/api/
- Backend version: https://backend-production-a1a3.up.railway.app/api/version

## Git And Deploy State

- Current local app-code fix: pending unknown_state-reprocess fix
- Current local HEAD: pending dedupe-reprocess commit
- Current origin/master: `dae71ebd7e0141f75998d0001010fbd3cee42b24`
- Current production build_sha: `dae71ebd7e01`
- Current working status: Diagnosing muhammad_gehad regression. The admin panels (clarified in `dae71eb`) revealed the real root cause: `_handle_new_comment`'s catch-all `else` block at server.py:17426 always returned an `automation_skipped` with `skip_reason='unknown_state'` whenever the existing comment doc carried NO known processed-state signal (no `reply_status=success`, no `dm_status=success`, no `replied` proof, no historical / partial-success / dm-failed / pending). For a polling cycle that wrote a seen-only doc, this incorrectly blocked rule loading forever. Fix in progress: when the catch-all cannot classify the existing doc as a known processed state, set `retry_existing=True` and continue to rule loading. Downstream `opening_dedupe_key` + provider-proof checks still independently prevent any duplicate send. New visibility event `existing_comment_unknown_state_reprocess` records the decision. Account-agnostic. No rule-matching, send, dedupe, HMAC, or rate-limit change.

Update these values at the start and end of every agent session.

## Confirmed Working

- Backend `/api/` health returns `ok`.
- Backend `/api/version` reports the deployed build SHA.
- Frontend public SPA routes return 200.
- `/app/admin/instagram-diagnostics` is not exposed as a frontend diagnostics UI.
- Protected backend admin support endpoints return 403 unauthenticated.
- Full backend suite passed at commit `d30f04e`: 622 tests.
- Full frontend suite passed at commit `d30f04e`: 184 tests.
- Frontend production build passed at commit `d30f04e`.
- Local backend suite passed after the stop-point summary accuracy fix: 623 tests.
- Local backend suite passed after the legacy general any-post matching fix: 627 tests.

## Current Blockers

- Billing remains blocked.
- Auth recovery email delivery E2E remains a billing blocker until proven end to end.
- Instagram automation still requires live verification after every production deploy because Meta behavior cannot be fully proven by unit tests.
- Instagram comment automation is currently observed through polling in the operator stop-point page; instant webhook delivery still needs protected endpoint/log confirmation and may depend on Meta Advanced Access/subscription state.
- Legacy general/any-post rule matching fix still requires production deploy and live retest before it can be considered known-good.
- Railway always-on status must be confirmed before billing.

## Do-Not-Touch Constraints

- Do not start Billing.
- Do not weaken webhook HMAC verification.
- Do not remove dedupe.
- Do not remove rate limits or anti-spam send pacing.
- Do not reintroduce `/app/admin/instagram-diagnostics`.
- Do not log tokens, secrets, authorization headers, full webhook payloads, full DM bodies, or passwords.
- Do not remove legacy account fallback logic without a tested migration.
- Do not use `users.active_instagram_account_id` for webhook execution.

## Latest Verification Commands

```powershell
git status --short
git rev-parse HEAD
git rev-parse origin/master
Invoke-WebRequest -Uri "https://backend-production-a1a3.up.railway.app/api/version" -UseBasicParsing
Invoke-WebRequest -Uri "https://backend-production-a1a3.up.railway.app/api/" -UseBasicParsing
python -m py_compile backend/server.py
python -m pytest backend/tests -x --tb=short -q
cd frontend
npm.cmd test -- --watchAll=false
npm.cmd run build
```

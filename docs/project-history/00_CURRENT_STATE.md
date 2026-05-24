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

- Current local app-code fix: `54fb527fb1fdb3eed0a676166d2ff7b85b38a206`
- Current local HEAD: pending inspector commit
- Current origin/master: `ff590dc76f75ce649059cd60f656b97d12a87d02` (pre-inspector)
- Current production build_sha: `ff590dc76f75`
- Current working status: legacy general matching fix `54fb527` deployed, but live retest still shows `rule_not_matched` on both linked accounts; adding protected backend-only `/api/admin/instagram/rule-coverage-inspector` for diagnosis. No automation logic change.

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

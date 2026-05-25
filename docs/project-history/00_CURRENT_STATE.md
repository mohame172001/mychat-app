# Current State

Last updated by: Codex
Last updated date: 2026-05-25

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

- Current local app-code fix: pending `fix(instagram): prevent fresh comments from being marked historical`.
- Current local HEAD: `75ec2a38638557e2a9bff7c70484feab95ea562f` before the pending Instagram historical-cutoff fix commit.
- Current origin/master: `75ec2a38638557e2a9bff7c70484feab95ea562f` before the pending Instagram historical-cutoff fix push.
- Current production build_sha: `75ec2a386385` before the pending Instagram historical-cutoff fix deploy.
- Current working status: Instagram polling comment handling fix is pending commit/deploy. It keeps real Graph timestamps authoritative for old comments, but if polling sees a comment with no Graph timestamp it uses first-seen time instead of skipping forever as missing/historical. Existing comment docs with stale `historical_before_rule_activation` skips are reprocessed only when the current payload proves the comment is at/after the stored activation cutoff. Stop Point now surfaces latest external comment partial id/media id/timestamp/activation-cutoff details. Separately, frontend-only dashboard polish landed: chart title now range-aware ("Performance — Last 24 hours" / "Last 7 days" / "Last 30 days" / "All time"); tooltip and axis labels now force the app UI language so English users no longer see Arabic month order from the browser locale; More stats becomes a chip-style chevron button with i18n labels; Total Contacts subtitle clarified to "All-time · Active account"; Top Automations sorts active rules first and visually de-emphasizes paused/draft rows. No Billing, HMAC, dedupe removal, rate-limit removal, frontend diagnostics route, dashboard backend behavior, or automation rule-scope change.

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
- Local multi-account automation routing suite passed after the dedupe-per-post and automatic text fallback patch: 131 tests.
- Local full backend suite passed after the dedupe-per-post and automatic text fallback patch: 674 tests.
- Local multi-account automation routing suite passed after the historical-cutoff polling fix: 135 tests.
- Local webhook timestamp fallback suite passed after the historical-cutoff polling fix: 7 tests.
- Local full backend suite passed after the historical-cutoff polling fix: 678 tests.

## Current Blockers

- Billing remains blocked.
- Auth recovery email delivery E2E remains a billing blocker until proven end to end.
- Instagram automation still requires live verification after every production deploy because Meta behavior cannot be fully proven by unit tests.
- Instagram comment automation is currently observed through polling in the operator stop-point page; instant webhook delivery still needs protected endpoint/log confirmation and may depend on Meta Advanced Access/subscription state.
- Dashboard simplification / x-axis label polish is deployed and still needs live operator UI confirmation before being considered known-good.
- Pending Instagram historical-cutoff fix must be deployed and live-tested before it can be considered known-good: same external commenter on a different eligible post should trigger again; exact duplicate should still skip; browser typed fallback and mobile quick reply should still work.
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

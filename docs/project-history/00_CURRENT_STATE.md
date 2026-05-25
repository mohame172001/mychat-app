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

- Current local app-code fix: pending admin diagnostic summary fix
- Current local HEAD: pending admin diagnostic clarity commit
- Current origin/master: `4ea7bef3e6298b0bb74610e9d09d4840f1b7e544`
- Current production build_sha: `4ea7bef3e629`
- Current working status: Cleaning up admin diagnostic panels so they reflect the known-good automation. (1) Stop Point summary: `account_resolved` and `polling_scanned_account` now broaden to any polling-stage event in the window so polling-only accounts no longer report False. (2) `unknown_state` / `rule_not_matched` resolution promotes `extra.classified_reason` and `early_exit_before_rule_loading` when `rule_loading_finished` never ran. (3) New summary flags `classified_reason`, `is_latest_event_rescan_of_processed`, `is_latest_event_historical`. (4) `next_recommended_action` covers the new reasons + the dedupe-skip family. (5) Multi-account-health gains per-account `last_comment_webhook_event_time`, `last_messaging_webhook_event_time`, `last_account_resolution_failed_at`, and a `webhook_delivery_status` label (comment_webhooks_received / comment_webhooks_observed_globally_not_mapped / polling_fallback_only). (6) Frontend Flight Recorder classifier no longer reports `silent_early_exit_possible` when a concrete skip reason exists; renders new badges for bot-own-reply / rescan / historical / unknown_state. (7) Webhook Health card renders the per-account delivery-status badge + the new timestamps. (8) Stop Point card shows `classified_reason` + state badges. No backend automation, rule-matching, webhook, polling, dedupe, HMAC, or rate-limit change. Known-good app code from `948a996` is preserved.

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

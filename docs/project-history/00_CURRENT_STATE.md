# Current State

Last updated by: Claude
Last updated date: 2026-05-26

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

- Current local app-code fix: pending `fix(instagram): widen polling target media + correct Stop Point decision matrix` (stacks on top of the still-pending unify-quick-reply patch). Implements full decision matrix: A) exact-comment provider-proof dedupe still skips, B) bot-own/self reply still skips, C) general any-post rule processes fresh comments after activation only, D) post-specific + selected-media match + process_existing_unreplied_comments=true processes previous unreplied comments and emits `historical_catchup_allowed` + `process_existing_unreplied_comment_processed`, E) post-specific + selected-media match + process_existing=false skips pre-activation, F) post-specific + selected-media mismatch returns `selected_media_no_match`, G) no fresh comment returned by polling → `no_fresh_comment_seen_in_poll`.
- Current local HEAD: `7aa4d52bac05e850dca6482d0a51364bdb4f3fcc`.
- Current origin/master: `7aa4d52bac05e850dca6482d0a51364bdb4f3fcc`.
- Current production build_sha: `7aa4d52bac05`.
- Current working status: Production is on `7aa4d52`, which includes the fresh-polling-priority automation fix. Current local pending work removes visible auto-appended browser/laptop fallback copy from outgoing Instagram DMs globally while preserving quick reply payloads and internal typed fallback continuation behind a pending session. No Billing, HMAC, dedupe removal, rate-limit removal, frontend diagnostics route, dashboard behavior, webhook/polling execution, or account-scope change.

Additional current-session note: `fix(instagram): unify quick reply behavior across accounts` is pending commit/deploy. Outgoing Instagram DMs no longer auto-append visible browser/laptop fallback instructions such as "If the button is not visible..." or visible manual-typing instructions. Quick reply payloads remain unchanged, and internal typed fallback remains available only when a valid pending session exists.

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
- Local multi-account automation routing suite passed after the fresh-polling-priority fix: 138 tests.
- Local full backend suite passed after the fresh-polling-priority fix: 681 tests.
- Local multi-account automation routing suite passed after removing visible quick-reply browser fallback copy: 139 tests.
- Local full backend suite passed after removing visible quick-reply browser fallback copy: 682 tests.
- Local multi-account automation routing suite passed after bumping polling target-media coverage to 25 and adding scan-started diagnostics: 140 tests.
- Local full backend suite passed after bumping polling target-media coverage to 25 and adding scan-started diagnostics: 683 tests.
- Local multi-account automation routing suite passed after Stop Point decision matrix overhaul: 144 tests.
- Local full backend suite passed after Stop Point decision matrix overhaul: 687 tests.

## Current Blockers

- Billing remains blocked.
- Auth recovery email delivery E2E remains a billing blocker until proven end to end.
- Instagram automation still requires live verification after every production deploy because Meta behavior cannot be fully proven by unit tests.
- Instagram comment automation is currently observed through polling in the operator stop-point page; instant webhook delivery still needs protected endpoint/log confirmation and may depend on Meta Advanced Access/subscription state.
- Dashboard simplification / x-axis label polish is deployed and still needs live operator UI confirmation before being considered known-good.
- Pending quick-reply parity fix must be deployed and live-tested before it can be considered known-good: outgoing opening DMs on all linked accounts should have the same creator-authored body format with no visible browser/laptop fallback instruction; quick reply buttons should remain present; internal typed fallback should remain scoped to a valid pending session.
- Pending polling-coverage + Stop-Point decision-matrix fix must be deployed and live-tested before it can be considered known-good: (a) a fresh comment on the 11th+ most recent post of any linked Instagram account should be discovered by the 15-second poller; Stop Point should show `target_media_count >= 11` and `fresh_comment_seen_in_last_poll=true`; (b) when polling has not yet seen a fresh comment, Stop Point should read `no_fresh_comment_seen_in_poll` rather than collapsing into `rule_not_matched` / `historical_before_rule_activation` / `already_replied_success`; (c) a post-specific rule with `process_existing_unreplied_comments=true` and a matching selected media should still process previous unreplied comments on that media and emit `process_existing_unreplied_comment_processed` on success.
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

# Current State

Last updated by: Codex
Last updated date: 2026-05-27

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

- Current local app-code fix override: pending `fix(instagram): prevent repeated opening DMs for same commenter`. Phase 2D keeps the existing opening-DM dedupe key shape (`user_id + instagram_account_id + automation_id/rule_id + media_id + commenter_id`) but separates public-reply-per-comment behavior from opening-DM cooldown behavior. Repeated comments from the same commenter on the same media/rule/account inside `COMMENT_DM_OPENING_DEDUPE_WINDOW_SECONDS` (default 86400, clamped 3600-604800) may still get public replies according to the existing comment-level policy, but the opening DM is skipped as `opening_dm_already_sent_for_commenter_media`. `COMMENT_DM_COMPLETED_FLOW_REOPEN_TTL_SECONDS` no longer reopens the opening DM inside the 24-hour cooldown. Different commenters and the same commenter on different media remain eligible under the existing policy. Billing, HMAC, dedupe semantics for exact comments, rate limits, quick reply copy, Dashboard/frontend, and username-specific routing remain unchanged.

- Current local app-code fix override: pending `fix(instagram): reduce polling noise and summarize scans`. This supersedes the older pending notes below for Phase 2/2B state. Phase 2C-A keeps polling recovery enabled, but filters provably historical/stale polling comments before `_handle_new_comment`, counts them in one `polling_scan_summary`, and updates Stop Point so old polling scans do not collapse into misleading `rule_not_matched`. Fresh polling comments, webhook comments, explicit selected-post catch-up, dedupe, HMAC, Billing, rate limits, quick reply copy, Dashboard/frontend, and account scoping remain unchanged.

- Current local app-code fix: pending `feat(instagram): media catalog + round-robin polling for full any-post coverage`. Stacks on top of the deployed Phase 1 SaaS hardening (`99ee2fc`). New `instagram_media_catalog` collection + `pollingRoundRobinCursor` on `instagram_accounts` + extended `_collect_target_media_ids` to compose recent + pinned + round-robin slices so `comment:any/post_scope=any` rules cover every post over time. Required before customer onboarding so a fresh comment on an older post is not silently invisible to polling. Billing remains blocked. Implements widened polling target-media coverage (10→25) + scan-started diagnostics + full Stop Point decision matrix: A) exact-comment provider-proof dedupe still skips, B) bot-own/self reply still skips, C) general any-post rule processes fresh comments after activation only, D) post-specific + selected-media match + process_existing_unreplied_comments=true processes previous unreplied comments and emits `historical_catchup_allowed` + `process_existing_unreplied_comment_processed`, E) post-specific + selected-media match + process_existing=false skips pre-activation, F) post-specific + selected-media mismatch returns `selected_media_no_match`, G) no fresh comment returned by polling → `no_fresh_comment_seen_in_poll`.
- Current local HEAD: `237e549d699c`.
- Current origin/master: `237e549d699c`.
- Current production build_sha: `237e549d699c`.
- Current working status: Production is on `237e549`; Phase 2B stale polling guard is live. Phase 2C-A polling-noise reduction is local and pending tests/deploy. No Billing, HMAC, dedupe removal, rate-limit removal, frontend diagnostics route, dashboard behavior, webhook execution, account-scope, or quick-reply-copy change.

Additional current-session note: `fix(instagram): unify quick reply behavior across accounts` is pending commit/deploy. Outgoing Instagram DMs no longer auto-append visible browser/laptop fallback instructions such as "If the button is not visible..." or visible manual-typing instructions. Quick reply payloads remain unchanged, and internal typed fallback remains available only when a valid pending session exists.

Additional current-session note: Phase 2B stale polling guard is deployed as `237e549`. Root cause: round-robin can first-see older comments on older media after rule activation and process them as if they were live. Live fix: polling-only guard skips Graph-timestamped comments older than `IG_POLL_FRESH_COMMENT_WINDOW_SECONDS` (default 3600, clamped 60-21600) unless explicit selected-post `process_existing_unreplied_comments=true` catch-up applies. Webhook events, exact-comment dedupe, HMAC, rate limits, quick reply copy, and account scoping remain unchanged.

Additional current-session note: Phase 2C-A is pending locally on top of deployed `237e549`. Root cause: `_poll_user_comments` still fed old polling rows into `_handle_new_comment`, causing `poller_comment_seen`, `dedupe_checked`, rule-loading, and historical skip traces for old comments every polling tick. Pending fix: prefilter polling-only historical/stale comments with provider timestamps before `_handle_new_comment`, emit one compact `polling_scan_summary`, and let Stop Point report `only_historical_or_stale_comments_seen` / `no_fresh_comment_seen_in_poll` instead of `rule_not_matched` when no fresh comment was scanned.

Additional current-session note: Phase 2D is pending locally. Root cause: the opening-DM cooldown could be effectively reopened by the shorter completed-flow reopen TTL, allowing a second opening DM to the same commenter on the same media/rule/account after repeated comments. Pending fix: `COMMENT_DM_OPENING_DEDUPE_WINDOW_SECONDS` defaults to 24 hours and gates opening DMs independently from comment-level public replies. Later comments inside the window record `opening_dm_already_sent_for_commenter_media` and skip only the opening DM.

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
- Local startup-bootstrap suite passed after adding instagram_automation_events TTL index: 6 tests (+3).
- Local full backend suite passed after adding instagram_automation_events TTL index: 690 tests (+3).
- Local Phase 1 SaaS hardening suite passed: 13 new tests in `test_phase1_saas_hardening.py`.
- Local full backend suite passed after Phase 1 SaaS hardening: 703 tests (+13).
- Local Phase 2 media-catalog + round-robin polling suite passed: 7 new tests in `test_phase2_media_catalog_round_robin.py`.
- Local full backend suite passed after Phase 2 media-catalog + round-robin polling: 710 tests (+7).
- Local multi-account automation routing suite passed after Phase 2B stale polling guard: 148 tests (+4).
- Local Phase 2 media-catalog + round-robin suite still passed after Phase 2B: 7 tests.
- Local full backend suite passed after Phase 2B stale polling guard: 714 tests (+4).
- Local multi-account automation routing suite passed after Phase 2C-A polling prefilter + summary: 152 tests (+4).
- Local Phase 2 media-catalog + round-robin suite still passed after Phase 2C-A: 7 tests.
- Local full backend suite passed after Phase 2C-A polling prefilter + summary: 718 tests (+4).
- Local multi-account automation routing suite passed after Phase 2D opening-DM same-user cooldown: 157 tests (+5).
- Local Phase 2 media-catalog + round-robin suite still passed after Phase 2D: 7 tests.
- Local full backend suite passed after Phase 2D opening-DM same-user cooldown: 723 tests (+5, excluding incomplete stashed Phase 2C-B WIP test).

## Current Blockers

- Billing remains blocked.
- Auth recovery email delivery E2E remains a billing blocker until proven end to end.
- Instagram automation still requires live verification after every production deploy because Meta behavior cannot be fully proven by unit tests.
- Instagram comment automation is currently observed through polling in the operator stop-point page; instant webhook delivery still needs protected endpoint/log confirmation and may depend on Meta Advanced Access/subscription state.
- Dashboard simplification / x-axis label polish is deployed and still needs live operator UI confirmation before being considered known-good.
- Pending quick-reply parity fix must be deployed and live-tested before it can be considered known-good: outgoing opening DMs on all linked accounts should have the same creator-authored body format with no visible browser/laptop fallback instruction; quick reply buttons should remain present; internal typed fallback should remain scoped to a valid pending session.
- Pending instagram_automation_events TTL fix must be deployed and verified: after deploy, confirm `ttl_instagram_automation_events_created` exists in Atlas Indexes tab on `instagram_automation_events` with `expireAfterSeconds=604800`; monitor `db.stats()` storage size over the next 7-14 days to confirm growth is now bounded. Diagnostic events from before the deploy that are older than 7 days will be pruned by Atlas's background TTL monitor (60s cadence) within minutes of index creation.
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

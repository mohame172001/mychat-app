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

- Current local HEAD before cleanup: `f6da7721bcd29baf597283420c88fb906c08355d`.
- Current production build_sha before cleanup: `f6da7721bcd2`.
- Current cleanup fix: pending `chore(cleanup): remove dead code and unused artifacts safely`. Scope is cleanup only: normalize root `.gitignore`, ignore local cache/build/temp artifacts, remove misleading stale polling-primary comments, and update project-history references. No behavior changes are intended. Polling fallback remains enabled, webhook remains enabled, and exact-comment dedupe, Phase 2D opening-DM cooldown, bot-own skip, stale/historical polling guards, account isolation, Billing, HMAC, rate limits, quick reply copy, DM automation, Dashboard/frontend routes, and username-specific behavior are unchanged. Cleanup commit SHA: `4151922054d759eeef37181113ffe95f76cc771e`.
- Deployed Phase 2S polling-primary fix: `f6da7721bcd29baf597283420c88fb906c08355d` / `fix(instagram): enable aggressive polling for comment automation`. Production `/api/version` reports `f6da7721bcd2`; `/api/` reports ok.
- Current local HEAD before Phase 2S polling-primary fix: `f76c0afa8cadfc459dfc4e9220c39754e73c4118`.
- Current production build_sha before Phase 2S polling-primary fix: `f76c0afa8cad`.
- Current local app-code fix: pending `fix(instagram): enable aggressive polling for comment automation`. User decision: polling is now the practical primary sender for comment automation because external comment webhooks remain incomplete for some tester comments. Production defaults now enable the polling loop and polling sends when env is absent in production: `IG_POLL_ENABLED=true`, `IG_POLLING_COMMENT_AUTOMATION_FALLBACK_ENABLED=true`, `IG_POLL_INTERVAL_SECONDS=15`, `IG_POLL_ROUND_ROBIN_BATCH=25`, `IG_POLL_RECENT_MEDIA_LIMIT=50`, `IG_POLL_FRESH_COMMENT_WINDOW_SECONDS=3600`. Explicit env values still override, including rollback values that disable the loop or send gate. Webhook remains enabled and continues to dedupe with polling; exact-comment dedupe, Phase 2D opening-DM cooldown, bot-own skip, activation cutoff, stale/historical polling guard, Billing, HMAC, rate limits, quick reply copy, DM automation, Dashboard routes, and username-specific behavior are unchanged.
- Current local HEAD before Phase 2R fresh-comment anchor state fix: `a29183a325655f778334928c989683a0b539500e`.
- Current production build_sha before Phase 2R fresh-comment anchor state fix: `a29183a32565`.
- Current local app-code fix: pending `fix(admin): prevent stale fresh-comment anchor filters`. Webhook Verification now starts with an empty fresh-comment anchor, uses a format-only placeholder (`YYYY-MM-DDTHH:mm:ssZ`), validates before sending, omits `after_utc` when the visible input is empty, clears the anchor on username change, adds an explicit Clear anchor action, and copies the backend-applied cutoff instead of unsent UI text. Backend `after_utc` handling is request-scoped: empty/whitespace/`null` means no cutoff and invalid non-empty values return a clear validation error. Cutoff-dependent fresh-comment signals remain omitted when no valid cutoff is applied. Polling remains disabled by default; no send behavior, Billing, HMAC, dedupe, Phase 2D cooldown, rate limits, quick reply copy, DM automation, Dashboard routes, or username-specific behavior changed.
- Current local HEAD before Phase 2Q external-comment visibility classifier fix: `92c219003730493548ccaff8ae917053014e7acf`.
- Current production build_sha before Phase 2Q external-comment visibility classifier fix: `92c219003730`.
- Current local app-code fix: pending `fix(instagram): classify external commenter visibility in webhook tests`. The Direct Graph comment check now uses the active connected account token to classify whether a fresh external comment is visible on the expected media and whether a matching `webhook_comment_detected` exists after the fresh-comment cutoff. It returns safe verdicts (`external_comment_visible_in_graph`, `external_comment_visible_in_graph_but_no_webhook_event`, `external_comment_not_visible_in_graph`, `external_comment_arrived_under_different_media`, `external_comment_filtered_before_logging`, `graph_comments_read_failed`) with only partial ids, booleans, bounded redacted Graph errors, and no raw text/tokens/full ids. The probe now forwards the Webhook Verification `after_utc` anchor and fixes the timestamp parsing bug in the Graph comments loop (`_parse_graph_datetime`). Polling remains disabled by default; no send behavior, Billing, HMAC, dedupe, Phase 2D cooldown, rate limits, quick reply copy, DM automation, Dashboard routes, or username-specific behavior changed.
- Current local HEAD before Phase 2P fresh-cutoff/bot-own classifier fix: `7eaf8b289123ca7187a0c2b2160269308d8b6675`.
- Current production build_sha before Phase 2P fresh-cutoff/bot-own classifier fix: `7eaf8b289123`.
- Current local app-code fix: pending `fix(instagram): apply fresh-comment cutoff and classify bot-own webhook skips`. Webhook Verification already sent `after_utc` to the backend when present, but the Copy JSON wrapper did not include the active fresh-comment anchor, making pasted diagnostics look like `after_utc=null`. The backend now also classifies webhook self-comment skips as terminal `webhook_skipped_bot_own_reply`, adds `only_bot_own_comment_seen_after_cutoff` and `fresh_external_comment_no_webhook_signal_after_comment_time` fresh-comment verdicts, exposes commenter/bot-own/terminal fields in flow verdicts, and shows commenter/bot-own columns in the admin UI. Polling remains disabled by default; no send behavior, Billing, HMAC, dedupe, Phase 2D cooldown, rate limits, quick reply copy, DM automation, Dashboard routes, or username-specific behavior changed.
- Current local HEAD before Phase 2O repair/readback fix: `2692eb10179a76f40f1e99288000b8f17aa08bed`.
- Current production build_sha before Phase 2O repair/readback fix: `2692eb10179a`.
- Current local app-code fix: pending `fix(instagram): expose and fix webhook subscription graph readback failure`. The Repair comment-webhooks flow now carries Graph subscribed-apps readback details end-to-end instead of collapsing them into generic `repair_graph_readback_failed`. Subscribe POST and readback GET use the same active `instagram_accounts` row IG business id and access token. Readback failures now expose only redacted diagnostic fields: endpoint kind, object id partial, HTTP status, response keys, Graph error code/subcode/message, and precise reason such as `graph_readback_permission_denied`, `graph_readback_wrong_object_id`, `graph_readback_missing_data_array`, `graph_readback_unexpected_shape`, `graph_readback_empty_response`, `graph_readback_parse_error`, or `graph_readback_timeout`. UI copy shows `readback failed: <reason>` instead of suggesting reconnect unless `comment_permission_not_granted` is proven. Polling remains disabled by default; Billing, HMAC, dedupe, Phase 2D cooldown, rate limits, quick reply copy, DM automation, Dashboard routes, and username-specific behavior are unchanged.
- Current local HEAD before Phase 2C-B work: `90fa8e3523e98377578554f92bcd071283e1a38a`.
- Current known-good code baseline: `debbcfb9f16d56b9cc511e24768ffd9086b8ef3b`.
- Current production build_sha before Phase 2C-B work: `90fa8e3523e9` (docs marker commit on top of known-good Phase 2D code).
- Current working status: Phase 1 SaaS hardening, Phase 2 media catalog + round-robin, Phase 2B stale polling guard, Phase 2C-A polling-noise reduction, and Phase 2D opening-DM same-user cooldown are stable through known-good code `debbcfb9f16d`.
- Current local app-code fix: pending `fix(instagram): resolve unmapped comment webhooks by media owner`. It preserves the existing primary `_find_user_doc_for_instagram_account_id(entry.id)` resolver first. Only unresolved comment webhook changes use a bounded media-owner probe across active/valid connected Instagram accounts with tokens. Exactly one media owner resolves; zero or multiple owners fail closed. Successful fallback self-heals `webhookEntryIdAliases` idempotently so later webhooks for the same `entry.id` resolve without probing. Diagnostics log only sanitized partial identifiers and bounded account identity samples. Polling fallback, HMAC, Billing, dedupe semantics, rate limits, quick reply copy, Dashboard/frontend, and username-specific behavior are unchanged.
- Do not update `02_KNOWN_GOOD_VERSIONS.md` for Phase 2C-B until live webhook fast-reply verification passes.
- Current local app-code fix: pending `chore(instagram): add safe webhook verification diagnostics` (Phase 2C-C). New admin endpoint `GET /api/admin/instagram/webhook-verification` returns a sanitized summary + filtered event list so the operator can verify whether Meta's comment webhooks arrive, map, parse, and call `_handle_new_comment(source='webhook')` — without DB access or shell credentials. Read-only, admin-gated via existing `_require_admin_permission(PERM_USERS_VIEW)`. Filters by `username`, `since_minutes` (1-1440), `comment_id_partial`, `media_id_partial`. Returns counters: webhook_comment_events_seen_count, webhook_comments_mapped_count, webhook_comments_unmapped_count, webhook_comments_reached_handle_count, webhook_comments_success_count, polling_success_count, phase2c_b_fallback_used_count, alias_self_heal_count, latest_webhook_comment_at, latest_polling_success_at. Per-event payload includes only allow-listed safe fields; tokens / full payloads / full identifiers are excluded by construction. No sending behavior, no dedupe, no HMAC, no rate-limit, no quick-reply, no Dashboard/frontend change.

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
- Local Phase 2C-B webhook media-owner resolver suite passed: 14 tests.
- Local multi-account automation routing suite passed after Phase 2C-B webhook resolver: 157 tests.
- Local Phase 2 media-catalog + round-robin suite still passed after Phase 2C-B: 7 tests.
- Local full backend suite passed after Phase 2C-B webhook resolver: 737 tests (+14).

## Current Blockers

- Billing remains blocked.
- Auth recovery email delivery E2E remains a billing blocker until proven end to end.
- Instagram automation still requires live verification after every production deploy because Meta behavior cannot be fully proven by unit tests.
- Instagram comment automation was often observed through polling fallback because some comment webhooks arrived with an unmapped `entry.id`. Phase 2C-B now resolves unmapped comment webhooks by media owner in code, but live fast-reply validation is still required before calling it known-good; Meta Advanced Access/subscription state can still affect whether comment webhooks arrive at all.
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

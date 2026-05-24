# Incidents

Use this file to record production-impacting problems and the exact fix.

## Template

### Incident: short title

- Date/time:
- Symptom:
- Affected area:
- Root cause:
- Fix commit:
- Tests:
- Deploy status:
- Lesson learned:
- Preventive test added:

## Recorded Incidents

### OAuth Duplicate Code Could Wipe Connection

- Date/time: 2026-05-24
- Symptom: Sentry reported Instagram callback failure: "This authorization code has been used".
- Affected area: Instagram OAuth callback and account connection state.
- Root cause: Replayed OAuth callback could reach token exchange failure path and risk clearing a working connection.
- Fix commit: `d30f04e`, building on `8ebe8c6`.
- Tests: `test_instagram_callback_meta_code_used_error_keeps_connection`; full backend passed.
- Deploy status: Deployed, production build_sha `d30f04e15582`.
- Lesson learned: Treat OAuth code replay as a safe replay outcome, never as a destructive connection failure.
- Preventive test added: Yes.

### Railway Startup Healthcheck Timeout Risk

- Date/time: 2026-05-24
- Symptom: Startup could be blocked by cold Mongo index creation and migrations.
- Affected area: Backend startup and Railway healthcheck.
- Root cause: Heavy bootstrap work ran directly inside FastAPI startup.
- Fix commit: `8ebe8c6`.
- Tests: `test_startup_schedules_bootstrap_instead_of_awaiting_it`; bootstrap tests passed.
- Deploy status: Deployed.
- Lesson learned: Startup must register long-running bootstrap work instead of blocking health.
- Preventive test added: Yes.

### Multi-Account Routing Confusion

- Date/time: 2026-05-22 to 2026-05-23
- Symptom: Account 1 and Account 2 could trigger wrong or incomplete automation flows.
- Affected area: Webhook routing, account resolution, token selection, rule scoping.
- Root cause: Some paths historically mixed active UI account, user-level legacy identifiers, and webhook event identities.
- Fix commit: Multiple commits including `7436984`, `f65cff7`, `7841c78`, `0dfaa72`, `e2be355`.
- Tests: Multi-account routing tests added and expanded.
- Deploy status: Deployed over several iterations.
- Lesson learned: Webhook execution must be account-first, never UI-active-account-first.
- Preventive test added: Yes.

### External Quick Reply Continuation Issue

- Date/time: 2026-05-23
- Symptom: Opening DM arrived, but clicking first quick reply did not continue for normal external users.
- Affected area: Instagram DM webhook continuation and session lookup.
- Root cause: Continuation logic needed to treat sender as external contact and resolve business account from recipient context.
- Fix commit: `9e2363d`, `3dbef2c`, with later fallback support in `129f420`.
- Tests: External-user quick reply and fallback tests were added.
- Deploy status: Deployed, but live Meta behavior still requires verification after any change.
- Lesson learned: Do not assume clicker exists in `instagram_accounts`.
- Preventive test added: Yes.

### Post-Specific Versus Any-Post Matching Divergence

- Date/time: 2026-05-23
- Symptom: General automation worked while post-specific automation did not behave the same.
- Affected area: Rule matching and post-match execution.
- Root cause: General and post-specific paths had drifted before the parity fix.
- Fix commit: `e2be355`.
- Tests: Account/post scoped parity tests added.
- Deploy status: Deployed.
- Lesson learned: Rule type should affect only match condition, not execution pipeline.
- Preventive test added: Yes.

### Diagnostics UI Removal

- Date/time: 2026-05-23
- Symptom: Operator no longer wanted production diagnostics page visible or accessible in frontend.
- Affected area: Frontend admin routes.
- Root cause: Temporary support UI remained in production frontend.
- Fix commit: `d29ec7d`.
- Tests: Frontend route and bundle checks.
- Deploy status: Deployed.
- Lesson learned: Keep protected backend support endpoints, but do not expose diagnostic UI in production.
- Preventive test added: Bundle/route checks in verification process.

### Media Picker Showed Internal Cache Error

- Date/time: 2026-05-24
- Symptom: Automation builder displayed `api_cache_uncacheable_response` when Instagram media endpoints failed.
- Affected area: Frontend cache and Automations media picker.
- Root cause: Safe `ok=false` JSON was treated as an exception and the internal exception code leaked into UI.
- Fix commit: `d30f04e`.
- Tests: Frontend apiCache test updated; full frontend passed.
- Deploy status: Deployed.
- Lesson learned: Failed JSON payloads should not be cached, but UI should receive safe failure details.
- Preventive test added: Yes.

### Stop Point Summary Hid External Comment Behind Bot Reply

- Date/time: 2026-05-24
- Symptom: Automation Stop Point could show `exact_stop_reason=bot_own_reply` for `@muhammad_gehad` even after a real external user comment had triggered the automation.
- Affected area: Protected backend stop-point summary and admin support output.
- Root cause: The summary reducer selected the latest comment-related event globally. A bot-owned public reply generated after a successful automation could be newer than the real external comment, so its `bot_own_reply` skip reason hid the meaningful external-user stop point.
- Fix commit: `e109f6e`.
- Tests: `test_summarize_stop_point_prefers_external_comment_over_bot_reply`; focused multi-account tests 92 passed; full backend 623 passed.
- Deploy status: Local, deploy required if accepted.
- Lesson learned: Bot-owned reply events should remain in the flight recorder but must not be treated as the primary support summary when a real external commenter event exists.
- Preventive test added: Yes.

### Polling Target Skipped Legacy General Rule's New Posts

- Date/time: 2026-05-25
- Symptom: muhammad_gehad only scanned old media and surfaced `already_replied_success` / `historical` / `bot_own_reply` events for OLD comments. Fresh comments on NEW posts on muhammad_gehad were never processed via polling even though mogehad17 (whose webhook arrived from Meta) succeeded on the same external commenter and same general rule shape.
- Affected area: `_collect_target_media_ids` (server.py:19985). Polling target collection only; rule matching and execution are unaffected.
- Root cause: `_collect_target_media_ids` read the raw top-level `a.get('trigger')` field. For a legacy general rule (`trigger='Manual'`/empty, `post_scope='any'`, `nodes[].data.trigger='comment:any'`), `trigger.startswith('comment:')` was False, so `needs_any` never became True, `_fetch_recent_media_ids` was never called, and the target list contained only whatever stale `selected_media_id`/`trigger_media_id` aliases lived on the rule. The user's NEW post — which had no matching alias — was never polled. mogehad17 only worked because Meta's webhook delivered the new comment directly, bypassing the target-collection path entirely. The canonical-trigger / canonical-post_scope helpers introduced by `54fb527` for rule classification existed but had never been applied to target collection.
- Fix commit: `948a996`. `_collect_target_media_ids` now reads through `_comment_rule_canonical_trigger_value` and `_comment_rule_post_scope_value`. Account-agnostic; identical change applies to every linked account. Post-specific rules still scope strictly to their selected media — the `_selected_specific_media_id` branch is unchanged.
- Tests: `test_collect_target_media_ids_includes_recent_for_legacy_general_rule`, `test_collect_target_media_ids_includes_recent_for_post_scope_only_general_rule`, `test_collect_target_media_ids_unchanged_for_post_specific_rule`, `test_legacy_general_rule_fresh_polling_comment_matches_and_sends`. `test_multi_account_automation_routing.py` 114 passed; backend full 645 passed.
- Deploy status: **Resolved.** Deployed to production `build_sha=948a9964e1d5`.
- Verification: Live retest with a fresh comment from a completely new external Instagram account on a new post on muhammad_gehad — automation reply + opening DM succeeded via the polling fallback. mogehad17 webhook path remained successful for the same commenter. Meta webhook delivery for muhammad_gehad's comment-field events is NOT covered by this resolution and is tracked separately as a Meta-side App Review / Advanced Access question.
- Lesson learned: When a code-path matures around a canonical-form helper (here `54fb527`'s canonical trigger/scope), every consumer of the raw legacy field must be updated. The polling target list is the exact mirror site of rule classification — both decide "should this rule cause us to scan media X?" and both must read through the same canonical lens.
- Preventive test added: Yes.

### Stop-Point Showed rule_not_matched on Repeated Duplicate Tests

- Date/time: 2026-05-24
- Symptom: Operator was testing by commenting repeatedly from the same external Instagram account on the same posts. Automation Stop Point reported `exact_stop_reason=rule_not_matched` and `reply_attempted=false`, `dm_attempted=false`. The operator suspected a rule-matching regression. Flight Recorder showed `poller_comment_seen` and `dedupe_checked` for the latest comment but no `rule_loading_finished` and no `rule_candidate_evaluated`.
- Affected area: Admin support reporting only — automation execution path was not affected. Dedupe was working correctly to prevent duplicate replies.
- Root cause: Inside `_handle_new_comment` (server.py:17005-17351), the dedupe / already-processed early-return paths returned to the caller WITHOUT writing a flight-recorder event. The summarizer's `rule_miss` lookup found no `rule_match_failed` / `automation_skipped` event for the latest `comment_id_partial`, so its fallback at server.py:21691 emitted the literal `'rule_not_matched'`. Affected paths: `comment_already_pending_queue`, the `already_replied_success` branch, the two `comment_already_partial_success` branches, the `comment_already_dm_failed` branch, the `public_reply_required_recovery` branch, and the generic catch-all that already resolved an `exact_reason` / `return_reason` before returning silently.
- Fix commit: pending (this commit). Each silent early-return now records an `automation_skipped` flight-recorder event with `skip_reason` set to the existing return reason string. No automation logic, no rule matching, no send path, no dedupe behavior changed — only an extra recorder write per silent return.
- Tests: `test_duplicate_comment_already_replied_records_skip_event_and_does_not_resend`, `test_duplicate_pending_queue_records_skip_event`, `test_duplicate_dm_failed_permanent_records_skip_event`, `test_fresh_comment_with_general_rule_still_matches_and_sends`. `test_multi_account_automation_routing.py` 110 passed; backend full 641 passed.
- Deploy status: Local, deploy required. Not eligible for `02_KNOWN_GOOD_VERSIONS.md` until live retest confirms Stop Point now shows the actual duplicate/already-processed reason for a repeated comment instead of `rule_not_matched`.
- Lesson learned: Every early-return path that exits before `rule_loading_finished` must record a flight-recorder event with the same reason it returns to the caller. Silent returns mislead the operator-facing summary into the worst possible default ("rule_not_matched") and waste investigation time.
- Preventive test added: Yes.

### Stop-Point Showed rule_not_matched Despite Successful Automation

- Date/time: 2026-05-24
- Symptom: After a successful run (`last_send_result.reply_status=success`, `dm_status=success`, `action_status=success` on the latest comment doc), Automation Stop Point reported `rule_matched=false`, `reply_attempted=false`, `dm_attempted=false`, `exact_stop_reason=rule_not_matched`. Webhook Health top-level `webhook.last_received_at` / `last_processed_at` were `null` even though some accounts showed `last_comment_source=webhook`.
- Affected area: Admin support summaries only (no automation execution path was involved).
- Root cause (A): `_latest_external_comment_event` always returned the most recent external comment (excluding bot-own-reply). The stop-point summarizer then scoped all event reads to that single `comment_id_partial`. If a NEWER unrelated external comment arrived after the success (e.g. produced no rule match because of dedupe, activation cutoff, or sibling-commenter), the events for the prior successful comment fell outside the scope and the summary collapsed to the literal default `rule_not_matched`.
- Root cause (B): The top-level admin webhook counters (`WEBHOOK_LAST_RECEIVED_AT`, `WEBHOOK_LAST_PROCESSED_AT`) are module-level globals that reset on every Railway redeploy. Per-account `source` is computed from the persistent `instagram_automation_events` collection. After a fresh deploy with no inbound webhook yet, the top-level fields were `null` while per-account `source=webhook` continued to be reported correctly — looking like a contradiction.
- Fix commit: pending (this commit). Selection helper now prefers the comment with the most recent `automation_success` event before falling back to the latest-external behavior. New `_resolve_webhook_counters` helper falls back to a flight-recorder lookup when the in-process globals are `None`. No automation logic, no rule matching, no send path touched.
- Tests: `test_summarize_stop_point_prefers_successful_comment_over_newer_unrelated`, `test_summarize_stop_point_falls_back_when_no_success`, `test_resolve_webhook_counters_falls_back_to_flight_recorder`, `test_resolve_webhook_counters_prefers_globals_when_set`; `test_multi_account_automation_routing.py` 104 passed; backend full 635 passed.
- Deploy status: Local, deploy required. Not eligible for `02_KNOWN_GOOD_VERSIONS.md` until live admin Stop Point shows a verified success after the deploy.
- Lesson learned: When a summary view scopes by `comment_id_partial`, the comment chosen must reflect the most meaningful outcome (success), not just the latest event. In-process counters that survive only the current boot must be backed by a persistent store before they are surfaced to a multi-day operator view.
- Preventive test added: Yes.

### Legacy General Any-Post Rules Did Not Match

- Date/time: 2026-05-24
- Symptom: Automation Stop Point showed `comment reached=true`, `source=polling`, `rule_matched=false`, `reply_attempted=false`, and `dm_attempted=false` for linked accounts even though the operator confirmed the rules were general/any-post.
- Affected area: Instagram comment automation rule matching.
- Root cause: The execution path in `_handle_new_comment` decided whether a rule should enter the comment matching branch using the top-level `trigger` field directly. Legacy/general rules could be persisted with `post_scope=any` and/or `nodes[].data.trigger=comment:any` while top-level `trigger` was missing, empty, or `Manual`, so the rule loaded but was never evaluated as a comment rule and produced no `rule_match_failed` event.
- Fix commit: `54fb527`.
- Tests: `py_compile backend/server.py`; `test_multi_account_automation_routing.py` 96 passed; full backend 627 passed.
- Deploy status: **Resolved.** Deployed through `a488bbb`; production reached `61103fb1a270` and the fix was live-verified end to end on both linked Instagram accounts.
- Verification: Live retest with a fresh comment on a general any-post rule on Account 1 and Account 2 produced reply + opening DM on both. The Rule Coverage admin tab reported `should_match=true` on the active general rule for each linked account. Webhook delivery latency (`source=polling`) remains a separate, unresolved observation and is NOT covered by this resolution.
- Lesson learned: Any-post versus post-specific should affect only the media match condition; comment-capability detection must read canonical trigger aliases and post scope, not only top-level `trigger`.
- Preventive test added: Yes.

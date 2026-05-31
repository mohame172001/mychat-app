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

### Stale Fresh-Comment Anchor Looked Active

- Date/time: 2026-05-31
- Symptom: Webhook Verification appeared to show an old fresh-comment anchor (`2026-05-29T21:55:00Z`) even though the operator did not intentionally type one, making the copied diagnostics and fresh-comment conclusions easy to misread.
- Affected area: Admin Webhook Verification UI/state and request-scoped `after_utc` validation only.
- Root cause: The anchor input used a real historical timestamp as its placeholder, so an empty field could visually resemble an active filter. Copy JSON also read the visible UI state rather than the backend-applied filter, and the backend accepted malformed/non-applied cutoff text without a strict request-scoped boundary.
- Fix commit: pending (this commit). Replace the real timestamp placeholder with `YYYY-MM-DDTHH:mm:ssZ`, add explicit "No fresh-comment anchor active" / "Filtering events after" status text, add Clear anchor, clear the value on username changes, validate before sending, copy only `backend_response.applied_filters.after_utc`, and make the backend treat empty/whitespace/`null` as no cutoff while rejecting invalid non-empty timestamps.
- Tests: Missing/empty/null `after_utc` has no applied cutoff and no fresh-comment signal; invalid `after_utc` returns a clear validation error; valid cutoff-dependent verdicts still work; Graph check rejects invalid cutoff; frontend structural test covers status, validation, clear button, and backend-applied Copy JSON.
- Deploy status: Local, deploy required.
- Lesson learned: Diagnostic filters need an explicit active/inactive state; placeholders must never look like live production values.
- Preventive test added: Yes.

### External Comment Missing While Self-Comment Webhook Arrived

- Date/time: 2026-05-31
- Symptom: For `muhammad_gehad`, comments webhooks arrived and a self-comment reached `webhook_comment_detected` then skipped as `bot_own_reply`, proving comment webhook delivery, account resolution, parsing, and bot-own protection. A later external tester comment did not appear in Webhook Verification and did not trigger automation while polling remained disabled.
- Affected area: Admin Webhook Verification / Direct Graph comment-check diagnostics only.
- Root cause: The existing Direct Graph comment-check endpoint could tell whether Graph returned matching comments, but it did not classify the key split needed here: Graph-visible external comment with no webhook, Graph-invisible external comment, different-media webhook, or a comment-shaped webhook filtered before `webhook_comment_detected`. It also called `parse_graph_datetime` instead of `_parse_graph_datetime`, so Graph comment timestamps could fail parsing and make time-window matching inconclusive.
- Fix commit: pending (this commit). Reuse the existing Direct Graph comment check, forward the active fresh-comment `after_utc` anchor, parse Graph timestamps with `_parse_graph_datetime`, and return precise safe verdicts: `external_comment_visible_in_graph`, `external_comment_visible_in_graph_but_no_webhook_event`, `external_comment_not_visible_in_graph`, `external_comment_arrived_under_different_media`, `external_comment_filtered_before_logging`, or `graph_comments_read_failed`.
- Tests: Graph-visible/no-webhook, Graph-not-visible, visible-and-webhook-detected, different-media, filtered-before-logging, no username-specific behavior, and UI structural coverage.
- Deploy status: Local, deploy required.
- Lesson learned: Once self-comment webhooks prove the handler path, the next diagnostic step is provider visibility for the external comment on the expected media, not polling fallback or generic webhook-repair work.
- Preventive test added: Yes.

### Fresh Comment Anchor Was Missing From Copied Diagnostics And Bot-Own Webhook Skips Looked In-Flight

- Date/time: 2026-05-31
- Symptom: Webhook Verification showed that comment webhooks were arriving and a self-comment was correctly skipped as `bot_own_reply`, but the per-comment verdict called it `webhook_in_flight`. The copied JSON also showed no active fresh-comment cutoff (`after_utc=null`/missing), so the operator could not prove whether the later external tester comment was inside the filtered window.
- Affected area: Admin Webhook Verification diagnostics only: fresh-comment signal, flow verdict classifier, and UI Copy JSON/table rendering.
- Root cause: The backend flow classifier had no terminal branch for `automation_skipped(skip_reason=bot_own_reply, source=webhook)`, so it fell through to the generic in-flight label. The frontend Copy JSON wrapper did not include the active fresh-comment anchor fields, even though the backend endpoint can accept and apply `after_utc`.
- Fix commit: pending (this commit). Copy JSON now includes `after_utc`, `after_local`, and `after_time_utc_effective`. The backend now returns terminal `webhook_skipped_bot_own_reply` for self-comments, plus fresh-comment verdicts `only_bot_own_comment_seen_after_cutoff` and `fresh_external_comment_no_webhook_signal_after_comment_time`. The UI shows commenter id, bot-own, skip reason, and terminal status so self-comments, external comments, and missing external signals are visually separate.
- Tests: Endpoint applies/echoes `after_utc`; bot-own webhook skip is terminal and not in-flight; fresh-comment signal distinguishes only self-comment from missing external webhook; frontend structural test covers copied cutoff and new verdict fields.
- Deploy status: Local, deploy required.
- Lesson learned: Diagnostic JSON must carry the exact operator-entered filter state, and expected terminal skips should not share the same label as incomplete webhook processing.
- Preventive test added: Yes.

### Repair Comment Webhooks Returned HTTP 200 But Graph Readback Failed Generically

- Date/time: 2026-05-31
- Symptom: After clicking Repair comment webhooks, the admin UI could show `Repair failed · HTTP 200` and "subscribed now: -" while the account panel still listed every required field as missing with generic `repair_graph_readback_failed`. Operators could not tell whether the Graph readback failed because of auth, wrong object id, unexpected JSON shape, timeout, parse error, or genuinely missing subscription fields.
- Affected area: Admin Repair comment-webhook flow: `_subscribe_instagram_account_to_webhooks`, `ensure_instagram_account_webhook_ready`, `certify_instagram_account_for_comment_webhooks`, `admin_instagram_repair_comment_webhooks`, and Webhook Verification UI copy.
- Root cause: The subscribe helper performed the POST and immediate readback GET with the active account IG id/token, but it only returned `verify_status` and subscribed fields. Readback JSON parse/shape problems were swallowed, Graph error bodies were not safely summarized, and certification collapsed any readback failure into generic `repair_graph_readback_failed`.
- Fix commit: pending (this commit). Keep the same active-row token/object for POST and GET, but classify readback failures precisely (`graph_readback_permission_denied`, `graph_readback_wrong_object_id`, `graph_readback_missing_data_array`, `graph_readback_unexpected_shape`, `graph_readback_empty_response`, `graph_readback_parse_error`, `graph_readback_timeout`, etc.). Persist and return only redacted metadata: endpoint kind, object id partial, HTTP status, response keys, and Graph error code/subcode/message. Certification now emits `repair_graph_readback_failed:<reason>` for readback failures, and the UI displays `readback failed: <reason>` instead of suggesting reconnect unless `comment_permission_not_granted` is proven.
- Tests: Certification/repair tests for ready, missing fields, HTTP 403 permission denied, wrong object id, unexpected shape, response-key redaction, same object/token usage, no username-specific behavior. Frontend Webhook Verification structural tests cover readback failure copy.
- Deploy status: Local, deploy required.
- Lesson learned: A successful subscribe POST is not enough. Repair must immediately read back the same Graph object with the same active token and surface a precise, redacted readback failure when that proof step fails.
- Preventive test added: Yes.

### Comment Webhooks Arrived With Unmapped Entry Ids

- Date/time: 2026-05-28
- Symptom: Fresh Instagram comments could be delayed because comment webhook events were observed globally but did not always map to the connected Instagram account by `entry.id`. Polling fallback eventually found comments, but real-time webhook handling did not consistently call `_handle_new_comment(..., source='webhook')`.
- Affected area: Instagram webhook account resolution in `_process_webhook`.
- Root cause: The primary resolver only trusted known Instagram account identity fields. Some Meta comment webhook payloads can identify the webhook entry with an id that is not already present on the stored `instagram_accounts` row, even though the payload references media owned by one of the connected accounts.
- Fix commit: pending (this commit). Preserve the primary `entry.id` resolver first. For unresolved comment webhook changes only, extract the media id and run a bounded account-agnostic owner probe across active/valid connected accounts with tokens. Exactly one readable owner resolves the account and continues the normal webhook comment pipeline; zero or multiple owners fail closed. Successful fallback stores the incoming `entry.id` in `webhookEntryIdAliases` idempotently so later events route without probing. Failure logs contain only sanitized identifier matrices and bounded connected-account identity samples.
- Tests: normal entry.id mapping still works; unmapped comment webhook resolves by media owner; resolved fallback calls `_handle_new_comment` with `source='webhook'`; alias self-heal is idempotent; later webhook resolves through alias without probing; unowned and ambiguous media fail closed; invalid token does not crash; sibling accounts select the correct owner; polling fallback unchanged; HMAC/dedupe/Phase 2D cooldown/no-username-specific checks. `test_phase2c_b_webhook_media_owner.py` 14 passed; `test_multi_account_automation_routing.py` 157 passed; `test_phase2_media_catalog_round_robin.py` 7 passed; full backend suite 737 passed.
- Deploy status: Local, deploy required. Do not mark known-good until live webhook fast-reply testing confirms a fresh comment reaches the webhook path quickly and no ambiguous/no-owner failures appear for valid owned media.
- Lesson learned: Webhook account resolution needs a fail-closed secondary proof path for comment media ownership. Never guess by username, active UI account, or single-tenant fallback for unresolved comment webhooks.
- Preventive test added: Yes.

### Repeated Comments Could Trigger Repeated Opening DMs

- Date/time: 2026-05-27
- Symptom: One Instagram user commented multiple times on the same post. Public replies were allowed per physical comment, but the same DM conversation could receive a second opening automation message after the user replied in DM and then commented again.
- Affected area: Instagram comment-to-DM opening-flow cooldown in `_handle_new_comment` / `execute_flow`.
- Root cause: The business opening-DM dedupe key was already scoped to commenter + media + rule/account, but the shorter completed-flow reopen TTL could reopen opening-DM eligibility before the desired same-user/media/rule cooldown window elapsed.
- Fix commit: pending (this commit). Add `COMMENT_DM_OPENING_DEDUPE_WINDOW_SECONDS` (default 86400, clamped 3600-604800) and ensure completed/pending sessions cannot allow a second opening DM inside that window. Repeated comments inside the window still run comment-level public reply logic, but opening DM is skipped with `opening_dm_already_sent_for_commenter_media`.
- Tests: same commenter same media three comments sends one opening DM; same commenter replies in DM then comments after 15 minutes still skips opening DM; after 24 hours can reopen if policy allows; different commenters get their own DM; same commenter different media gets own DM; webhook and polling parity. `test_multi_account_automation_routing.py` 157 passed; `test_phase2_media_catalog_round_robin.py` 7 passed; full backend suite 723 passed.
- Deploy status: Local, deploy required. Do not mark known-good until live Instagram test confirms same user 3 comments on one post sends one opening DM within 24 hours.
- Lesson learned: Exact-comment dedupe and business opening-DM cooldown are separate contracts. Public replies can be per comment while opening DMs must be governed by commenter/media/rule/account cooldown.
- Preventive test added: Yes.

### Phase 2 Polling Produced Deep Automation Logs For Old Comments

- Date/time: 2026-05-26
- Symptom: After Phase 2/2B, polling no longer sent delayed replies to stale comments, but it still continuously logged old polling rows through deep stages such as `poller_comment_seen`, `dedupe_checked`, `rule_loading_started`, `rule_loading_finished`, `rule_candidate_evaluated`, and `automation_skipped(historical_before_rule_activation)`. This made Stop Point noisy and could surface misleading `rule_not_matched` when no fresh comment was scanned.
- Affected area: Instagram polling fallback in `_poll_user_comments`, plus Stop Point summarization.
- Root cause: `_poll_user_comments` sent every returned Graph comment into `_handle_new_comment`. Historical/stale filtering happened inside the full automation path, after detailed per-comment logs had already been written.
- Fix commit: pending (this commit). Add a polling-only prefilter before `_handle_new_comment` for provider-timestamped comments that are clearly `historical_before_rule_activation` or `stale_polling_comment` for a matching rule and do not have explicit selected-post catch-up enabled. Emit one `polling_scan_summary` per poll run with counts instead of full traces for those old rows. Stop Point now understands summary-only scans and reports `only_historical_or_stale_comments_seen` / `no_fresh_comment_seen_in_poll`.
- Tests: `test_poll_user_comments_prefilters_stale_polling_comment_without_full_trace`, `test_poll_user_comments_prefilters_historical_polling_comment_without_full_trace`, `test_poll_user_comments_process_existing_catchup_still_enters_full_trace`, `test_summarize_stop_point_summary_only_old_polling_not_rule_not_matched`; `test_multi_account_automation_routing.py` 152 passed; `test_phase2_media_catalog_round_robin.py` 7 passed; full backend suite 718 passed.
- Deploy status: Local, deploy required. Do not mark known-good until live verification confirms a fresh polling/webhook comment still gets a full trace, old polling comments summarize only, and both linked accounts remain account-agnostic.
- Lesson learned: Polling fallback should be a quiet recovery mechanism, not a continuous full automation feeder for historical rows. Filter obvious old polling rows before the full rule/dedupe/send pipeline.
- Preventive test added: Yes.

### Phase 2 Round-Robin Processed Stale Comments After Activation

- Date/time: 2026-05-26
- Symptom: After Phase 2 `ed3d842`, media catalog + round-robin correctly discovered older posts, but it could also first-see old comments on those older posts and process them if their provider timestamp was after rule activation. In live evidence, a comment with provider timestamp many hours old reached `rule_match_success`, `public_reply_success`, `opening_dm_success`, and `automation_success`, making automation appear to reply repeatedly to old interactions.
- Affected area: Instagram polling fallback in `_handle_new_comment`, specifically activation/freshness gating inside `apply_activation_cutoff`.
- Root cause: The cutoff only asked whether `comment_timestamp < activationStartedAt`. After round-robin, `comment_timestamp >= activationStartedAt` is not sufficient proof of a live/new interaction, because the poller may first discover an older comment on an older media item well after it was created.
- Fix commit: pending (this commit). Add a polling-only stale guard: when `source='polling'`, `process_existing_unreplied_comments` is false, Graph supplied a timestamp, and age exceeds `IG_POLL_FRESH_COMMENT_WINDOW_SECONDS` (default 3600, clamped 60-21600), skip as `stale_polling_comment` before public reply/DM. Webhook events are not affected, missing polling timestamps still use first-seen safety, and selected-post `process_existing_unreplied_comments=true` catch-up remains eligible.
- Tests: `test_polling_stale_comment_after_activation_skips_without_sending`, `test_polling_stale_webhook_comment_after_activation_is_not_blocked`, `test_post_specific_process_existing_allows_stale_polling_catchup`, `test_instagram_automation_has_no_username_specific_branches`; `test_multi_account_automation_routing.py` 148 passed; `test_phase2_media_catalog_round_robin.py` 7 passed; full backend 714 passed.
- Deploy status: Local, deploy required. Do not mark known-good until live retest confirms: a brand-new comment on old media still processes, an old re-scan skips as `stale_polling_comment`, exact duplicates still skip, and both linked accounts behave the same.
- Lesson learned: After expanding polling coverage, freshness must be an explicit safety dimension separate from rule activation. Activation time protects pre-rule history; freshness protects old post-round-robin discoveries.
- Preventive test added: Yes.

### Phase 1 SaaS Hardening — Required Before Real-User Onboarding

- Date/time: 2026-05-26
- Symptom: Full audit at build `ce901be8b400` concluded "Not ready for real-user onboarding; needs infrastructure/storage fixes first." Four latent traps were identified that would either misroute a multi-tenant customer's events, exhaust Atlas storage again, or mislead operators investigating live failures.
- Affected area: Webhook resolver fallback (server.py:20106-20116), Story Reply automation loading (server.py:20368-20370), three transient collections without TTL retention (webhook_processing_failures, comment_dm_sessions, link_click_events), Stop Point summary reporting (success-vs-dedupe label and literal `historical` skip_reason).
- Root cause:
  1. Single-tenant fallback would attribute any unmapped webhook to the only-connected-account when count==1. Safe today (2 accounts), unsafe the moment the very first SaaS customer signs up.
  2. Story Reply automations loaded by `{user_id, status, trigger}` only — a sibling-account rule could fire on a different account's story webhook under the same owner.
  3. Three transient collections accumulated indefinitely. The DLQ retry queue, closed comment-DM sessions, and raw click events all wrote without retention.
  4. Stop Point's stop_reason classifier let later polling re-scans (`automation_skipped(already_replied_success)`) shadow a genuine `automation_success` for the same comment. Separately, the no-fresh-comment override missed the literal `skip_reason='historical'` so old polling re-scans surfaced confusingly.
- Fix commit: pending (this commit). (1) `_resolve_single_tenant_fallback_flag()` defaults the legacy fallback to OFF in production and ON elsewhere; operator can override via `INSTAGRAM_SINGLE_TENANT_FALLBACK`. Startup warning if enabled in production. (2) Story Reply branch now uses `_account_scoped_query(user_id, ig_account_id)` matching the comment + DM webhook paths. (3) Three new TTL indexes: `ttl_webhook_dlq_terminal_at` (30d on `terminal_at` set when status transitions to terminal; active `pending_retry` rows unaffected), `ttl_comment_dm_sessions_expires_at` (30d past natural `expiresAt`; pending sessions have future `expiresAt`), `ttl_link_click_events_clicked_at` (90d; dashboard caps at 5000 most-recent reads). All env-tunable, all use the collMod-on-conflict fallback. (4) Stop Point: success-wins-over-dedupe override + literal `'historical'` skip_reason added to the no-fresh-comment override set.
- Tests: 13 new tests in `test_phase1_saas_hardening.py` covering all four fixes with regression-resistant static-source checks for the most-easily-reverted patterns. `test_multi_account_automation_routing.py` and `test_historical_*.py` continue to pass. Backend full: 703 passed (+13).
- Deploy status: Local, deploy required. Not eligible for `02_KNOWN_GOOD_VERSIONS.md` until a fresh end-to-end Instagram smoke test confirms: (a) `/api/version` returns the new SHA, (b) Atlas Indexes tab shows all four TTL indexes, (c) a fresh external comment produces `automation_success` and Stop Point reports it without dedupe-collapse, (d) no Sentry quota errors after deploy.
- Lesson learned: A SaaS-readiness audit must check each fallback path AND each reporting label for whether it remains safe when more than one tenant exists. The four fixes here are individually small but collectively cover the "first paying customer accidentally lands in someone else's account" scenario.
- Preventive test added: Yes. Including static-source checks that the regressions cannot be silently reverted.

### MongoDB Atlas Free Tier Filled By instagram_automation_events

- Date/time: 2026-05-26
- Symptom: Production MongoDB Atlas hit 512MB/512MB. Writes were blocked across the entire database — automation, dashboard, sessions, OAuth, everything. Manual triage showed `instagram_automation_events` was ~465MB by itself, dominating the deployment quota.
- Affected area: Diagnostic/flight-recorder write path only. No automation execution, dedupe, HMAC, send, or rate-limit code path is involved in the disk pressure.
- Root cause: `_record_instagram_automation_event` writes one document per stage (poller_comment_seen, rule_loading_finished, rule_match_success, automation_skipped, automation_success, etc.). On a polling-heavy account with many media + many old comments, every 15-second polling tick produces tens of rows even for already-processed comments (re-scans). With no retention policy on the collection it accumulated indefinitely. The collection backs the protected Admin Stop Point / Flight Recorder / Webhook Health views — operators only need recent rows (typical support window ≤ 7 days) but the collection retained months.
- Manual recovery: `instagram_automation_events` was dropped from Atlas to restore write capacity. Flight Recorder is empty until new events accumulate; this is a fresh diagnostic window, not data loss for end users.
- Fix commit: pending (this commit). Add a TTL index `ttl_instagram_automation_events_created` on `{created_at: 1}` with `expireAfterSeconds=604800` (7-day default). Atlas's background TTL monitor (60s cadence) prunes expired diagnostic events going forward, capping growth at ~7 days of events per account. Window is tunable via `IG_AUTOMATION_EVENTS_TTL_SECONDS` env var, clamped to `[86400, 7776000]` (1 day floor, 90 day ceiling) so a misconfigured value can neither empty the collection in one polling tick nor silently allow another overflow. If the index already exists with conflicting options, `collMod` updates the window in place. Index creation runs inside the existing non-blocking `_index_bootstrap` task scheduled from startup, so Railway healthcheck remains unaffected.
- Event-noise reduction: deliberately deferred. The user constraint was "only do this if it does not break Admin Stop Point diagnostics" — every event class is currently load-bearing for at least one Stop Point classification branch (e.g. `poller_comment_seen` drives `fresh_comment_seen_in_last_poll`; `automation_skipped(bot_own_reply)` drives the bot-own-reply selector at `_latest_external_comment_event`). The TTL alone bounds storage; reducing event recording requires deeper Stop Point regression testing and is a separate workstream.
- Tests: `test_index_bootstrap_creates_instagram_automation_events_ttl_index`, `test_index_bootstrap_clamps_ttl_within_safe_bounds`, `test_index_bootstrap_ttl_failure_does_not_crash_other_indexes`. `test_startup_bootstrap.py` 6 passed (+3). Backend full 690 passed (+3).
- Deploy status: Local, deploy required. Verify after deploy that the TTL index exists in Atlas (Indexes tab on `instagram_automation_events`) and that storage stays bounded over the next 7-14 days.
- Lesson learned: Every collection that takes per-event writes needs a TTL or rotation policy from day one. A diagnostic collection that the operator UI does not need beyond ~7 days should not be allowed to outlive a single support window. Atlas free tier has no built-in storage alerts at 80%/90% — add a runbook step to monitor `db.stats()` total size weekly until on a paid tier.
- Preventive test added: Yes.

### Visible Browser Fallback Copy Differed Across Linked Accounts

- Date/time: 2026-05-25
- Symptom: A browser/laptop fallback instruction was visible in outgoing Instagram DMs for one linked account but not another, creating account-to-account behavior differences and a separate "laptop version" of the automation message.
- Affected area: Instagram comment-DM opening message composition.
- Root cause: The centralized quick-reply fallback helper appended visible fallback copy at send time on the comment-DM flow entry path. Accounts/rule shapes that used that helper showed the extra copy, while other paths could preserve the creator-authored body, making the product feel inconsistent across linked accounts.
- Fix commit: pending (this commit).
- Tests: `test_opening_dm_with_quick_reply_does_not_append_arabic_browser_fallback`, `test_opening_dm_with_quick_reply_does_not_append_english_browser_fallback`, `test_linked_accounts_share_quick_reply_message_format_without_visible_fallback`; `test_multi_account_automation_routing.py` 139 passed; backend full 682 passed.
- Deploy status: Local, deploy required. Not eligible for `02_KNOWN_GOOD_VERSIONS.md` until live retest confirms both linked accounts send the same creator-authored opening DM body with quick replies still present.
- Lesson learned: User-visible message composition must be account-agnostic. Internal typed fallback can exist as a backend safety path, but it should not change outgoing copy unless the product explicitly enables it globally.
- Preventive test added: Yes.

### Polling Surfaced Old Re-Scans Instead Of Fresh Live Comment

- Date/time: 2026-05-25
- Symptom: After `b2ceca2`, the user retested from the same external Instagram account on a different post under `muhammad_gehad`, but Stop Point still showed `already_replied_success`. The selected “latest external comment” had provider timestamp `2026-05-19T20:02:11` while it was re-seen on `2026-05-25`, proving the summary was showing an old physical comment re-scan, not the new live test comment.
- Affected area: Instagram polling comment fetch order, bounded polling candidate ordering, and protected Stop Point summary selection.
- Root cause: The polling path fetched a bounded `/comments` slice without requesting newest-first order and processed the provider-returned order directly. Old already-processed comments and bot-owned replies could therefore consume the visible slice and the support summary could present an old processed re-scan as if it represented the new live test.
- Fix commit: pending (this commit).
- Tests: `test_polled_comments_sort_newest_external_before_old_rescans_and_bot_replies`, `test_poll_user_comments_requests_newest_first_order_and_processes_sorted_comments`, `test_summarize_stop_point_old_rescan_does_not_claim_fresh_live_comment`; `test_multi_account_automation_routing.py` 138 passed; backend full 681 passed.
- Deploy status: Local, deploy required. Not eligible for `02_KNOWN_GOOD_VERSIONS.md` until live retest confirms a fresh comment appears in polling/Stop Point, same-commenter different-post triggers, exact duplicates still skip, and quick-reply fallback remains working.
- Lesson learned: For fallback polling, do not trust provider default order. Always request newest-first where possible, locally prioritize fresh external comments in the bounded slice, and make support summaries distinguish old re-scans from absent fresh comments.
- Preventive test added: Yes.

### Fresh Polling Comments Were Marked Historical

- Date/time: 2026-05-25
- Symptom: After `e2bc4a3`, a fresh comment from the same external Instagram user on a different eligible post still skipped. Flight Recorder showed the polling path reached the comment and loaded one rule, then emitted `historical_before_rule_activation` / historical-related skips, while Stop Point could still fall back to the generic `rule_not_matched` summary.
- Affected area: Instagram polling comment timestamp handling, existing-comment historical skip classification, and protected Stop Point summary fields.
- Root cause: Polling comments without a Graph `timestamp` / `created_time` had no reliable proof that they were fresh, so the historical cutoff logic could treat them as unprovable/historical. Separately, an existing comment document with a stale `historical_before_rule_activation` skip could be trusted forever even when a current payload proved the comment timestamp was at/after the stored activation cutoff. The support summary did not surface enough timestamp/cutoff metadata, so the operator saw `rule_not_matched` instead of the concrete historical cutoff.
- Fix commit: pending (this commit).
- Tests: `test_fresh_polling_comment_missing_timestamp_uses_first_seen`, `test_old_polling_comment_before_activation_still_skips`, `test_stale_historical_skip_reprocesses_when_current_payload_is_after_activation`, `test_summarize_stop_point_surfaces_historical_skip_not_rule_not_matched`, plus webhook timestamp fallback regression. `test_multi_account_automation_routing.py` 135 passed; `test_webhook_timestamp_fallback.py` 7 passed; backend full 678 passed.
- Deploy status: Local, deploy required. Not eligible for `02_KNOWN_GOOD_VERSIONS.md` until live retest confirms same-commenter/different-post works, exact duplicates still skip, browser typed fallback works, and mobile quick reply still works.
- Lesson learned: For polling-only Meta flows, a missing provider timestamp is not evidence that a comment predates rule activation. Use provider timestamps when present; otherwise use first-seen time for newly observed polling events and keep exact-comment/provider-proof dedupe as the duplicate safety layer.
- Preventive test added: Yes.

### Dashboard Range Labels Were Crowded And Range Switching Felt Heavy

- Date/time: 2026-05-25
- Symptom: After the dashboard range redesign, the chart rendered too many x-axis labels (especially repeated weekday labels for longer ranges), switching ranges felt like a heavy reload, and the main view still exposed secondary metrics that made the dashboard noisy.
- Affected area: Dashboard frontend only.
- Root cause: `Dashboard.jsx` rendered one x-axis label for every chart bucket while using a fixed 7-column label grid, so 24h/30d/all ranges produced crowded/repeated labels. Range changes cleared the visible data when the new range had no cache entry, causing skeleton/loading UI instead of stale-while-revalidate behavior. Secondary KPI tiles were always visible.
- Fix commit: `c084311`.
- Tests: Frontend 187 passed; frontend production build passed.
- Deploy status: Deployed. Not eligible for known-good until live UI confirms the chart and range switching feel correct.
- Lesson learned: Chart label density must be derived from the selected range, and range filters should keep the previous safe dashboard state visible while refreshing.
- Preventive test added: Yes.

### Same Commenter On Different Post Was Skipped And Web Users Needed Manual Button Text

- Date/time: 2026-05-25
- Symptom: A same external Instagram commenter posted on a different eligible post, but the run could be skipped as already processed before rule loading. Separately, Instagram Web/laptop users might see the opening DM without the quick-reply button and creators were being forced to manually add "type this word" fallback text to every automation.
- Affected area: `_handle_new_comment` comment-event dedupe edge case and comment-DM opening message composition.
- Root cause: The exact-comment dedupe lookup trusted an existing processed comment document even when that document carried a different `media_id`, so a stale/colliding legacy row could block a fresh post interaction. Opening DMs with quick replies did not append an automatic typed fallback instruction derived from the button title, even though typed fallback continuation support already existed for pending sessions.
- Fix commit: pending (this commit).
- Tests: `test_same_commenter_different_post_general_rule_triggers_again`, `test_already_replied_success_from_other_media_does_not_block_new_post`, Arabic/English fallback append tests, no-duplicate fallback test, random text without pending session test. `test_multi_account_automation_routing.py` 131 passed; backend full 674 passed.
- Deploy status: Local, deploy required. Not eligible for `02_KNOWN_GOOD_VERSIONS.md` until live retest confirms same-commenter/different-post triggers, exact duplicates still skip, mobile quick replies still work, and typed browser fallback continues once.
- Lesson learned: Dedupe proof must include the post/media dimension for business-flow behavior, and user-facing quick-reply messages must include a web fallback automatically rather than relying on creator-authored boilerplate.
- Preventive test added: Yes.

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

### Total Contacts Combined Both Linked Accounts on Active-Account Dashboard

- Date/time: 2026-05-25
- Symptom: The dashboard's Total Contacts card (subtitle "Active Instagram account") displayed a number that combined contacts from BOTH linked Instagram accounts on a 2-account workspace. The number did not change when the active Instagram account was switched.
- Affected area: `_dashboard_scoped_docs` in `_calculate_dashboard_summary_live`. Dashboard read path only — no Instagram automation, rule matching, webhook, polling, dedupe, HMAC, or rate-limit code was involved.
- Root cause: When `getActiveInstagramAccount(user_id)` returned None (e.g. brief context-resolution miss between active-account switches, or any transient lookup failure), the dashboard fell back to a broad `{'user_id': user_id}` query in `_dashboard_scoped_docs`. That query matched contacts/comments/clicks/dm_logs from EVERY linked Instagram account on the workspace and unioned them all into the same response. On a single-account workspace that's correct; on a multi-account workspace it silently leaked. The cache TTL (60s / 5min stale) meant the leaked union persisted even after the active context was re-resolved.
- Fix commit: pending (this commit). The broad fallback now applies only when `include_unscoped=True` (i.e. ≤1 active linked account). With ≥2 active accounts and no active context, `_dashboard_scoped_docs` returns an empty result instead of leaking the union. Account-agnostic.
- Tests: `test_dashboard_scoped_docs_no_broad_fallback_when_multiaccount`. `test_dashboard_summary.py` 25 passed; backend full 668 passed.
- Deploy status: Local, deploy required. Not eligible for `02_KNOWN_GOOD_VERSIONS.md` until live retest confirms Total Contacts changes when the operator switches active Instagram account.
- Lesson learned: A defensive "broad fallback when context is missing" must not silently combine isolated tenants. On a multi-tenant workspace, the safe default is to return empty (or to refuse the read) rather than silently union — the cache then traps the union for minutes.
- Preventive test added: Yes.

### Incomplete Comment Doc Blocked as `comment_processed_unknown_state`

- Date/time: 2026-05-25
- Symptom: muhammad_gehad polling-fallback automation stopped firing for fresh comments. The (now-clearer) admin panels showed `exact_stop_reason=comment_processed_unknown_state`, `classified_reason=comment_processed_unknown_state`, `rule_matched=false`, `reply_attempted=false`, `dm_attempted=false`. Flight Recorder for the latest comment had `poller_comment_seen` + `dedupe_checked` + `automation_skipped(skip_reason=unknown_state)` but NO `rule_loading_finished`. mogehad17 (webhook path) continued to work because webhook delivery creates a fresh `ig_comment_id` with no existing doc, so the catch-all was never reached.
- Affected area: `_handle_new_comment` existing-comment dedupe classification (server.py:17426–17475). No webhook handler, polling loop, rule matcher, or send helper was involved.
- Root cause: The catch-all `else` block initialized `exact_reason='comment_processed_unknown_state'` and `return_reason='unknown_state'` as DEFAULTS, then tried to upgrade to a known state (historical / partial_success / replied_success / dm_failed / legacy already_replied). If none of those matched, the defaults stayed, an `automation_skipped` was recorded, and the function returned BEFORE rule loading. A comment doc that had been written by a previous polling cycle in a seen-only / incomplete shape (no canonical reply_status, no canonical dm_status, no `replied` proof, no recognized skip_reason) was therefore silently blocked forever despite carrying no proof that any send had ever been made.
- Fix commit: pending (this commit). The catch-all now skips only when it CAN classify the doc into a known processed state. When it cannot, the comment is treated as a retry on the existing doc: `retry_existing=True`, `existing_doc_id` / `existing_created` are captured, a new `existing_comment_unknown_state_reprocess` flight-recorder event is written, and execution falls through to rule loading. Downstream `opening_dedupe_key` and the provider-proof checks in `execute_flow` / `send_ig_message` independently protect against any actual duplicate send — so this is safe even if the comment turns out to have been processed via a path we didn't recognize.
- Tests: `test_unknown_state_comment_doc_is_reprocessed_not_silently_skipped`, `test_unknown_state_reprocess_does_not_overflow_into_duplicate_send`, `test_unknown_state_reprocess_does_not_override_known_skips`. `test_multi_account_automation_routing.py` 125 passed; backend full 661 passed.
- Deploy status: Local, deploy required. Not eligible for `02_KNOWN_GOOD_VERSIONS.md` until live retest confirms muhammad_gehad fresh comments reply again AND mogehad17 webhook path remains intact AND no duplicate sends on previously-succeeded comments.
- Lesson learned: A defensive catch-all classification must NOT silently treat "unknown" as "skip" when the skip decision is destructive (blocks rule loading forever). The safe default in dedupe code is to reprocess and rely on downstream provider-proof dedupe layers — a duplicate-send risk that's already independently guarded — rather than silently dropping legitimate work.
- Preventive test added: Yes.

### Admin Diagnostic Panels Showed Misleading State After Known-Good Deploy

- Date/time: 2026-05-25
- Symptom: Even though multi-account automation was live-verified working at `948a996`, the admin panels reported contradictory states for `muhammad_gehad`: `account_resolved=false`, `polling_scanned_account=false`, `exact_stop_reason=unknown_state` / `rule_not_matched`. Flight Recorder showed `suspected_candidate=silent_early_exit_possible` even when `automation_skipped` events with concrete `skip_reason` existed. Webhook Health didn't distinguish "comment webhooks observed globally vs mapped to this account" so the operator couldn't tell whether the polling fallback was the intended state.
- Affected area: Admin support panels only — `summarize_account_automation_stop_point`, `admin_instagram_multi_account_health`, and the frontend Flight Recorder / Webhook Health / Stop Point cards. No automation execution path was involved.
- Root cause: (a) `account_resolved` checked only the webhook-only `account_resolution_success` event — polling never writes it. (b) `polling_scanned_account` checked only the leading `poller_account_scan_started` event — if that aged out of the 100-event window but per-comment events remained, it was reported False. (c) When `rule_miss.skip_reason` was empty/None, the summarizer fell back to the literal `'rule_not_matched'` even though `extra.classified_reason` carried the actual reason (from `d4dae1a`'s silent-dedupe enrichment). (d) `multi-account-health` didn't surface per-account webhook delivery status. (e) Frontend Flight Recorder classifier didn't honor the concrete skip reasons.
- Fix commit: pending (this commit). Backend: widened `account_resolved` to accept polling-stage events; widened `polling_scanned_account` to media-scan/comment-seen too; added `_resolved_skip_reason` helper that promotes `classified_reason` when `skip_reason='unknown_state'`; new `early_exit_before_rule_loading` reason; new summary fields `classified_reason`, `is_latest_event_rescan_of_processed`, `is_latest_event_historical`; new `webhook_delivery_status` label per account in `multi-account-health` plus `last_comment_webhook_event_time` / `last_messaging_webhook_event_time` / `last_account_resolution_failed_at`. Frontend: Flight Recorder no longer defaults to `silent_early_exit_possible` when a concrete skip reason exists; Webhook Health renders a delivery-status badge; Stop Point card surfaces `classified_reason` and state badges.
- Tests: `test_summarize_stop_point_account_resolved_true_on_polling`, `test_summarize_stop_point_polling_scanned_account_via_comment_seen`, `test_summarize_stop_point_surfaces_dedupe_skip_reason_over_rule_not_matched`, `test_summarize_stop_point_surfaces_classified_reason_when_skip_reason_is_unknown`, `test_summarize_stop_point_labels_early_exit_when_no_rule_loading`, `test_summarize_stop_point_flags_rescan_of_processed_comment`, `test_multi_account_health_labels_polling_fallback_vs_global_unmapped`, `test_multi_account_health_labels_polling_fallback_when_no_comment_webhooks`. `test_multi_account_automation_routing.py` 122 passed; backend full 658 passed.
- Deploy status: Local, deploy required.
- Lesson learned: An aggregation that depends on a single event stage will report False whenever that stage falls outside the recent-event window. Use a set of compatible stages as the proof-of-state. Reason strings must propagate from the recorder's `extra.classified_reason` upward — every catch-all `unknown_state` must be enrichable.
- Preventive test added: Yes.

### Dashboard Conversion Rate + Top Automations Ordering

- Date/time: 2026-05-25
- Symptom: Read-only dashboard audit found two metric-correctness issues. (1) Conversion Rate mixed time windows — numerator counted unique link-click users in the current calendar month while the denominator was all-time `totalContacts`. Workspaces with old contact history saw the displayed conversion rate artificially collapse at the start of each new month. (2) "Top Automations" was the 6 most recently created automations (`autos[:6]` after a `created` desc fetch), even though the card label said "Top" and rendered `sent` next to each row — newly created drafts ranked above older high-volume rules.
- Affected area: Dashboard summary endpoint (`/api/dashboard/summary`) and Dashboard frontend card row. No Instagram automation execution path was involved.
- Root cause (1): `_calculate_dashboard_summary_live` collected the numerator inside the per-month click loop but used the full `contact_keys` set (all-time, union of every source) as the denominator.
- Root cause (2): the top-automations slice used the natural fetch order (`created` desc) without re-sorting by activity.
- Fix commit: pending (this commit). (1) New `month_contact_keys` set collected from the per-month click loop and the per-month comment loop, with the same own-account filter and account-scope as `contact_keys`. Conversion rate = `converted_count / len(month_contact_keys) * 100`, 0 when month is empty. `totalContacts` remains all-time. (2) `_top_auto_sort_key` sorts by `(-sent, 0 if active else 1, -created_ts)` before slicing 6. Frontend card subtitles clarify scope ("This month" on messages/conversion, "Active account" on contacts when multiple accounts connected). Optional secondary KPI row exposes payload fields already returned by the endpoint. Backend payload shape unchanged.
- Tests: `test_conversion_rate_uses_current_month_contacts_as_denominator`, `test_conversion_rate_zero_when_no_current_month_contacts`, `test_conversion_rate_excludes_bot_own_replies_from_denominator`, `test_top_automations_orders_by_sent_then_active_then_created`, `test_top_automations_does_not_leak_other_accounts`. `test_dashboard_summary.py` 18 passed; backend full 650 passed; frontend 184 passed; frontend build passed.
- Deploy status: Local, deploy required. Not eligible for `02_KNOWN_GOOD_VERSIONS.md` until a live dashboard retest confirms the numbers display correctly.
- Lesson learned: When two metrics compose into a ratio, the audit checklist must verify they share the same time window AND the same account/identity filter. A label like "Top X" must correspond to a sort key whose comparator references X.
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

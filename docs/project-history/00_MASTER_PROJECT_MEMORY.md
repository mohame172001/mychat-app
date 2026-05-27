# MyChat — Master Project Memory / Current-State Handoff

**Date:** 2026-05-26  
**Owner:** Mohamed Gehad / Mohamed Nassar  
**Purpose:** This file is the single local memory source for the MyChat project. Claude Code, Codex, ChatGPT, or any coding agent must read this before making assumptions or changing code.

---

## 0. Non-negotiable rule

MyChat is being built as a real multi-user SaaS, not as a one-off tool for one Instagram account.

Therefore:

- Do not special-case `muhammad_gehad`.
- Do not special-case `mogehad17`.
- Do not add username-specific behavior.
- Do not create separate execution paths per Instagram username.
- Every fix must be account-agnostic, multi-account safe, and SaaS-safe.
- Any automation action must be scoped by stable account identity, not by UI state alone.

Correct generic execution path:

```text
Instagram account connected
→ account resolver
→ webhook or polling discovers comment/message
→ normalized event
→ _handle_new_comment or the relevant DM handler
→ account-scoped rule loading
→ rule matching
→ dedupe / historical safety
→ public reply / opening DM / flow continuation
→ logs / Flight Recorder / Stop Point / dashboard
```

Forbidden shortcuts:

```text
if username == "muhammad_gehad"
if username == "mogehad17"
if active account then assume event belongs there
use the first/only account unless explicitly proven safe
```

---

## 1. Product vision

MyChat is a SaaS for Instagram automation.

Main product goal:

```text
A creator/business connects an Instagram Professional account,
creates automation rules,
and MyChat automatically replies to eligible comments,
opens DMs,
continues DM flows / quick replies,
tracks conversations, clicks, contacts, and usage,
and exposes diagnostics/admin tools for support.
```

The product should support:

- One user with one Instagram account.
- One user with multiple Instagram accounts.
- Many users, each with one or more Instagram accounts.
- Account-specific rules and logs.
- No cross-account leakage.
- No accidental automation on the wrong account.
- Predictable recovery and rollback.

---

## 2. Current stack and production URLs

Frontend:

```text
React / Create React App / CRACO
```

Backend:

```text
FastAPI / Python
```

Database:

```text
MongoDB Atlas / Motor
```

Hosting:

```text
Railway
```

Meta integration:

```text
Instagram Business Login
Graph API
Webhooks
Polling fallback
```

Production:

```text
Frontend:
https://frontend-production-6eb2.up.railway.app

Backend:
https://backend-production-a1a3.up.railway.app

Backend health:
https://backend-production-a1a3.up.railway.app/api/

Backend version:
https://backend-production-a1a3.up.railway.app/api/version
```

Auth:

```text
Email/password
Google Sign-In
JWT sessions with session_version
Password change
Password reset
Email verification
```

---

## 3. Current confidence verdict

The architecture is moving in the right direction, especially after the multi-account scoping work, TTL storage fix, and Phase 2 media catalog / round-robin design.

However, the project should **not** be called fully launch-ready yet.

Reasons:

1. Phase 2 `ed3d842` must be confirmed live on production.
2. The media catalog / round-robin system needs real smoke tests on both `muhammad_gehad` and `mogehad17`.
3. Cross-account isolation must be verified with live Flight Recorder data.
4. The token-refresh cron previously returned `500` during the MongoDB quota incident and must be re-run after recovery.
5. Sentry must be checked for new errors after Phase 2.
6. Atlas backup strategy is still a launch blocker.
7. Webhook delivery/mapping still has a known Meta-side / access issue; polling compensates, but polling-only will not scale to large SaaS usage.
8. Billing / Stripe is blocked and must not be touched until the automation foundation is known-good.

Architecture verdict:

```text
Good direction:
- account-scoped design
- multi-account aware collections
- diagnostics tooling
- TTL on high-volume transient logs
- media catalog / round-robin to avoid latest-25 limitation

Not yet fully safe:
- needs final production verification
- needs backup plan
- needs webhook reliability/Meta access solution before scale
- needs one local project-history source read by all coding agents
```

---

## 4. Known connected Instagram test accounts

Current two test accounts under the same owner:

```text
muhammad_gehad
instagram_account_id_partial = 178...615

mogehad17
instagram_account_id_partial = 178...342
```

Important historical issue:

```text
mogehad17 appeared to work,
while muhammad_gehad appeared to skip or stop.
```

Current interpretation from the latest evidence:

- This did not automatically prove rule matching was broken.
- In several cases the system was seeing old comments, bot-own replies, or already-processed comments.
- These exit before rule loading by design.
- Stop Point sometimes surfaced the later dedupe skip instead of the earlier successful automation.
- A true diagnosis requires the actual fresh `comment_id` and `media_id` from the test comment.

---

## 5. Main Instagram automation behavior

### 5.1 Comment-to-DM flow

Expected flow:

```text
User connects Instagram Professional account
→ creates active automation rule
→ external user comments on eligible Instagram media
→ MyChat posts public reply
→ MyChat sends opening DM
→ session/quick replies/clicks/logs are tracked
```

For a general rule:

```text
trigger = comment:any
post_scope = any
```

Expected product behavior:

```text
A new comment on ANY post owned by that Instagram account should be eligible,
even if the post is old.
```

Important safety rule:

```text
Historical existing comments must not be processed
unless process_existing_unreplied_comments is explicitly enabled.
```

### 5.2 Dedupe behavior

Dedupe must prevent duplicate sends on the same physical comment/session.

Known dedupe concept:

```text
opening_dedupe_key is based on:
user_id
instagram_account_id
automation_id
media_id
commenter_id
```

This must stay strict.

Do not weaken dedupe globally unless a real false-success bug is proven.

Phase 2D - Opening DM Same-User Cooldown:

```text
COMMENT_DM_OPENING_DEDUPE_WINDOW_SECONDS defaults to 86400 seconds
and is clamped to 3600..604800.

The opening DM dedupe key remains:
user_id + instagram_account_id + automation_id/rule_id + media_id + commenter_id

Within that cooldown, repeated physical comments from the same commenter
on the same media/rule/account may still receive public replies according
to existing comment-level policy, but opening DM is skipped with:
opening_dm_already_sent_for_commenter_media
```

Important:

- `COMMENT_DM_COMPLETED_FLOW_REOPEN_TTL_SECONDS` is only for internal continuation/reopen logic and must not permit a second opening DM inside the opening cooldown.
- Different commenters on the same media can still receive their own opening DM.
- The same commenter on a different media can still receive an opening DM if existing product policy allows.
- Webhook and polling paths must behave the same.
- Billing, HMAC, exact-comment dedupe, rate limits, quick reply copy, Dashboard/frontend, and username-specific behavior are unchanged.

### 5.3 Early returns before rule loading

`_handle_new_comment` can exit before rule loading when the comment is:

- bot's own reply
- historical
- already replied successfully
- already DM'd / completed
- otherwise dedupe-blocked

This is expected behavior. It means `rules_loaded=false` can coexist with `rules_count=1` because they come from different event stages.

---

## 6. Webhook + polling architecture

Target architecture:

```text
Webhook-first
Polling fallback
Same handler after normalization
Same account-scoped rule loading
Same dedupe and send logic
```

Webhook status:

- Webhook handlers exist.
- Account resolver is designed to map event payloads to the correct Instagram account.
- There has been a known production issue where comment webhooks are globally observed but not mapped to the target accounts.
- This is likely related to Meta subscription / permissions / Advanced Access rather than necessarily MyChat logic.
- Polling fallback has compensated in real tests.

Polling status:

- Polling runs per account.
- Polling is account-isolated.
- Polling historically scanned only latest 25 media.
- That latest-25 behavior was a product correctness bug for `comment:any + post_scope:any`.

Scaling warning:

```text
Polling-only can work for small testing,
but it will not scale to hundreds/thousands of users.
Reliable webhook mapping is a hard requirement before serious SaaS launch.
```

---

## 7. Latest-25 limitation and Phase 2 fix

### Old problem

Before Phase 2:

```text
_fetch_recent_media_ids hard-capped at 25
_collect_target_media_ids only used recent media
_poll_user_comments scanned media_ids[:25]
```

Result:

```text
comment:any + post_scope:any
actually meant:
any new comment on the latest 25 posts only
```

This was wrong.

A comment on the 26th-most-recent post could never be seen by polling.

### Phase 2 solution

Commit:

```text
ed3d842
```

Implemented architecture:

```text
instagram_media_catalog
+ recent media slice
+ selected media pinned every tick
+ older media round-robin batch
```

Design:

```text
_recent media_:
Polled every tick.

_selected/pinned media_:
Always included, even if not recent.

_round-robin catalog media_:
Older media covered gradually over multiple ticks.
```

Expected effect:

```text
For 60 posts and batch 25:
ceil(60 / 25) = 3 ticks ≈ 45 seconds

For 1000 posts:
ceil(1000 / 25) = 40 ticks ≈ 10 minutes
```

### New collection

```text
instagram_media_catalog
```

Fields:

```text
user_id
instagramAccountDbId
instagramAccountId
igUserId
media_id
media_timestamp
permalink
media_type
last_seen_at
created
updated
```

Indexes:

```text
instagram_media_catalog_account_media_unique
instagram_media_catalog_account_timestamp
```

No TTL on this collection. Historical media metadata is load-bearing.

### Cursor state

Stored on:

```text
instagram_accounts.pollingRoundRobinCursor
instagram_accounts.pollingRoundRobinCursorUpdatedAt
```

### Env knobs

```text
IG_POLL_RECENT_MEDIA_LIMIT = 25
IG_POLL_ROUND_ROBIN_BATCH = 25
IG_MEDIA_CATALOG_SYNC_INTERVAL_SECONDS = 600
IG_MEDIA_CATALOG_MAX_PAGES = 20
```

Emergency option:

```text
IG_POLL_ROUND_ROBIN_BATCH=0
```

This disables round-robin while keeping recent + pinned media.

### Tests

Phase 2 added 7 tests.

Reported full backend result:

```text
710 passed
0 failed
```

### Current verification requirement

First live check:

```bash
curl https://backend-production-a1a3.up.railway.app/api/version
curl https://backend-production-a1a3.up.railway.app/api/
```

Expected if Phase 2 is live:

```text
/api/version starts with ed3d842
/api/ returns ok
```

If `/api/version` still starts with `99ee2fc`, Railway did not deploy Phase 2 or deploy failed.

---

## 7B. Phase 2B stale polling comment guard

After Phase 2 media catalog + round-robin, polling can discover old comments on old posts.

To prevent delayed replies to historical comments, polling skips comments older than:

```text
IG_POLL_FRESH_COMMENT_WINDOW_SECONDS
```

Default:

```text
3600 seconds
```

Clamp:

```text
60 to 21600 seconds
```

This applies only to:

```text
source = polling
```

Webhook events are not blocked.

Explicit selected-post catch-up remains allowed:

```text
process_existing_unreplied_comments = true
```

Skip reason:

```text
stale_polling_comment
```

Unchanged:

- dedupe
- HMAC
- Billing
- rate limits / send pacing
- quick reply copy
- account scoping
- webhook handling

Product behavior remains:

```text
A NEW comment on ANY owned post is eligible, even if the post itself is old.
Historical existing comments are skipped unless explicit catch-up is enabled.
```

---

## 7C. Phase 2C-A early polling filter and scan summaries

Production `237e549` prevents stale polling comments from sending delayed replies, but polling can still create noisy deep traces for old comments.

Phase 2C-A keeps polling recovery enabled while making it quieter and more webhook-first:

```text
_poll_user_comments
-> classify provider-timestamped polling comment against active rules
-> if historical/stale and process_existing_unreplied_comments is false:
   summarize only
-> otherwise call _handle_new_comment
```

Filtered polling rows do not emit full per-comment automation traces:

```text
poller_comment_seen
dedupe_checked
rule_loading_started
rule_loading_finished
rule_candidate_evaluated
automation_skipped
```

Instead, the poller emits one compact event:

```text
stage = polling_scan_summary
```

Important summary fields:

- media_checked
- comments_seen
- fresh_candidates
- historical_skipped
- stale_skipped
- bot_own_skipped
- already_processed_skipped
- comments_sent_to_handle_new_comment
- automation_success_count
- errors_count
- target_media_count
- recent_media_limit
- round_robin_batch_size
- round_robin_cursor_position
- total_known_media_count

Full traces are still required for:

- webhook comments
- fresh polling comments
- missing-timestamp polling comments that cannot be safely prefiltered
- explicit selected-post catch-up
- send attempts
- send failures
- automation_success
- unexpected errors

Stop Point should read `polling_scan_summary` and report:

```text
only_historical_or_stale_comments_seen
```

or:

```text
no_fresh_comment_seen_in_poll
```

instead of `rule_not_matched` when no fresh polling comment was processed.

Unchanged:

- dedupe semantics
- HMAC
- Billing
- rate limits / send pacing
- quick reply copy
- Dashboard/frontend
- username/account-specific behavior

---

## 8. MongoDB quota incident and storage

### Incident

MongoDB Atlas Free Tier reached:

```text
512MB / 512MB
```

Writes were blocked.

Error pattern:

```text
OperationFailure:
you are over your space quota
writes are blocked on your cluster
```

Affected paths:

```text
/api/instagram/webhook
/api/cron/refresh-instagram-tokens
_poll_user_comments
_handle_new_comment
db.comments.update_one
db.webhook_log.insert_one
```

Root cause:

```text
instagram_automation_events grew to about 465MB
```

Manual recovery:

```text
Drop instagram_automation_events
```

Permanent fix:

```text
TTL index on instagram_automation_events
```

Index:

```text
ttl_instagram_automation_events_created
expireAfterSeconds = 604800
retention = 7 days
```

Commit involved:

```text
ce901be
```

### Remaining storage risks

Known remaining areas to evaluate:

- `webhook_processing_failures` / DLQ growth
- `link_click_events` raw growth
- `comment_dm_sessions` closed sessions
- Atlas backups not configured on free tier

Launch blocker:

```text
Move to a paid Atlas tier with backups OR implement a reliable nightly backup strategy.
```

Do not TTL:

- `comments` if it holds dedupe proof
- `dm_logs` if it holds dedupe proof
- `users`
- `instagram_accounts`
- `automations`
- `dm_rules`
- `contacts`
- `usage_events` / billing-critical ledgers
- `data_deletion_requests`

---

## 9. Phase 1 SaaS infrastructure hardening

Commit:

```text
99ee2fc
```

Reported production state:

```text
/api/version returned 99ee2fc
/api/ returned ok
703 passed
0 failed
```

Key changes:

1. Single-tenant webhook fallback disabled by default in production.
2. Story Reply account scoping fixed.
3. TTL indexes added for transient collections.
4. Stop Point reporting fixes added.

Important constraint:

```text
Phase 1 did not touch:
Billing
sending logic
dedupe semantics
HMAC
quick reply copy
```

Phase 1 goal:

```text
Stability and SaaS safety before new features.
```

---

## 10. Stop Point / Flight Recorder / diagnostics

Implemented diagnostics include:

- Multi-account health
- Automation Stop Point
- Automation Flight Recorder
- Rule Coverage Inspector
- Webhook Health
- system health / observability status
- media/rule diagnostics

Stop Point issues discovered:

1. Successful automation can later appear as `already_replied_success` because a later polling tick re-saw the already-processed comment.
2. A fresh comment may not be seen by polling if its media was outside target media coverage before Phase 2.
3. Historical comments can confuse summary labels.
4. `rules_count=1` does not mean `rules_loaded=true`; the former can come from poller scan start, while the latter needs `_handle_new_comment` to reach rule loading for the selected comment.

Important interpretation rule:

```text
Stop Point is a summary view.
Flight Recorder is closer to the truth.
Actual comment_id + media_id timeline is required before declaring a functional bug.
```

If a fresh comment is skipped, inspect:

```text
db.comments:
reply_status
dm_status
action_status
replied
provider_reply_id
reply_provider_id
provider_message_id
opening_message_id
last_send_error
previous_skip_reason
opening_dedupe_key
```

If `reply_status=success` but no provider proof exists, classify as possible false-success state and propose a generic repair only.

---

## 11. Cron / token refresh

Cron service:

```text
instagram-token-refresh-cron
```

Purpose:

```text
Refresh Instagram long-lived tokens
```

Relevant endpoint:

```text
POST /api/cron/refresh-instagram-tokens
Authorization: CRON_SECRET
```

Known incident:

```text
Cron run failed with Status: 500
CRON_SECRET exists: true
CRON_SECRET length: 48
```

Likely cause at the time:

```text
MongoDB quota incident caused backend writes to fail.
```

Required check after recovery:

```text
Re-run/restart cron.
Confirm status = 200.
If still 500, investigate /api/cron/refresh-instagram-tokens.
```

This is important before launch because expired Instagram tokens will silently break automation.

---

## 12. Auth, security, and user account features

Implemented / present:

- Email/password login.
- Signup.
- Google Sign-In as additive auth.
- Email verification.
- Password reset.
- Password change with current password verification.
- Session version increment to revoke old sessions.
- Profile editing for display fields.
- Notification preferences.
- Suspended/deleted user protection.
- Stronger validation max-lengths in Phase 2.19.
- Production OpenAPI/docs disabled unless env flag allows.
- Security headers and no-store cache headers on authenticated/admin routes.
- Sentry optional observability with scrubber.
- Token/authorization/raw comment/DM text redaction in observability.

Important security constraints:

- Do not disable HMAC verification.
- Do not log tokens.
- Do not print raw webhook payloads or raw user messages in diagnostics.
- Do not expose full Instagram account IDs in UI/logs; use safe partials.
- Do not weaken rate limits.

Known warning:

- In-memory rate limits are acceptable for a single Railway replica.
- For multi-replica scale, move rate limiting to Redis/Mongo/shared store.

---

## 13. Admin console and roles

Admin console exists.

Admin permissions/roles include:

```text
owner
admin
support
viewer
user
```

Permissions include:

```text
admin.overview.view
admin.users.view
admin.users.manage
admin.plans.assign
admin.automations.disable
admin.failures.view
admin.audit.view
admin.members.view
admin.members.manage
admin.owner.manage
```

Admin bootstrap:

```text
ADMIN_EMAILS
```

Admin endpoints are protected and sanitized.

Important admin caution:

```text
Admin endpoints are operator-level tools, not customer-facing tenant admin tools.
Do not expose them to normal users.
```

Known admin tools:

- User management
- Plans / usage overview
- Multi-account health
- Automation Stop Point
- Flight Recorder
- Rule Coverage
- Webhook Health
- Admin audit logs
- Force-disconnect Instagram account for orphan/locked rows
- Repair tools behind env flag or admin access

---

## 14. Dashboard, analytics, and performance

Dashboard work done:

- KPI cards.
- Secondary KPI row.
- Range selector: 24h / 7d / 30d / all.
- Conversion-rate formula fixed to use same time window for numerator and denominator.
- Top Automations sorting fixed by sent count, active status, created date.
- Multi-account dashboard scoping fixed to avoid cross-account contact leakage.
- Dashboard cache key includes range.
- Time formatting fixed through centralized `frontend/src/lib/dateTime.js`.
- Admin and dashboard timestamps no longer expose raw ugly ISO strings in the main UI.
- Performance fixes:
  - dashboard summary backend aggregation parallelized
  - Mongo projections/indexes added
  - X-Response-Time header
  - slow request logging
  - frontend route preloading
  - removed overly aggressive 8-second timeout on dashboard summary

Known UI phrase from old issue:

```text
Showing cached dashboard data. Refresh failed.
```

This came from stale-while-revalidate behavior and backend timeouts.

Current caution:

```text
Dashboard looks improved, but live browser smoke tests are still required after each deploy.
```

---

## 15. Standalone DM Automation

Implemented direction:

```text
DM Automation separate from comment automation.
```

Features:

- Dashboard → Instagram DM Automation page.
- Keyword-triggered DM rules.
- Match modes:
  - exact
  - contains
  - starts_with
- Auto-reply message body.
- Activation toggle.
- Separate logs/endpoints for DM execution path.

Needs verification:

- Messaging webhooks subscribed correctly.
- Account resolver maps sender/recipient IDs correctly.
- DM rules are account-scoped.
- No sibling-account DM ping-pong.
- Logs do not expose raw message bodies.

---

## 16. Quick replies / sessions / follow-up flows

Known requirements:

- Comment automation opens DM.
- Quick reply / button flows continue the conversation.
- Completed sessions and pending sessions use TTL/dedupe to prevent spam.
- There was a prior stale-session reset tool / diagnostics flow.
- Anti-spam pacing must remain unchanged unless proven bug.

Hard constraint:

```text
Do not change quick reply copy.
Do not weaken anti-spam pacing.
Do not delete broad data.
Any reset must be explicit, scoped, previewed, and confirmed.
```

---

## 17. Billing / plans / usage

Billing status:

```text
Blocked / not started / must not be touched now.
```

Existing product infrastructure:

- `user_plans` collection exists.
- Usage events / monthly usage exist.
- Reservation ledgers exist.
- Usage counters and admin usage drilldowns exist.
- Plan appears as `free` in current admin usage response.
- `billing_enabled: False`.

Do not touch:

```text
Billing
Stripe
Payment logic
Plan charging
```

Until:

1. Instagram automation is known-good.
2. Multi-account isolation is proven.
3. Storage/backup risks are solved.
4. Cron/token refresh is healthy.
5. Production smoke tests pass.

---

## 18. Known commits / milestones from available history

These are not a complete git log. Agents must inspect git directly.

Known milestones:

```text
3e71de0
- stale session / recent test flows / reset diagnostics
- full backend suite reported 545 passed

61103fb
- Admin Rule Coverage UI verified in live bundle
- no UI code bug when tab not visible; permission-gated like Stop Point

72bc25c
- added admin-protected rule-coverage-inspector
- production verified
- billing blocked

3b076f6
- fixed admin summarizer to prefer latest successful automation status
- reporting-only; no execution change
- backend tests 635 passed

138abb6
- polling coverage widened from 10 to 25
- fresh-comment prioritization / docs around known-good

ac61705
- docs follow-up after 138abb6

4912756
- dashboard/admin timestamp formatting
- frontend tests 198 passed
- backend not touched

ce901be
- TTL for instagram_automation_events
- production confirmed post-TTL build in one audit

99ee2fc
- Phase 1 SaaS hardening
- 703 passed
- production reportedly live

ed3d842
- Phase 2 media catalog + round-robin polling
- 710 passed
- pushed, but production deployment still required verification
```

Important:

```text
Do not trust this list as canonical.
Every coding agent must run:
git status
git log --oneline -20
git branch --show-current
git rev-parse HEAD
curl /api/version
```

---

## 19. Current must-do verification checklist

Before any new feature:

### A. Production version

```bash
curl https://backend-production-a1a3.up.railway.app/api/version
curl https://backend-production-a1a3.up.railway.app/api/
```

Expected for Phase 2:

```text
build_sha starts with ed3d842
health ok
```

### B. Atlas verification

After Phase 2 deploy, verify:

```text
collection exists:
instagram_media_catalog

indexes:
instagram_media_catalog_account_media_unique
instagram_media_catalog_account_timestamp

instagram_accounts fields:
pollingRoundRobinCursor
pollingRoundRobinCursorUpdatedAt
lastMediaCatalogSyncAt
```

### C. Sentry

Check no new critical errors:

```text
OperationFailure
over quota
_sync_instagram_media_catalog
_media_catalog_known_ids
_collect_target_media_ids
_poll_user_comments
_handle_new_comment
```

### D. Cron

Run/restart:

```text
instagram-token-refresh-cron
```

Expected:

```text
Status 200
```

### E. Smoke test 1 — old post on muhammad_gehad

1. Choose an old post outside latest 25.
2. Use a fresh external Instagram tester.
3. Post a new comment.
4. Wait:

```text
ceil(total_known_media_count / 25) × 15 seconds
```

Expected:

```text
public reply lands
opening DM arrives
Stop Point = automation_success
fresh_comment_seen_in_last_poll = true
```

### F. Smoke test 2 — recent post on mogehad17

Expected:

```text
public reply lands
opening DM arrives
automation_success
```

### G. Smoke test 3 — cross-account isolation

After muhammad_gehad success, check mogehad17 Flight Recorder.

Expected:

```text
muhammad_gehad media_id_partial does NOT appear under mogehad17
muhammad_gehad comment_id_partial does NOT appear under mogehad17
```

If leakage appears:

```text
Major regression.
Rollback immediately.
```

---

## 20. Root-cause decision matrix for fresh comment issues

When a test fails, choose exactly one:

A. Correct behavior: only old/already-processed/bot-own comments were seen.  
B. Fresh comment not yet reached by round-robin media batch.  
C. Fresh comment reached polling but was incorrectly dedupe-skipped.  
D. Rule query/scoping bug.  
E. Webhook mapping asymmetry, polling compensates.  
F. Real cross-account routing bug.  
G. Admin reporting-only issue.  
H. Insufficient evidence; list exactly what output is missing.

Do not implement a fix before this classification.

---

## 21. If a fix is needed

Before changing code, every agent must provide:

```text
1. Exact root cause
2. Exact file/function
3. Smallest generic account-agnostic fix
4. Tests to add
5. Regression risks
6. Deployment notes
7. Rollback plan
```

Allowed generic fixes:

- improve media catalog / round-robin diagnostics
- make full target media list visible
- fix account-scoped rule query
- fix false dedupe provider-proof repair
- fix webhook account alias mapping for all accounts
- improve Stop Point distinction:
  - old skip
  - fresh skipped
  - dedupe proof missing
  - webhook unmapped but polling compensated
- add tests for fresh old-post comments on multiple accounts

Forbidden:

- no username-specific fix
- no per-account branch
- no Billing change
- no HMAC weakening
- no quick reply copy change
- no unsafe rate-limit loosening
- no unbounded scan of all posts every 15 seconds
- no broad DB deletes

---

## 22. Local project-history system to add to the repo

Create or maintain:

```text
docs/project-history/
  README.md
  00_CURRENT_STATE.md
  01_CHANGELOG.md
  02_KNOWN_GOOD_VERSIONS.md
  03_INCIDENTS.md
  04_AGENT_HANDOFF.md
  05_ROLLBACK_PLAYBOOK.md
  06_ARCHITECTURE_NOTES.md
  07_PRODUCTION_CHECKLIST.md
  08_FEATURE_MATRIX.md
  09_DECISIONS.md
  10_TESTING_LOG.md
  11_OPEN_RISKS.md
  12_OPERATOR_CHECKLISTS.md
  AGENT_RULES.md
```

Recommended purpose:

### `README.md`

Explains that these files are mandatory reading before any agent edits code.

### `00_CURRENT_STATE.md`

Current production SHA, current repo SHA, current known-good state, current blockers.

### `01_CHANGELOG.md`

Human-readable change log, one entry per commit/phase.

### `02_KNOWN_GOOD_VERSIONS.md`

Known-good commits with:
- what passed
- which tests passed
- production verification
- rollback target
- known limitations

### `03_INCIDENTS.md`

Mongo quota incident, Stop Point reporting issue, webhook mapping issue, etc.

### `04_AGENT_HANDOFF.md`

Copy-paste prompt for Claude/Codex/ChatGPT.

### `05_ROLLBACK_PLAYBOOK.md`

Exact rollback commands and criteria:
- when to revert Phase 2
- when to disable round-robin
- how to recover cron
- what not to delete

### `06_ARCHITECTURE_NOTES.md`

Account scoping, dedupe keys, webhook vs polling, media catalog design, event lifecycle.

### `07_PRODUCTION_CHECKLIST.md`

Deployment verification checklist.

### `08_FEATURE_MATRIX.md`

Feature-by-feature status.

### `09_DECISIONS.md`

Architecture decisions and why.

### `10_TESTING_LOG.md`

Manual smoke test results with date/account/comment/media.

### `11_OPEN_RISKS.md`

Launch blockers and non-blockers.

### `12_OPERATOR_CHECKLISTS.md`

Atlas/Sentry/Railway/Admin UI checklists.

### `AGENT_RULES.md`

Hard constraints for all coding agents.

---

## 23. Suggested feature matrix

| Area | Status | Confidence | Next action |
|---|---:|---:|---|
| Core comment-to-DM automation | Implemented | Medium-high | Final smoke on both accounts |
| Public reply send | Implemented | Medium-high | Verify provider proof |
| Opening DM send | Implemented | Medium | Verify provider proof |
| Quick replies / DM flow | Implemented | Medium | Do not change copy; smoke fresh sessions |
| Standalone DM Automation | Implemented | Medium | Verify messaging webhook subscriptions |
| Multi-account scoping | Improved | Medium-high | Cross-account Flight Recorder proof |
| Webhook comments | Implemented but mapping gap | Medium-low | Meta subscription/Advanced Access check |
| Polling fallback | Implemented | High for small scale | Phase 2 live verification |
| Media catalog / round-robin | Implemented/pushed | Medium | Confirm deploy + Atlas + smoke tests |
| Stop Point | Implemented | Medium | Treat as summary, not sole truth |
| Flight Recorder | Implemented | High | Use for timeline proof |
| Rule Coverage Inspector | Implemented | High | Use for rule-shape debugging |
| Admin Console | Implemented | Medium-high | Continue permission/live UI checks |
| Dashboard | Improved | Medium-high | Live UX/browser checks |
| Auth email/password | Implemented | High | Standard regression tests |
| Google Sign-In | Implemented additive | Medium | Verify config/env |
| Password reset/change | Implemented | High | Keep token redaction |
| Observability/Sentry | Implemented optional | Medium | Verify DSN/env and scrubber |
| Token refresh cron | Implemented | Medium-low | Re-run after Mongo recovery |
| Billing/Stripe | Blocked/not started | N/A | Do not touch yet |
| Backups | Missing/critical | Low | Add Atlas backup strategy |

---

## 24. Agent bootstrap prompt

Paste this to Claude/Codex before any work:

```text
You are working on MyChat.

Before making any assumptions or code changes, read:

docs/project-history/README.md
docs/project-history/00_CURRENT_STATE.md
docs/project-history/01_CHANGELOG.md
docs/project-history/02_KNOWN_GOOD_VERSIONS.md
docs/project-history/03_INCIDENTS.md
docs/project-history/04_AGENT_HANDOFF.md
docs/project-history/05_ROLLBACK_PLAYBOOK.md
docs/project-history/06_ARCHITECTURE_NOTES.md
docs/project-history/07_PRODUCTION_CHECKLIST.md
docs/project-history/08_FEATURE_MATRIX.md
docs/project-history/09_DECISIONS.md
docs/project-history/10_TESTING_LOG.md
docs/project-history/11_OPEN_RISKS.md
docs/project-history/AGENT_RULES.md

Then run:

git status
git branch --show-current
git log --oneline -20
git rev-parse HEAD
curl https://backend-production-a1a3.up.railway.app/api/version
curl https://backend-production-a1a3.up.railway.app/api/

Do not assume you know changes made by another agent.
Do not implement before auditing.
Do not special-case usernames.
Do not touch Billing.
Do not change HMAC verification.
Do not change dedupe semantics unless you prove a bug.
Do not change quick reply copy.
Do not loosen rate limits recklessly.
Do not unbound polling over all posts every 15 seconds.

Current core requirement:
All connected Instagram accounts must follow the same generic account-agnostic pipeline.

For trigger=comment:any and post_scope=any:
A new comment on ANY post of that account is eligible, even if the post is old.
Historical existing comments remain skipped unless process_existing_unreplied_comments is explicitly enabled.

Current priority:
Verify whether Phase 2 media catalog / round-robin polling commit ed3d842 is live and known-good.
If not live, do not assume it is.
If live, verify catalog, indexes, cursor, Sentry, cron, and smoke tests.

Required report before any fix:
A. Current production version
B. Git state
C. What is verified from code
D. What is verified from production data
E. Side-by-side account comparison
F. Exact divergence point, if any
G. Root cause conclusion A-H
H. Proposed fix, if needed
I. Tests to add/run
J. Deployment/rollback notes
K. What remains before declaring known-good
```

---

## 25. Immediate next best step

The next best step is not a new feature.

It is:

```text
1. Put this project-history system inside the repo.
2. Make every agent read it before edits.
3. Verify production version.
4. Verify Phase 2 live state.
5. Run smoke tests on muhammad_gehad and mogehad17.
6. Update 02_KNOWN_GOOD_VERSIONS.md only after evidence.
```

Until that is done, do not build Billing, do not redesign automation, and do not add more complexity.

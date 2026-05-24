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
- Fix commit: Pending local stop-point summary fix.
- Tests: `test_summarize_stop_point_prefers_external_comment_over_bot_reply`; focused multi-account tests 92 passed; full backend 623 passed.
- Deploy status: Local, deploy required if accepted.
- Lesson learned: Bot-owned reply events should remain in the flight recorder but must not be treated as the primary support summary when a real external commenter event exists.
- Preventive test added: Yes.

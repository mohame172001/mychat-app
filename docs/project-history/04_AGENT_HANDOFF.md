# Agent Handoff Instructions

Read this file first in every Claude Code or Codex session.

## Mandatory First Steps

1. Do not rely on chat memory.
2. Run:

```powershell
git status --short
git rev-parse HEAD
git rev-parse origin/master
Invoke-WebRequest -Uri "https://backend-production-a1a3.up.railway.app/api/version" -UseBasicParsing
Invoke-WebRequest -Uri "https://backend-production-a1a3.up.railway.app/api/" -UseBasicParsing
```

3. Read:
   - `docs/project-history/00_CURRENT_STATE.md`
   - `docs/project-history/02_KNOWN_GOOD_VERSIONS.md`
4. Report the current state before editing unless the operator explicitly asked for immediate code changes.

## Required Updates After Work

- Always update `00_CURRENT_STATE.md` and `01_CHANGELOG.md` after any meaningful commit.
- If an incident is fixed, update `03_INCIDENTS.md`.
- If production is verified, record the production `build_sha`.
- If a rollback point becomes known-good, update `02_KNOWN_GOOD_VERSIONS.md`.

## Permanent Constraints

- Never start Billing unless explicitly requested by the operator.
- Never mark Billing ready while P0/P1 product, auth, or performance blockers remain.
- Never reintroduce `/app/admin/instagram-diagnostics`.
- Never weaken webhook HMAC verification.
- Never remove dedupe.
- Never remove rate limits or anti-spam send pacing.
- Never log tokens, secrets, authorization headers, full webhook payloads, full DM bodies, passwords, or reset tokens.
- Never use `users.active_instagram_account_id` for webhook execution.
- Never delete legacy fallback behavior without a tested migration.

## Instagram Automation Safety Rules

- Account switching in the UI affects dashboard context only.
- Webhook and postback execution must resolve the Instagram account from the event.
- Any-post and post-specific automations must use the same execution pipeline after matching.
- Normal external Instagram users must not be required to exist in `instagram_accounts`.
- Protected backend support endpoints may exist, but frontend diagnostics UI must stay hidden/removed.

# Architecture Notes

## Instagram Comment-To-DM Flow

1. Instagram sends webhook to `POST /api/instagram/webhook`.
2. Backend verifies HMAC before processing.
3. Backend parses comment or messaging event.
4. Account resolver identifies the connected Instagram business account from event identity, not from UI active account.
5. Active automations are loaded for that account.
6. Rule matching selects any-post or post-specific rule.
7. After a rule matches, execution pipeline is shared:
   - optional public comment reply
   - opening DM
   - comment-DM session creation when rule is deferred/button flow
   - dedupe and usage tracking

## Webhook Path

- Webhook path is the fast path.
- It must not rely on polling or dashboard active account.
- It must not perform heavy dashboard aggregation or broad account scans.
- It must use sanitized structured logs only.

## Polling Fallback

- Polling scans valid connected accounts as a fallback.
- Each account must use its own token and rules.
- Polling must share the same comment processing pipeline as webhook once a comment is found.
- Polling should not become the normal speed path if webhook is healthy.

## Multi-Account Resolution

Canonical source of truth:

- `instagram_accounts`
- `users.active_instagram_account_id` for UI context only

Compatibility fields may exist:

- `userId`
- `user_id`
- `instagramAccountId`
- `igUserId`
- `ig_user_id`
- user-level legacy Instagram fields

Webhook execution must resolve by event account identity, not active UI account.

## Any-Post Versus Post-Specific Rules

Only match condition differs:

- Any-post matches eligible comments on any account-owned post.
- Post-specific matches only selected `media_id` or equivalent alias.

After matching, both must call the same execution pipeline.

## External User Quick Reply Continuation

The clicker is an external Instagram user/contact.

The clicker must not be required to exist in:

- `instagram_accounts`
- MyChat users
- connected owner accounts

On quick reply:

- resolve business account from recipient context
- treat sender as external contact
- locate session by payload/session id, then by account plus external sender fallback
- send next message from the same connected business account

## Dedupe And Session Model

Dedupe namespace must include enough fields to prevent cross-account contamination:

- owner user id
- Instagram account id
- automation/rule id
- media id
- commenter id
- comment/message id where relevant

Completed sessions should block immediate duplicates but must not block fresh live tests forever.

## Automation Stop-Point Support

Protected backend support endpoints may expose sanitized account health and stop-point summaries.

Frontend diagnostics UI must remain hidden or removed in production.

## Admin UI Constraints

- Keep owner/admin console protected.
- Do not expose internal diagnostics route at `/app/admin/instagram-diagnostics`.
- Do not show raw provider payloads, tokens, secrets, or full message bodies.

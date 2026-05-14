# Canonical Field Migration Plan

Generated: 2026-05-14

This is a deferred migration plan. Do not remove legacy fallback fields until a tested migration proves no production tenant still depends on them.

## Current Mixed Identifiers

- User identifiers: `user_id`, `userId`
- Instagram account identifiers: `instagramAccountId`, `igUserId`, `ig_user_id`, `instagram_account_id`, `instagramAccountDbId`, `accountId`
- Email identifiers: `email`, `normalized_email`

## Target Canonical Fields

- Backend-owned user reference: `user_id`
- Instagram business account reference: `instagramAccountId`
- Auth lookup: `normalized_email`

## Migration Steps

1. Add read-only diagnostics that count documents by legacy/canonical field presence.
2. Add backfill script that writes canonical fields without deleting legacy fields.
3. Add indexes for canonical query shapes.
4. Run backfill in staging, then production.
5. Switch writes to canonical fields only while reads keep fallback.
6. Add tests proving old and new documents resolve to the same owner/account.
7. After an observation window, consider removing legacy fallbacks in a separate phase.

## Deferred Risk

Current fallback logic is intentionally retained. The main risk is query/index complexity, not immediate account isolation failure. Billing can proceed only if this remains documented and no P0/P1 IDOR is found.

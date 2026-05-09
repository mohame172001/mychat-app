# Phase 2.12 Security Hardening Notes

Baseline: `bd4dd80`.

## Fixed in This Phase

- Existing JWTs for users later marked `suspended` or `deleted` are rejected by the shared active-user dependency.
- Risky endpoints now have explicit throttles: Instagram connect URL generation, retry public reply, Meta data deletion callback, admin reconciliation, and admin usage-subject backfill.
- Frontend user/account data caches are cleared on login, signup, Google login, logout, malformed stored user state, and 401 auth reset.
- Direct vulnerable frontend `axios` dependency was patched to `1.15.2`.
- Direct vulnerable backend `pymongo` dependency was patched to `4.6.3`.
- Incident response runbook added for token leaks, admin compromise, webhook abuse, raw log leaks, deletion escalation, rollback, and Railway env rotation.

## Verified Existing Controls

- Canonical Instagram account identity remains `instagram_accounts.instagramAccountId`; `igUserId` and `users.ig_user_id` are legacy aliases.
- Duplicate active Instagram ownership is guarded by app logic and the startup index plan from the account-limit phase.
- Account-scoped usage/backfill remains non-destructive and dry-run by default.
- Tracked links resolve from stored short codes and only accept stored `http` or `https` destinations.
- Webhook HMAC, webhook verify token, production docs disabling, CORS allowlist handling, and security headers are present in backend configuration.
- Admin repair tools default disabled unless `ENABLE_ADMIN_REPAIR_TOOLS=true`.
- Repo secret scan found expected placeholders/config references only, not committed live secret values.

## Deferred Findings

- `starlette` advisories remain through the current FastAPI pin. Fixing them requires a coordinated FastAPI/Starlette compatibility upgrade and full regression pass.
- Frontend audit still reports transitive CRA/serve toolchain advisories (`fast-uri`, `serialize-javascript`, `underscore`, `@tootallnate/once`). These are transitive build/runtime-tool dependencies and should be handled in a toolchain modernization pass rather than a forced major override.
- Mongo backup posture and restore procedure must be confirmed in Railway/Mongo provider settings; no destructive backup or restore action was performed.

## Production Rollout Checklist

1. Deploy backend and frontend from the security hardening commit.
2. Confirm backend startup creates/keeps the Instagram-account unique partial index.
3. Run the Instagram usage-subject backfill endpoint in dry-run mode.
4. Review admin reconciliation for unmapped or ambiguous legacy usage.
5. Confirm production flags: HMAC enforcement enabled, webhook verify token set, `ENABLE_ADMIN_REPAIR_TOOLS` false or unset, `DEBUG` false.
6. Monitor sanitized logs for `instagram_account_already_connected`, `rate_limit_hit`, authorization-denied, duplicate webhook, and retry-reply events.

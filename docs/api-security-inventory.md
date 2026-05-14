# API Security Inventory

Generated: 2026-05-13

Total routes inventoried: 115 API routes plus the tracked-link redirect route.

No unauthenticated sensitive mutation route was identified in this inventory. Public routes are constrained by credentials, verification token/HMAC, signed request behavior, short code lookup, or generic responses.

| Method | Path | Auth required | Admin permission if any | Resource ownership check | Rate limit | Input validation | Sensitive output risk | Test coverage |
|---|---|---|---|---|---|---|---|---|
| POST | /api/meta/data-deletion | no/public constrained | n/a | n/a or token/signature/short-code scoped | yes/bounded | yes | low | smoke/security tests |
| POST | /api/auth/signup | no/public constrained | n/a | n/a or token/signature/short-code scoped | yes/bounded | yes | low | auth tests |
| POST | /api/auth/login | no/public constrained | n/a | n/a or token/signature/short-code scoped | yes/bounded | yes | low | auth tests |
| GET | /api/auth/me | yes | n/a | current user scoped | yes/bounded | yes/basic | low | auth tests |
| POST | /api/auth/password | yes | n/a | current user scoped | yes/bounded | yes | low | password-change tests |
| POST | /api/auth/forgot-password | no/public constrained | n/a | n/a; generic response prevents account enumeration | yes/bounded | yes | low | password-reset tests |
| POST | /api/auth/reset-password | no/public constrained | n/a | hashed reset-token scoped | yes/bounded | yes | low | password-reset tests |
| POST | /api/auth/resend-verification | no/public constrained | n/a | n/a or token/signature/short-code scoped | yes/bounded | yes | low | auth tests |
| POST | /api/auth/verify-email | no/public constrained | n/a | n/a or token/signature/short-code scoped | yes/bounded | yes | low | auth tests |
| GET | /api/auth/verify-email | no/public constrained | n/a | n/a or token/signature/short-code scoped | yes/bounded | yes/basic | low | auth tests |
| GET | /api/auth/google/config | no/public constrained | n/a | n/a or token/signature/short-code scoped | yes/bounded | yes/basic | low | auth tests |
| POST | /api/auth/google | no/public constrained | n/a | n/a or token/signature/short-code scoped | yes/bounded | yes | low | auth tests |
| GET | /api/automations/summary | yes | n/a | current user scoped | bounded | yes/basic | low | automation tests |
| GET | /api/automations | yes | n/a | current user scoped | bounded | yes/basic | low | automation tests |
| POST | /api/automations | yes | n/a | current user scoped | bounded | yes | low | automation tests |
| GET | /api/automations/{aid} | yes | n/a | owner/account scoped | bounded | yes | low | automation tests |
| PATCH | /api/automations/{aid} | yes | n/a | owner/account scoped | bounded | yes | low | automation tests |
| DELETE | /api/automations/{aid} | yes | n/a | owner/account scoped | bounded | yes | low | automation tests |
| POST | /api/automations/{aid}/duplicate | yes | n/a | owner/account scoped | bounded | yes | low | automation tests |
| POST | /api/automations/quick-comment-rule | yes | n/a | current user scoped | bounded | yes | low | automation tests |
| GET | /api/contacts | yes | n/a | current user scoped | bounded | yes/basic | low | smoke/security tests |
| POST | /api/contacts | yes | n/a | current user scoped | bounded | yes | low | smoke/security tests |
| PATCH | /api/contacts/{cid} | yes | n/a | owner/account scoped | bounded | yes | low | smoke/security tests |
| DELETE | /api/contacts/{cid} | yes | n/a | owner/account scoped | bounded | yes | low | smoke/security tests |
| GET | /api/broadcasts | yes | n/a | current user scoped | bounded | yes/basic | low | smoke/security tests |
| POST | /api/broadcasts | yes | n/a | current user scoped | bounded | yes | low | smoke/security tests |
| PATCH | /api/broadcasts/{bid} | yes | n/a | owner/account scoped | bounded | yes | low | smoke/security tests |
| POST | /api/broadcasts/{bid}/send | yes | n/a | owner/account scoped | bounded | yes | low | smoke/security tests |
| GET | /api/conversations | yes | n/a | current user scoped | bounded | yes/basic | low | smoke/security tests |
| GET | /api/conversations/{cid} | yes | n/a | owner/account scoped | bounded | yes | low | smoke/security tests |
| POST | /api/conversations/{cid}/messages | yes | n/a | owner/account scoped | bounded | yes | low | smoke/security tests |
| GET | /api/comments | yes | n/a | current user scoped | bounded | yes/basic | yes, sanitized | comment/retry tests |
| POST | /api/comments/{cid}/reply | yes | n/a | owner/account scoped | yes/bounded | yes | yes, sanitized | comment/retry tests |
| GET | /api/comments/{comment_id}/diagnostics | yes | n/a | owner/account scoped | yes/bounded | yes | yes, sanitized | comment/retry tests |
| POST | /api/comments/{cid}/retry-reply | yes | n/a | owner/account scoped | yes/bounded | yes | yes, sanitized | comment/retry tests |
| GET | /api/comments/{comment_id}/why-not-replied | yes | n/a | owner/account scoped | yes/bounded | yes | yes, sanitized | comment/retry tests |
| GET | /api/comments/{comment_id}/diagnose-specific-rule-reply-plan | yes | n/a | owner/account scoped | yes/bounded | yes | yes, sanitized | comment/retry tests |
| GET | /api/admin/tools-enabled | yes | admin permission | admin target scoped | yes/bounded | yes/basic | yes, sanitized | admin/security tests |
| GET | /api/admin/comments/{ig_comment_id}/specific-reply-diagnosis | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| POST | /api/admin/comments/{ig_comment_id}/repair-specific-public-reply | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| POST | /api/admin/comments/{ig_comment_id}/process-retry-now | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| GET | /api/dashboard/metric-sources | yes | n/a | current user scoped | bounded | yes/basic | yes, sanitized | dashboard tests |
| GET | /api/dashboard/summary | yes | n/a | current user scoped | bounded | yes/basic | yes, sanitized | dashboard tests |
| GET | /api/dashboard/stats | yes | n/a | current user scoped | bounded | yes/basic | yes, sanitized | dashboard tests |
| GET | /api/usage/current | yes | n/a | current user scoped | bounded | yes/basic | low | smoke/security tests |
| GET | /api/observability/status | yes | n/a | current user scoped | bounded | yes/basic | low | smoke/security tests |
| GET | /api/plans | no/public constrained | n/a | n/a or token/signature/short-code scoped | bounded | yes/basic | low | smoke/security tests |
| GET | /api/plan/current | yes | n/a | current user scoped | bounded | yes/basic | low | smoke/security tests |
| POST | /api/admin/users/{target_user_id}/plan | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| GET | /api/admin/users/{target_user_id}/plan | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| GET | /api/admin/usage/{target_user_id} | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| GET | /api/admin/me | yes | admin permission | admin target scoped | yes/bounded | yes/basic | yes, sanitized | admin/security tests |
| GET | /api/admin/overview | yes | admin permission | admin target scoped | yes/bounded | yes/basic | yes, sanitized | admin/security tests |
| GET | /api/admin/users | yes | admin permission | admin target scoped | yes/bounded | yes/basic | yes, sanitized | admin/security tests |
| GET | /api/admin/users/{target_user_id}/detail | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| POST | /api/admin/automations/{automation_id}/disable | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| GET | /api/admin/audit-log | yes | admin permission | admin target scoped | yes/bounded | yes/basic | yes, sanitized | admin/security tests |
| GET | /api/admin/members | yes | admin member permission | admin target scoped | yes/bounded | yes/basic | yes, sanitized | admin/security tests |
| POST | /api/admin/members | yes | admin member permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| PATCH | /api/admin/members/{target_user_id} | yes | admin member permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| DELETE | /api/admin/members/{target_user_id} | yes | admin member permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| GET | /api/admin/users/{target_user_id}/limit-overrides | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| POST | /api/admin/users/{target_user_id}/limit-overrides | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| PATCH | /api/admin/users/{target_user_id}/limit-overrides/{override_id}/revoke | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| GET | /api/admin/users/{target_user_id}/effective-limits | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| POST | /api/admin/users/{target_user_id}/revoke-sessions | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| GET | /api/admin/auth/normalized-email-diagnostics | yes | admin permission | admin target scoped | yes/bounded | yes/basic | yes, sanitized | admin/security tests |
| POST | /api/admin/auth/backfill-normalized-email | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| POST | /api/admin/users/{target_user_id}/suspend | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| POST | /api/admin/users/{target_user_id}/unsuspend | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| POST | /api/admin/users/{target_user_id}/delete | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| GET | /api/admin/metrics/reconciliation | yes | admin audit/diagnostic permission | admin target scoped | yes/bounded | yes/basic | yes, sanitized | admin/security tests |
| GET | /api/admin/limits/usage-reservation-diagnostics | yes | admin audit/diagnostic permission | admin target scoped | yes/bounded | yes/basic | yes, sanitized | admin/security tests |
| POST | /api/admin/limits/backfill-instagram-usage-subjects | yes | admin audit/diagnostic permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| GET | /api/admin/limits/instagram-account-diagnostics | yes | admin audit/diagnostic permission | admin target scoped | yes/bounded | yes/basic | yes, sanitized | admin/security tests |
| GET | /api/instagram/auth-url | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| GET | /api/instagram/callback | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| GET | /api/instagram/oauth/last-attempt | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| DELETE | /api/admin/users/{email} | yes | admin permission | admin target scoped | yes/bounded | yes | yes, sanitized | admin/security tests |
| GET | /api/instagram/status | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| GET | /api/instagram/profile | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| POST | /api/instagram/subscribe-webhook | yes | n/a | current user scoped | yes/bounded | yes | yes, sanitized | smoke/security tests |
| POST | /api/instagram/subscribe-webhook-legacy | yes | n/a | current user scoped | yes/bounded | yes | yes, sanitized | smoke/security tests |
| GET | /api/instagram/force-resubscribe | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| GET | /api/instagram/debug-dump | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| GET | /api/instagram/media | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| GET | /api/instagram/media/diagnostics | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| GET | /api/instagram/identity-matrix | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| POST | /api/instagram/disconnect | yes | n/a | current user scoped | yes/bounded | yes | yes, sanitized | smoke/security tests |
| GET | /api/instagram/webhook | no/public constrained | n/a | n/a or token/signature/short-code scoped | yes/bounded | yes/basic | yes, sanitized | webhook tests |
| POST | /api/instagram/webhook | no/public constrained | n/a | n/a or token/signature/short-code scoped | yes/bounded | yes | yes, sanitized | webhook tests |
| GET | /api/instagram/webhook-log | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | webhook tests |
| GET | /api/instagram/webhook/diagnostics | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | webhook tests |
| POST | /api/instagram/webhook/resubscribe | yes | n/a | current user scoped | yes/bounded | yes | yes, sanitized | webhook tests |
| GET | /api/instagram/automation-health | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| POST | /api/instagram/process-unreplied-comments | yes | n/a | current user scoped | yes/bounded | yes | yes, sanitized | smoke/security tests |
| GET | /api/instagram/poll-now | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| POST | /api/instagram/comments/poll-now | yes | n/a | current user scoped | yes/bounded | yes | yes, sanitized | comment/retry tests |
| GET | /api/instagram/comments/processed | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | comment/retry tests |
| GET | /api/instagram/diagnostics/full | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| GET | /api/instagram/dm/rules | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| POST | /api/instagram/dm/rules | yes | n/a | current user scoped | yes/bounded | yes | yes, sanitized | smoke/security tests |
| PATCH | /api/instagram/dm/rules/{rid} | yes | n/a | owner/account scoped | yes/bounded | yes | yes, sanitized | smoke/security tests |
| DELETE | /api/instagram/dm/rules/{rid} | yes | n/a | owner/account scoped | yes/bounded | yes | yes, sanitized | smoke/security tests |
| POST | /api/instagram/dm/test-rule | yes | n/a | current user scoped | yes/bounded | yes | yes, sanitized | smoke/security tests |
| GET | /api/instagram/dm/logs | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| GET | /api/instagram/dm/diagnostics | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| GET | /api/instagram/credentials/diagnostics | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| GET | /api/instagram/dm/debug-latest | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| POST | /api/instagram/dm/resubscribe | yes | n/a | current user scoped | yes/bounded | yes | yes, sanitized | smoke/security tests |
| GET | /api/instagram/accounts | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| POST | /api/instagram/accounts/{account_id}/activate | yes | n/a | owner/account scoped | yes/bounded | yes | yes, sanitized | smoke/security tests |
| POST | /api/cron/refresh-instagram-tokens | yes | n/a | current user scoped | bounded | yes | low | smoke/security tests |
| GET | /api/instagram/token-refresh/status | yes | n/a | current user scoped | yes/bounded | yes/basic | yes, sanitized | smoke/security tests |
| GET | /api/ | no/public constrained | n/a | n/a or token/signature/short-code scoped | bounded | yes/basic | low | smoke/security tests |
| GET | /r/{short_code} | no/public constrained | n/a | n/a or token/signature/short-code scoped | bounded | yes | low | redirect tests |

## API9 Inventory Management Delta

- Public, auth, admin, diagnostics, webhook, billing-placeholder, data-deletion, redirect/link, Instagram OAuth, and repair/debug routes are included in the table above.
- Each route row records auth requirement, admin permission, ownership model, rate-limit/boundedness, input validation, sensitive-output risk, and test coverage.
- Production exposure is represented by auth requirement plus public-route constraints; public routes are constrained by generic responses, signed/HMAC/token checks, or short-code lookup.

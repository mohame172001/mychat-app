# Account recovery notes — Phase 2.14

Status: implemented in commit after `a238d6a`. Public UI and backend endpoints are live, but production email-delivery E2E is still blocked before billing can start.

## Endpoints

```
POST /api/auth/forgot-password   { email }            → { ok:true, status:"sent_if_account_exists" }
POST /api/auth/reset-password    { token, new_password } → { ok:true, status:"password_reset" }
```

Both endpoints live on the `/api` router prefix (`app.include_router(api)`).
The frontend axios client uses `baseURL = ${REACT_APP_BACKEND_URL}/api`,
so the public production URLs are:

```
https://backend-production-a1a3.up.railway.app/api/auth/forgot-password
https://backend-production-a1a3.up.railway.app/api/auth/reset-password
```

## Token model

| Field on `users` | Purpose | Lifetime |
|---|---|---|
| `password_reset_token_hash` | HMAC-SHA256(JWT_SECRET, raw_token) | one-shot — cleared on use |
| `password_reset_sent_at` | issuance timestamp | until next issuance |
| `password_reset_expires_at` | absolute expiry | 1h (env: `PASSWORD_RESET_TOKEN_TTL_HOURS`) |
| `password_reset_used_at` | mark-used timestamp | persisted post-use |

Raw token = `secrets.token_urlsafe(32)`. Only the hash is stored — same primitive that Phase 2.12 already uses for email verification.

## Privacy contract

- `forgot-password` always returns the same generic response shape for unknown email, Google-only account, password user, and rate-limited caller (the rate-limited case raises 429 with a generic detail — no email-existence leak).
- `reset-password` returns 400 with a stable detail string for invalid / used / expired / too-short cases — never mentions whether the user exists.
- Raw token is **never** logged. The log line at issuance records only `user_id` and a boolean `sent=true|false`.
- Raw token is **never** stored on the row — only the HMAC hash.
- Raw token is **never** echoed in any response body — the only legitimate consumer is the email-delivery webhook.
- Frontend: `ResetPassword.jsx` reads `?token=` once at mount and scrubs the query string via `setSearchParams(..., { replace: true })` so a Back navigation doesn't expose it.
- Frontend never writes the token to `localStorage` / `sessionStorage`.

## Rate limits

| Bucket | Limit | Window |
|---|---|---|
| `password_reset_request_ip` | 5 | 1 hour |
| `password_reset_request_email` (hashed) | 3 | 1 hour |

Both buckets reuse the same sliding-window limiter that `/auth/resend-verification` already uses.

## Side effects on success

- `password_hash` rotated to the bcrypt of the new password.
- `password_reset_token_hash` `$unset`.
- `password_reset_used_at` set to `now`.
- `session_version` incremented by 1 → every JWT issued before the reset returns 401 `session_revoked` on its next request. `session_revocation_reason='password_reset'` is also recorded.
- `updated_at` refreshed.

## Email delivery

Reuses the existing email-verification webhook transport (`EMAIL_VERIFICATION_WEBHOOK_URL`, optional `EMAIL_VERIFICATION_WEBHOOK_TOKEN`). The payload uses a distinct template name:

```
{ to, template: "mychat_password_reset", reset_url: "<FRONTEND_URL>/reset-password?token=..." }
```

If the webhook isn't configured, the issuance branch logs `sent=false` and the user gets the same generic success response — there is no `503` leak.

## Google-only accounts

Users created via Google Sign-In have `password_hash = None`. The forgot-password path short-circuits with the generic response and never issues a token. If those users want a password later, the intended UX is: log in via Google → Settings → "Set a password" (out of scope for Phase 2.14).

## E2E checks that still require operator action

Latest Phase 2.14E production observation:

- Forgot-password UI rendered and showed the generic success message.
- The reset email did not arrive for the temporary production test account.
- Settings password-change E2E passed independently: authenticated Settings Security rendered, toggles and autocomplete worked, the password change succeeded, the current session was revoked on the next protected navigation, the previous password failed, and the new password succeeded.

Remaining reset-specific checks:

1. Confirm a real reset email is delivered when `EMAIL_VERIFICATION_WEBHOOK_URL` is set.
2. Click the link, set a new password, confirm login with new password works.
3. Confirm a second login with the old password is rejected.
4. Confirm any other JWT for that user returns 401 `session_revoked`.
5. Manually verify the rate limits in production via 6 rapid requests from the same IP.
6. Confirm the link in the email never contains the user's email/id, only the token + reset path.

Until all 6 are verified end-to-end on the live host with the production
email webhook, Phase 2.14 stays **OPEN** and billing remains **BLOCKED**.

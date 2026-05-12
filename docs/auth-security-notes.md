# Auth Security Notes

These notes document MyChat's current authentication and session safety posture. Do not add secrets, passwords, password hashes, Google credentials, JWTs, access tokens, cookies, or raw Meta data to this file.

## Auth Methods

- Email/password login and signup issue the standard MyChat JWT.
- Google Sign-In verifies a Google ID token server-side and then issues the same MyChat JWT shape.
- Instagram/Meta OAuth is not an app login method. It only connects an Instagram professional account after the MyChat user is authenticated.

## JWT Freshness

- Normal app APIs resolve the user from the database on each request through the active-user dependency.
- If a user is suspended after a JWT is issued, normal APIs return `403 account_suspended`.
- If a user is soft-deleted after a JWT is issued, normal APIs return `403 account_deleted`.
- Admin endpoints still allow an active admin to view suspended/deleted target users. The target user's status does not block admin visibility.
- JWT invalidation is currently coarse-grained. Rotating `JWT_SECRET` invalidates all sessions. Per-user session revocation remains a future hardening item.

## Suspended and Deleted Login Behavior

- Invalid email/username or invalid password returns a generic invalid-credentials response.
- A suspended/deleted status is only revealed after the password or Google identity has been verified.
- Email/password login returns `account_suspended` or `account_deleted` for valid credentials on blocked accounts.
- Google login returns the same safe codes for linked suspended/deleted users.

## Email Identity Policy

- Email lookup is case-insensitive.
- New users store `normalized_email = lowercase(trim(email))`.
- Legacy mixed-case email rows are still found using a bounded case-insensitive lookup.
- Plus-addressing is intentionally allowed as a distinct email identity for now. For example, `user+team@example.com` and `user@example.com` are not silently merged.
- The current database index on `normalized_email` is non-unique so operators can inspect legacy duplicates before any future strict unique index.
- Accounts are never merged automatically.

## Google Identity Rules

- Google login requires `email_verified=true`.
- `google_sub` is the stable Google identity and has a sparse unique index.
- Existing email/password users can be linked to Google when the verified Google email matches the existing account and the account is not already linked to another Google subject.
- Same email with a different `google_sub`, or `google_sub` owned by one user while the email belongs to another user, returns a conflict.
- Google credential, ID token, decoded payload, and `google_sub` are not returned to the frontend.

## Admin Bootstrap Behavior

- `ADMIN_EMAILS` is an owner escape hatch.
- A user whose normalized email is in `ADMIN_EMAILS` is treated as owner and may be lazily inserted into `admin_members`.
- A disabled admin member loses admin access unless their email remains in `ADMIN_EMAILS`, in which case the environment value is intentionally treated as recovery access.
- Fully removing a bootstrap owner requires removing the email from `ADMIN_EMAILS` and updating/removing the `admin_members` row.
- `/api/admin/me` may return the current caller's email, role, permissions, and `bootstrap_owner` boolean, but it must never return the full `ADMIN_EMAILS` list.
- The last-owner invariant remains enforced for member updates/removals.

## Login and Signup Abuse Controls

- Login is rate-limited by IP and normalized identifier hash.
- Signup is rate-limited by IP and normalized email hash.
- Rate-limit logs use hashes or IPs only; passwords and submitted credentials are not logged.
- Email/password signup currently does not require email verification. Add verification before large-scale public acquisition or higher-risk billing flows.

## Password Reset Status

- Password reset is not currently implemented.
- Future password reset must use random tokens, hash tokens at rest, keep a short TTL, be single-use, return generic responses for existing/non-existing emails, rate-limit by IP and email hash, avoid logging raw tokens, and invalidate active sessions where the session model supports it.

## Logout and Browser Session Storage

- The frontend stores `mychat_token` and `mychat_user` in browser localStorage.
- Logout removes both keys and clears the API cache.
- Login, signup, and Google login clear stale user-specific API cache after storing the new session.
- A `401` response removes the stored token/user and clears the API cache.
- Route guards should prevent private app pages from rendering after logout or browser-back navigation with no valid session.

## Deferred Risks

- P2: exact atomic plan-limit reservation under very high concurrency remains deferred from Phase 2.12D.
- P2: per-user session revocation/session versioning is not implemented; `JWT_SECRET` rotation is the current global invalidation method.
- P2: email verification for password signup is not implemented.
- P3: strict unique `normalized_email` index should wait until legacy duplicate diagnostics are reviewed.

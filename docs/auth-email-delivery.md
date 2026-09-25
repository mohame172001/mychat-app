# Auth Email Delivery

Password resets and email verification use Resend when both `RESEND_API_KEY`
and `AUTH_EMAIL_FROM` are set in the backend environment. Store them in Replit
Secrets and include them in the production deployment. Never use frontend
environment variables or commit a real key.

- Verify `mail.mychaat.net` in Resend using the DNS records it provides.
- Create a sending-only API key restricted to that domain.
- Set `AUTH_EMAIL_FROM` to `MyChaat <no-reply@mail.mychaat.net>`.
- Redeploy. No database migration or Resend SDK is required.
- Request a reset on `https://mychaat.net/forgot-password` for an existing
  password-based account and check Resend's delivery status, then the inbox.

`FRONTEND_URL` supplies both the reset and verification link origin. Verification
opens `/verify-email`; the user explicitly confirms before the POST consumes the
token. Old `/api/auth/verify-email` GET links redirect to that page without
consuming the token, so ordinary email scanner GET requests cannot verify users.
Production Replit startup resolves the public origin from `backend/public_site.json`.
Auth emails require HTTPS links.

Optional `AUTH_EMAIL_REPLY_TO` sets Resend's `reply_to` header. Set it only to a
working receiving address; it does not create a mailbox or configure DNS.

The optional legacy `EMAIL_VERIFICATION_WEBHOOK_URL` transport remains supported
when Resend is not configured. A failed Resend request does not fall through to
another provider or retry blindly, preventing duplicate delivery.

The forgot-password endpoint intentionally gives a generic response for unknown
emails, Google-only accounts, and delivery failures. This prevents account
enumeration; it does not prove an email was sent. Backend logs record acceptance
or sanitized failure only, never addresses, keys, or reset links. Provider
acceptance is distinct from delivery; Resend's email events show the latter.

Existing expiring, single-use hashed reset tokens, rate limits, and session
revocation remain unchanged. The account owner should enter the new password.
New password signups use the existing verification policy when delivery is
configured. No existing user roles or passwords are changed by enabling email.

## Deployment validation (2026-09-25)

- Sending domain verified; TLS set to enforced; tracking not enabled.
- Domain-scoped sending key and sender added to both project and production
  secrets. No secret values are committed here.
- 50 backend tests passed locally and on Replit; 14 frontend recovery tests
  passed locally. Replit production build and deployment succeeded.
- Production health returned `ok: true` after startup completed.
- A real delivery-check email from Replit was marked `Delivered` by Resend.
- The owner's actual forgot-password request reached production but logged
  `password_reset_request_unknown`. No reset email was issued for that request.
  The existing account must be located or migrated into this production database
  before its reset can be tested end to end. Do not bypass this by silently
  creating an admin, changing a password, or copying over the whole database.

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

`FRONTEND_URL` supplies the reset link origin and `BACKEND_PUBLIC_URL` supplies
the verification link origin. Production Replit startup resolves both from
`backend/public_site.json`. Auth emails require HTTPS links.

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

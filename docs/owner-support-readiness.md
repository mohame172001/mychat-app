# Owner, support and webhook activation

## September 26, 2026

- Replit `ADMIN_EMAILS` is the server-side owner allowlist; no client role grant.
- Admin UI cache is session-scoped, expires after one minute and does not cache
  network failures. Backend authorization remains authoritative.
- `/support` submits category, reply email and message to `POST /api/support`.
  The server validates input, limits IP/email requests, rejects honeypots,
  and sends plain text to the fixed `support@mychaat.net` recipient.
  Client-supplied addresses are Reply-To only, never sending identities/recipients.
- Requires existing `RESEND_API_KEY` and `AUTH_EMAIL_FROM`. Set
  `AUTH_EMAIL_REPLY_TO=support@mychaat.net` for replies to account emails.
- Root domain DKIM, sending CNAMEs and receiving MX were added in Replit without
  replacing website or mail.mychaat.net records. Resend reports Verified.
  A real test message was delivered and appeared in Resend Receiving.
- Support inbox: https://resend.com/emails/receiving . This is not a Gmail mailbox.
- Meta Instagram webhook was still pointed at the old Railway deployment.
  Verification against https://mychaat.net/api/instagram/webhook returned the
  exact challenge with HTTP 200; Meta then saved this URL successfully.
  A real comment/action test is still required; callback verification alone does
  not prove signature validation, account subscription or action execution.
- Replit reports domain registration email verification is required within
  15 days. Owner must complete the registrar email if not already completed.

No capacity claim for 1,000 users follows from these changes. Approval/access,
API quotas, workload, worker configuration and load testing remain separate.

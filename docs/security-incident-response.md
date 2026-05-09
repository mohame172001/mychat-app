# Security Incident Response

This checklist is for MyChat operators responding to security incidents. Do not paste secrets, access tokens, raw webhook bodies, raw comments, raw replies, or raw DMs into tickets, chat, logs, screenshots, or third-party tools.

## Token Leak

1. Identify the exposed credential type without copying the value into the incident record.
2. Rotate the affected Railway environment variable or Meta/Google credential.
3. Revoke affected Meta access tokens when applicable and force Instagram reconnect for impacted accounts.
4. Invalidate affected JWT sessions by rotating `JWT_SECRET` if application tokens may be exposed.
5. Review logs for access from unfamiliar IPs and record only sanitized indicators.
6. Notify impacted users if account data may have been accessed.

## Compromised Admin

1. Suspend or remove the compromised admin role from the Admin Console.
2. Rotate that user's password or revoke Google access at the identity provider.
3. Review admin audit logs for plan, allowance, suspend/delete, repair-tool, and metrics actions.
4. Revoke any suspicious changes and document sanitized action IDs.
5. Ensure at least one trusted owner remains before removing owner privileges.

## Meta Webhook Abuse

1. Confirm webhook verify token and HMAC enforcement are enabled in production.
2. Rotate `WEBHOOK_VERIFY_TOKEN` and Meta app secret if either may be exposed.
3. Check duplicate processing, queue, and provider-proof logs for unusual spikes.
4. Pause affected automations if abuse could trigger unwanted replies or DMs.
5. Report only event IDs, hashed account IDs, and sanitized reasons.

## Raw Log Leak

1. Stop the source of unsafe logging and deploy the scrubber fix.
2. Identify log windows and systems that received the raw payload.
3. Rotate any credential or token visible in the leaked logs.
4. Request deletion/retention reduction from logging providers when supported.
5. Add a regression test covering the leaked field names.

## User Data Deletion Escalation

1. Verify requester identity through the supported data deletion process.
2. Do not send private data in email replies.
3. Pause automations and disconnect Instagram accounts before destructive deletion.
4. Preserve audit logs required for security/legal purposes where policy allows.
5. Confirm deletion status through the public data-deletion instructions page.

## Rollback Procedure

1. Identify the last known-good commit and Railway deployment.
2. Roll back the affected service only: backend for API/security defects, frontend for UI-only defects.
3. Confirm `/api/` health and the frontend login page after rollback.
4. Monitor error rates, queue status, webhook acknowledgements, and duplicate-send logs.
5. Open a follow-up fix branch from the latest mainline rather than editing production manually.

## Railway Environment Rotation

1. Rotate one secret at a time where possible.
2. Redeploy the service that consumes the rotated variable.
3. Confirm health checks and one low-impact authenticated request.
4. Never copy real values into commits, screenshots, tests, or docs.
5. Update `.env.example` only with placeholder names, never values.

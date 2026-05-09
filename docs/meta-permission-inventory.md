# Meta Permission Inventory

Last updated: May 9, 2026

This document is the operator-facing inventory for Meta App Review. MyChat uses Instagram API with Business Login. Do not request permissions that are not listed here unless the product has a reviewed feature that needs them.

## Production URLs

- Frontend: `https://frontend-production-6eb2.up.railway.app`
- Backend API: `https://backend-production-a1a3.up.railway.app/api`
- Privacy Policy: `https://frontend-production-6eb2.up.railway.app/privacy`
- Terms of Service: `https://frontend-production-6eb2.up.railway.app/terms`
- Data Deletion: `https://frontend-production-6eb2.up.railway.app/data-deletion`
- Webhook callback: `https://backend-production-a1a3.up.railway.app/api/instagram/webhook`
- Data deletion callback: `https://backend-production-a1a3.up.railway.app/api/meta/data-deletion`

## Permissions

| Permission | Why MyChat needs it | Reviewer UI path | How to test | Stored data | Not stored / safeguards |
| --- | --- | --- | --- | --- | --- |
| `instagram_business_basic` | Connect the user's Instagram Business or Creator account, identify the account, display account status, and fetch basic media metadata needed for selected-post automations. | Settings -> Instagram connection, Automations -> selected post rule. | Log in, connect Instagram, confirm account appears in Settings, create an automation scoped to one selected post. | Instagram account ID, username, profile picture URL, account type, media IDs/basic metadata. | Access tokens are backend-only and never returned to frontend/admin UI. Raw Graph responses are not exposed. |
| `instagram_business_manage_comments` | Read and reply to comments on connected Instagram media according to rules configured by the account owner. | Automations -> comment rule, Comments page, Dashboard status. | Create a selected-post automation with public reply enabled, comment from a test account, verify public reply and comment status. | Comment IDs, media IDs, commenter username/scoped ID, status fields, provider-confirmed reply metadata. | Public reply success is recorded only after provider confirmation. Tokens and raw Graph error bodies are redacted from logs. |
| `instagram_business_manage_messages` | Send configured Instagram DMs and process messaging webhooks for DM automations and comment-to-DM flows. | Automations -> DM message/follow gate, DM Automation page, Comments status. | Enable DM in an automation, comment from a test account that can receive messages, verify the DM and status. | Instagram-scoped sender ID, session/status metadata, configured automation messages, provider-confirmed DM metadata. | DM failures do not block public replies. Full private text is not returned in diagnostics/admin views. |

## Webhooks

The Instagram webhook endpoint receives comment and message events. It ACKs quickly and processes work through tracked background tasks/queue. HMAC validation is supported through `META_WEBHOOK_APP_SECRET` / `META_APP_SECRET`; production should run with `META_WEBHOOK_HMAC_ENFORCE=1` after the correct app secret is configured.

Webhook logs are sanitized. Do not enable or ship raw webhook payload logging in production.

## Data deletion

Users can request deletion at `/data-deletion` or by emailing support. Meta can call `/api/meta/data-deletion`; the endpoint returns a confirmation code and stores only hashed request metadata.

## App Review notes

- MyChat automations only run for accounts the user connects and rules the user configures.
- Broad historical comment replies remain disabled by default; selected-post historical catch-up is constrained to a single selected media ID.
- Admin repair tools must remain disabled in production unless an operator deliberately opens a short repair window.

# Meta App Review Checklist

Last updated: May 9, 2026

Use this checklist to prepare the Meta reviewer account and submission notes. Do not commit real reviewer passwords, access tokens, app secrets, or Instagram credentials.

## Public URLs

- Frontend: `https://frontend-production-6eb2.up.railway.app`
- Backend health: `https://backend-production-a1a3.up.railway.app/api/`
- Privacy Policy: `https://frontend-production-6eb2.up.railway.app/privacy`
- Terms of Service: `https://frontend-production-6eb2.up.railway.app/terms`
- Data Deletion: `https://frontend-production-6eb2.up.railway.app/data-deletion`
- OAuth redirect URL: `https://backend-production-a1a3.up.railway.app/api/instagram/callback`
- Webhook callback URL: `https://backend-production-a1a3.up.railway.app/api/instagram/webhook`
- Data deletion callback URL: `https://backend-production-a1a3.up.railway.app/api/meta/data-deletion`

## Reviewer access placeholders

- MyChat reviewer login: `<provide in Meta dashboard only>`
- MyChat reviewer password: `<provide in Meta dashboard only>`
- Test Instagram professional account: `<provide in Meta dashboard only>`
- Test Instagram password: `<provide in Meta dashboard only>`
- Selected test post/reel URL: `<provide in Meta dashboard only>`

## Pre-submission environment checks

- `ENABLE_ADMIN_REPAIR_TOOLS` is false or unset.
- `ADMIN_EMAILS` contains only operator/admin email addresses.
- `META_WEBHOOK_VERIFY_TOKEN` is set in production.
- `META_WEBHOOK_APP_SECRET` or `META_APP_SECRET` is set.
- `META_WEBHOOK_HMAC_ENFORCE=1` once the production app secret is verified.
- `CORS_ALLOWED_ORIGINS` includes only production frontend origins.
- `ENABLE_DOCS_IN_PRODUCTION` is unset/false.
- Google Sign-In env vars may be enabled, but Google credentials are not relevant to Meta review.
- No raw access tokens, raw comment text, raw DM text, raw reply text, or raw Graph error bodies appear in logs.

## Reviewer test script

1. Open the frontend URL.
2. Open Privacy, Terms, and Data Deletion from the public links.
3. Log in with the reviewer MyChat account.
4. Open Settings -> Instagram.
5. Connect or confirm the connected Instagram professional account.
6. Open Automations.
7. Create a new automation scoped to one selected post/media.
8. Configure:
   - public comment reply enabled,
   - DM enabled,
   - optional follow gate disabled for the basic review flow.
9. Save and activate the automation.
10. From the test Instagram account, add a new comment on the selected post.
11. Confirm MyChat logs/status show the specific-post automation matched.
12. Confirm the public reply appears under the Instagram comment.
13. Confirm the DM is received by the commenter.
14. Open Comments in MyChat.
15. Confirm the comment status shows success or partial status if the test account cannot receive DMs.
16. If a retryable public reply failure fixture exists, click Retry Reply and confirm it does not duplicate successful steps.
17. Optional follow verification path:
    - enable follow gate and actual follow verification,
    - comment from an account that does not follow,
    - tap the follow confirmation,
    - confirm the final link is not sent until the account actually follows,
    - follow the business account and tap again,
    - confirm the final link is sent once.
18. Open Dashboard and verify current-month activity appears.
19. Open Settings and disconnect Instagram if the reviewer needs to validate disconnect behavior.
20. Open Data Deletion and submit a deletion request or use the email process.

## Submission notes

- MyChat does not use Instagram data for ads, resale, or model training.
- MyChat only sends messages/replies configured by the connected account owner.
- Comment and DM delivery statuses are tracked independently to avoid duplicate replies.
- Historical broad-account comment replies are not enabled by default.

# Meta App Review Screencast Script

Last updated: May 9, 2026

Record a short, unedited screencast that shows the reviewer exactly why each requested Instagram permission is needed. Do not show production secrets, Railway environment variables, access tokens, OAuth client secrets, raw webhook payloads, or private user data unrelated to the test account.

## Recording setup

- Browser profile: clean/incognito if possible.
- MyChat test user: reviewer-safe account only.
- Instagram test account: professional account owned by the operator.
- Commenter account: separate Instagram test account.
- Keep the browser zoom at 100%.
- Keep DevTools closed unless showing a safe status page.

## Flow

1. Show the public MyChat URL.
2. Open Privacy Policy, Terms, and Data Deletion from the public links.
3. Return to Login and sign in with the reviewer MyChat account.
4. Open Settings -> Instagram.
5. Click Connect Instagram or show the connected account status.
6. Complete the Instagram/Meta OAuth consent flow if reconnecting.
7. Return to MyChat and show the connected Instagram account in Settings.
8. Open Automations.
9. Create an automation for one selected post/media.
10. Show the selected post scope clearly.
11. Add a public comment reply message.
12. Add a DM message.
13. Save and activate the automation.
14. Switch to Instagram with the commenter account.
15. Add a new comment on the selected post.
16. Return to Instagram post and show the public reply appears.
17. Open Instagram DMs and show the configured DM arrives.
18. Return to MyChat Comments.
19. Show the comment status, provider-confirmed reply status, and DM status.
20. Optional follow gate:
    - enable follow gate,
    - attempt confirmation without following,
    - show no final link is sent,
    - follow the account,
    - confirm again,
    - show final link sent once.
21. Open Dashboard and show usage/activity updated.
22. Open System Health and show safe operational diagnostics with no tokens.
23. Open Data Deletion and show the deletion request process.

## Reviewer narration

- `instagram_business_basic`: "MyChat uses this to connect the professional account and let the user select a post for automation."
- `instagram_business_manage_comments`: "MyChat uses this to detect comments and publish the exact public reply configured by the account owner."
- `instagram_business_manage_messages`: "MyChat uses this to send the configured DM response and track delivery state."

## Safety callouts

- Public reply and DM statuses are independent.
- The app does not send duplicate replies when webhook/polling events repeat.
- Broad historical comments are not processed by default.
- Tokens and raw Graph payloads are not shown in the UI or logs.

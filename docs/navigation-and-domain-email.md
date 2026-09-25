# Navigation and Domain Email

## Navigation changes

- Protected routes preserve a validated, same-site `next` destination through
  email/password or Google login, and across the login/signup links.
- Connect Instagram opens `/app/settings?tab=instagram`. Settings tabs update
  the URL and respond to back/forward navigation.
- New Automation opens `/app/automations?create=1`, which opens the existing
  builder. Creation/edit links preserve the selected-account query parameters.
- Help and Contact open `/support`, with recovery, verification, status, and
  policy links. `/contact` redirects there; unknown routes show the 404 page.
- `/verify-email` handles confirmation and resending with readable results.
  Tokens are removed from the address bar and are consumed only on explicit
  confirmation. Page-view analytics no longer include query strings.

## Mail cutover status

The currently verified sending domain is `mail.mychaat.net`. Keep the working
`AUTH_EMAIL_FROM=MyChaat <no-reply@mail.mychaat.net>` and its existing Resend key
until a replacement is verified. Both account verification and password reset
use this backend mail transport; no browser secret is needed.

Target addresses, not yet activated:

- Sender: `MyChaat <no-reply@mychaat.net>`
- Support and reply-to: `support@mychaat.net`

Root-domain receiving requires owner approval for Resend and DNS changes. The
site temporarily uses the owner's existing address, centralized in
`frontend/src/lib/contactSupport.js`, rather than advertise an inactive mailbox.
No sending or receiving DNS records were changed in the navigation release.

After approval, add `mychaat.net` in Resend without deleting the working
subdomain. Add its provided DNS records in Replit, enable receiving, and verify
inbound mail before changing the site's support constant. Preserve website
records and any existing mail routing. Create a root-domain sending-only key,
store it in Replit project and production secrets, update `AUTH_EMAIL_FROM` and
`AUTH_EMAIL_REPLY_TO`, and redeploy. Check a real delivery and an inbound message.

Resend receiving is an inbox in its Emails > Receiving dashboard, not a Gmail
mailbox. Observe provider retention limits; forwarding or a full mailbox would
be a separate, explicitly configured workflow. Never claim that a `mailto:`
link alone activates receiving.

## Validation

- Frontend: 34 suites / 277 tests passed; production build passed locally.
- Backend: 88 auth, identity, Google, password-reset, and mail tests passed.
- Tests use isolated fixtures; they do not prove live delivery or migrate the
  owner's legacy account. Existing roles, passwords and Instagram data are unchanged.

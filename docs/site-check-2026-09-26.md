# Site check - 2026-09-26

Target: https://mychaat.net (Replit), not the legacy Railway site.

## Verified

- Public home, login, signup, privacy, terms, data-deletion, support, status,
  and status API returned HTTP 200, with CSP and nosniff headers.
- Account, automation-list, and admin-user APIs rejected unauthenticated
  access with HTTP 403. Plain webhook GET was also rejected as expected.
- Home Features and Get Started links navigated to the expected locations.
- Fresh mobile signup at 390px fit; resizing a previously loaded desktop
  signup exposed a real overflow caused by Google's fixed-width iframe.
- Full local backend suite: 1012 passed, 55 skipped, 12 subtests passed.
  Skipped integration tests require an explicitly isolated Replit test DB;
  they were not run against production or an unverified development DB.
- Frontend unit suite and optimized production build passed. New regression
  coverage checks responsive Google sizing, listener cleanup, and preserving
  an in-progress login while resizing.

## Fixes

- Four PostgreSQL test modules now use the same sibling-import convention
  as the rest of the pytest suite. Previously their relative imports broke
  collection before the full suite could run.
- The public status API uses the actual configured database backend in its
  message instead of always claiming MongoDB. Four tests cover both database
  types and connectivity failures, including sanitized errors.
- Login/signup grid columns can shrink. Google sign-in rerenders at the
  available container width, deferring resize changes during login.

## Not claimed

No new account, real payment, password reset, Instagram comment, or outbound
DM was created by this check. The available live-site browser session was
signed out, so authenticated creation/activation needs the owner's session.
The status endpoint reported API/database operational and webhook activity
unknown at the time of inspection; this does not prove webhook delivery.

The separate laptop cache cleanup was blocked by the command execution policy
even after approval. No files were removed; C had about 37.18 GiB free.

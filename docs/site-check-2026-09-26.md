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
- Full local backend suite after startup/upsert regression coverage: 1030 passed,
  55 skipped, 12 subtests passed.
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

## Deployment startup regression

After publishing the audit fixes, the deployment build succeeded but live
requests returned 500. Production logs identified a statement timeout in
`PostgresDocumentDatabase.initialize()` at `CREATE INDEX IF NOT EXISTS ...
USING gin`. A read-only production catalog query confirmed all 32 document
GIN indexes already existed and were valid.

Startup now checks PostgreSQL's index catalog before issuing index DDL.
Valid existing indexes are reused without acquiring a DDL table lock;
missing indexes are still created. Invalid, unready, wrong-table/schema, or
wrong-uniqueness indexes raise instead of silently weakening constraints.
TTL registry updates, schema serialization, and statement timeouts remain.

Thirteen regression cases cover reuse, creation, constraint validation,
startup serialization, TTL metadata, and visible index failures. Replit's
selected startup/observability suite passed 36 tests; its full frontend
suite passed 284 tests. No production records or indexes were deleted and
development-to-production database copying remained disabled.

After the startup fix, the public routes and status API returned 200, the
database reported `PostgreSQL reachable`, protected APIs returned 403, empty
login input returned 422, and an unsigned webhook POST returned 403. The
Google config endpoint reported enabled. A short 500 window still occurred
while Replit switched containers and awaited the index bootstrap; this check
does not claim zero-downtime publishing.

Live logs then exposed `_id is immutable` while recording usage. The adapter
incorrectly validated `$setOnInsert._id` on existing documents even though
the entire operator must be ignored on updates. This also affects repeated
usage-reservation bucket upserts. The adapter now skips insert-only fields
for existing rows; actual identity changes remain forbidden. Five unit
regressions cover initial insertion, repeated increments, bucket defaults,
and rejection of identity changes. No quota limits or old outcomes were reset.

## Not claimed

The existing Google account successfully signed in to the live dashboard.
Dashboard, Automations, Settings, and Billing navigation reached their
expected routes. Instagram showed the connected username and the builder
loaded real posts. Empty-keyword validation disabled Go Live; entering a
test keyword enabled it, and DM preview displayed the input correctly.
The builder has no separate save-draft action, so the test returned without
clicking Go Live or changing the owner's existing active rule.

No new account, real payment, password reset, Instagram comment, or outbound
DM was created by this check. Rule persistence/activation and actual Meta
delivery were not tested end-to-end. Billing explicitly reports that paid
upgrades are not enabled; no payment readiness is claimed.
The status endpoint reported API/database operational and webhook activity
unknown at the time of inspection; this does not prove webhook delivery.

The separate laptop cache cleanup was blocked by the command execution policy
even after approval. No files were removed; C had about 37.18 GiB free.

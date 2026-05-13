# Cleanup — deferred items

Items identified by the Phase 2.15 audit that were deliberately NOT removed because removing them safely requires either a backfill migration, a product decision, or a separately-scoped refactor.

## Frontend

### `frontend/src/pages/SystemHealth.jsx`
- **Status:** kept on disk; not route-loaded since Phase 2.13B.
- **Why kept:** `frontend/src/lib/googleUiVisibility.test.js` asserts the file contains specific strings (`Google Sign-In configured`, `google-signin-configured`, etc.) to lock the Google badge contract.
- **Action to fully remove:** move the Google badge into another page (e.g. AdminConsole or Settings) and rewrite the test to assert against the new home, then delete `SystemHealth.jsx`. Out of scope for Phase 2.15.

### `frontend/src/pages/Contacts.jsx`
- **Status:** kept on disk; not route-loaded.
- **Why kept:** Landing marketing label references "Contacts"; Dashboard reads `totalContacts` from `/api/dashboard/summary`; backend has `/api/contacts/*` endpoints. The page may be re-linked once the contacts feature is reintroduced.
- **Action:** product decision required before delete. If contacts are permanently retired, delete the page, the backend endpoints, the Dashboard stat, and the Landing copy together.

## Backend

### `/api/broadcasts/*` (4 endpoints)
- **Status:** kept.
- **Why:** the cleanup rule forbids removing backend endpoints just because the frontend stopped calling them. Tests + admin tooling may still rely on the data shape.
- **Action to remove:** confirm no admin / cron / test path uses them; delete endpoints + `broadcasts` collection indexes + related tests in a dedicated phase.

### `/api/conversations/*` (3 endpoints)
- **Status:** kept.
- **Why:** same rule as broadcasts. The DM flow may still write `conversations` documents even though the LiveChat UI is gone.
- **Action to remove:** trace whether `conversations` is written by the DM/webhook path; if it's still relied on by usage reporting or admin, retain.

### `/api/contacts/*` (4 endpoints)
- **Status:** kept.
- **Why:** Dashboard `totalContacts` aggregation reads this collection.
- **Action to remove:** can't, until the Contacts page returns or the stat is removed from Dashboard.

### IG identifier triplication
- **Status:** kept.
- **Where:** `instagramAccountId` (137 hits in `backend/server.py`), `igUserId` (74 hits), `ig_user_id` (188 hits). `_account_scoped_query` accepts all three.
- **Why:** legacy rows in production carry mixed casing. A backfill migration is needed before any column is canonicalised.
- **Action to remove:** Phase 3-ish data migration: write a one-shot Mongo script that copies `instagramAccountId → ig_user_id`, drop the helper alias, and update tests. Out of scope for Phase 2.15.

### `except Exception:` blocks (71 occurrences)
- **Status:** kept.
- **Why:** most are intentional best-effort logging guards (analytics writes, observability scrubbers, audit log writers). Narrowing the catch class without per-call review risks turning a current "log warning + continue" into an unhandled 500.
- **Action to remove:** per-call audit in a dedicated reliability phase.

### Email lookup mixed pattern
- **Status:** intentional fallback.
- **Why:** `_find_user_by_email` cascades `normalized_email → email → case-insensitive regex` so legacy rows that pre-date the `normalized_email` field still resolve. Removing any branch breaks recovery for those rows.
- **Action:** backfill `normalized_email` for all legacy users, then collapse to a single branch.

## Tests

### `googleUiVisibility.test.js` couples to `SystemHealth.jsx`
- Move the Google badge to another page first, then update this test to point at the new location. Tracked as part of the SystemHealth deferred item above.

## Documentation

- The 19 untouched docs in `docs/` describe historical / policy state. They reference removed UI surfaces only in the context of "what existed before"; no doc currently claims billing is enabled. No staleness rewrites required this phase.

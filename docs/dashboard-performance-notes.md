# Dashboard Performance Notes

Generated: 2026-05-14

## Request Path

Authenticated app bootstrap:

1. `GET /api/auth/me`
2. `GET /api/instagram/accounts`
3. `GET /api/dashboard/summary`

The dashboard frontend uses stale-while-revalidate caching with a user/account-scoped key. Manual Refresh bypasses cache. Failed, HTML, 401, 403, 404, and 500 responses are not cached.

## Backend Sections

`GET /api/dashboard/summary` currently measures:

- active Instagram account lookup
- account metadata
- usage limits / plan
- usage events
- automations
- contacts
- link clicks
- comments

The endpoint returns safe timing headers:

- `X-Dashboard-Summary-Time`
- `X-Dashboard-Summary-Slowest`
- `X-Dashboard-Summary-Source`

## Current Data Source

Source is now a read-through dashboard snapshot:

- `rebuilt`: no usable snapshot existed, so the backend computed the live summary once and stored it.
- `read_model`: a fresh `dashboard_summaries` snapshot was returned.
- `stale_read_model`: an acceptable stale snapshot was returned immediately and a background refresh was scheduled.
- `stale_fallback` / `live_fallback`: reserved fallback states if rebuild fails and a safe snapshot/live path remains available.

The snapshot collection is `dashboard_summaries`. Fresh TTL is 60 seconds and max stale is 5 minutes. The stored `summary` preserves the existing dashboard response shape so the frontend does not need a separate contract.

The live calculation remains as the rebuild/fallback source. It preserves legacy account fallback behavior and bounded collection reads.

## Frontend UX Contract

The frontend shows cached/snapshot dashboard data immediately when it is within max stale and refreshes in the background. Phase 2.18 extends this from memory-only cache to opt-in persistent safe browser snapshots for:

- dashboard summary
- automations summary/list
- comments first page/filter
- sanitized Instagram accounts list

Persistent snapshots are user/account scoped and are cleared on logout, account switch, or matching cache invalidation. The cache layer does not persist auth tokens, passwords, reset tokens, provider payloads, HTML responses, 401/403 responses, or 5xx responses.

After auth bootstrap restores `mychat_user`, the app warms core route chunks and data for Dashboard, Automations, Comments, and Instagram accounts using low-priority idle prefetch. This is what makes refresh/new-tab UX materially different from memory-only SWR.

Product decision: do not show visible freshness labels such as "Updated X ago", "Last updated", or Arabic equivalents. If background refresh fails while cached data exists, show only:

`Couldn't refresh. Showing the latest available data.`

Manual Refresh still bypasses cache.

## Infrastructure Trigger

If `GET /api/` or the first authenticated API after idle is slow while warm dashboard summary is within budget, the primary fix is Railway always-on backend service configuration. Code changes cannot fully remove a sleeping container cold start.

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

Source is `live`: the response is computed from bounded raw collections. This preserves correctness and legacy account fallback behavior, but repeated warm requests may still be expensive for large tenants.

## Read Model Trigger

Implement a persisted `dashboard_summaries` read model if production warm measurements show:

- p50 above 500 ms, or
- p95 above 1200 ms, or
- slowest section repeatedly dominated by bounded scans of comments, contacts, link clicks, or usage events.

The read model must preserve exact visible numbers and keep live calculation as a fallback until equivalence tests pass.

## Infrastructure Trigger

If `GET /api/` or the first authenticated API after idle is slow while warm dashboard summary is within budget, the primary fix is Railway always-on backend service configuration. Code changes cannot fully remove a sleeping container cold start.

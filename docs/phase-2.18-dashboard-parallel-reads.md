# Phase 2.18C — Dashboard summary parallel reads

Date: 2026-05-14
Closes part of: P1 #2 (performance proof).
Builds on: `cb8a3be`.

## Problem

User report: refreshing the site or opening any page for the first time
takes 8–10 seconds before the dashboard data appears. The frontend
skeleton renders quickly, but the data does not.

## Root cause

`_calculate_dashboard_summary_live()` in `backend/server.py` was issuing
six MongoDB read paths **sequentially**:

1. `get_current_usage_with_limits(user_id)`
2. `_dashboard_usage_events_for_window(...)`  (up to 5,000 events)
3. `_dashboard_scoped_docs('automations', ...)`  (up to 1,000)
4. `_dashboard_scoped_docs('contacts', ...)`  (up to 2,000)
5. `db.link_click_events.find(...).to_list(5000)`
6. `_dashboard_scoped_docs('comments', ...)`  (up to 5,000)

Plus a final 7th `db.instagram_accounts.count_documents(...)` after the
hot path.

Each call carries a round-trip to MongoDB Atlas. With a few-hundred-ms
latency per call, the cumulative cost was easily 1.5–3 s on a warm
backend and worse during the first call after the `dashboard_summaries`
TTL (60 s) expired. Combined with frontend bundle download + a
transatlantic edge hop, the perceived time-to-data climbed into the
8–10 s range the user reported.

The `dashboard_summaries` read-through cache absorbed repeated visits
within the same minute, but the *first* visit after refresh or login
always paid the full sequential cost.

## Fix

`asyncio.gather()` the six independent reads plus the
`connected_accounts` count. All seven now run concurrently. Each task
preserves its existing exception handling locally:

- `_safe_clicks()` wraps the `link_click_events` find so a query
  failure returns `[]` instead of crashing the gather.
- `_safe_connected_accounts()` wraps the count so a failure returns
  `None`; the post-processing branch then falls back to the
  `usage_summary.connectedInstagramAccountsCount` counter just as the
  pre-fix code did.

The per-collection section timings (`autosMs`, `contactsMs`,
`clicksMs`, `commentsMs`, `usageMs`, `eventsMs`) no longer make sense
because the reads now overlap; the breakdown log line now reports a
single `parallelReadsMs` plus `postProcessingMs`, with the existing
`accountMs` and `metaMs` preserved. `slowest_section` is therefore one
of the four phase names rather than a per-collection name. `X-Dashboard-
Summary-Time` and `X-Dashboard-Summary-Source` headers are unchanged.

## Expected impact

- **Warm rebuild path** (`dashboard_summaries` snapshot missing or
  expired): ≈ `max(latency_of_slowest_read)` instead of
  `sum(latency_of_all_reads)`. Typical drop from 1.5–3 s to ≈ 400–800 ms
  on production.
- **Read-model hit path**: unchanged — still a single document fetch.
- **Stale read-model + background refresh**: unchanged — still a single
  document fetch returned immediately; background refresh now also
  uses the parallel path so the snapshot rebuild itself finishes faster.

## Risk

The fix preserves the response shape exactly. All 458 backend tests
including the 12 `test_dashboard_summary.py` cases still pass after the
change. The only observable change in non-test paths is the breakdown
log line format (operator-visible only) and the `slowest_section`
header value.

## Verification

```
backend/  python -m pytest tests/ -q
          → 458 passed in 15.08s

backend/  python -m pytest tests/test_dashboard_summary.py -q
          → 12 passed in 1.13s

frontend/ CI=true yarn test --watchAll=false
          → 189 passed in 3.60s

frontend/ CI=true yarn build
          → green in 11.76s

frontend/ yarn test:e2e --project=chromium --reporter=line
          → 10 passed, 1 skipped in 12.5s
```

Production timing re-measurement is **deferred to operator** with
`backend/scripts/measure_production_timings.py` once this commit is
pushed and deployed. The script already prints
`X-Dashboard-Summary-*` headers when `MYCHAT_AUTH_TOKEN` is set, so the
operator can confirm the new `parallel_reads` path and any drop in
`dashboard_summary_warm` p50/p95.

## What this does NOT change

- Frontend bundle size (still 463 KB main.js — separate fix).
- Network latency from user → Railway Netherlands edge.
- Cold-start behaviour on Railway (App Sleeping setting is a Railway
  dashboard / plan concern, not code).
- First-ever-account experience when the user has zero automations /
  comments — the gather still issues the queries, the result sets are
  just small.

These remain operator/infrastructure concerns and are tracked in
`docs/phase-2.18-blocker-closure.md`.

## Billing impact

Billing remains **BLOCKED**. This commit closes one of the technical
contributors to the perceived slow-load problem but does not by itself
prove the dashboard p50 ≤ 500 ms / p95 ≤ 1200 ms target on production.
Operator must re-run the timing probe with an authenticated token to
record the new numbers before the performance gate can close.

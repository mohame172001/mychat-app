# Performance Budget

These are operating targets for MyChat before Billing. They are not hard CI gates yet unless a focused test explicitly covers the behavior.

## Frontend Targets

- Public page shell visible: under 1.5s on a normal connection.
- Authenticated route shell visible: under 1.5s after auth bootstrap.
- Cached route data visible: immediate or under 300ms.
- No major route should show a blank white page while API data loads.
- No infinite spinner without an error or retry path.
- Manual Refresh always bypasses cache.
- Mutations invalidate only affected cache prefixes.
- Navigation must not use `window.location.assign` for normal in-app route changes.
- Failed, HTML, 401, 403, 404, or 500 responses must not be cached as data.

## API Targets

- Dashboard summary: under 2s target.
- Automations summary: under 2s target.
- Comments list: under 2s target for a bounded page.
- Admin users list: under 3s target for bounded search/page.
- Admin user detail: shell under 1.5s; full sections may load progressively.
- System Health: live status response should remain lightweight.

## API Count Targets

- Dashboard: 1 primary summary call on mount; cached navigation should render immediately and refresh in background.
- Automations: 1 primary summary call; profile/media/detail calls only when needed.
- Comments: 1 bounded list call per filter/page plus websocket connection.
- Billing: current plan and plan catalog may load in parallel.
- Admin users: 1 paginated users call per search/page key.
- Admin detail: 1 detail call today; split into sections if payload/time grows.

## Production Verification

After frontend deploys:

1. Fetch `/asset-manifest.json` and inspect all JS chunks.
2. Confirm changed performance strings or chunk hash are present.
3. Verify public pages return 200.
4. Verify protected API routes return 403 unauthenticated.
5. For authenticated smoke, compare first load vs second navigation for dashboard, automations, comments, admin users, and admin detail.


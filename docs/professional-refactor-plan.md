# MyChat Professional Refactor Plan

This plan records the safe cleanup boundary for the production codebase. It is intentionally phased so Instagram automation, HMAC validation, dedupe, and rate limits are not disturbed by broad file moves.

## Current Entry Points

- Backend production entry point remains `backend/server.py`.
- Frontend production routes remain centralized in `frontend/src/App.js`.
- Admin/backend diagnostic APIs remain protected server-side for internal support.
- The frontend Instagram diagnostics UI is removed from the production route surface.

## Stable Frontend Routes

- `/`
- `/login`
- `/signup`
- `/forgot-password`
- `/reset-password`
- `/privacy`
- `/terms`
- `/data-deletion`
- `/status`
- `/app`
- `/app/automations`
- `/app/automations/:id`
- `/app/dm-automation`
- `/app/settings`
- `/app/billing`
- `/app/admin`
- `/app/admin/specific-reply-debug`

`/app/admin/instagram-diagnostics` is intentionally no longer registered. Direct visits fall through the protected app catch-all and redirect to `/app`.

## Backend Refactor Phases

1. Create `backend/app/` package scaffolding behind `backend/server.py`.
2. Move pure utilities first:
   - id formatting and redaction helpers
   - time parsing helpers
   - pagination helpers
3. Move Instagram pure services only after import compatibility tests exist:
   - rule normalizer
   - rule classification
   - dedupe key helpers
4. Move stateful services after the pure helpers are stable:
   - account resolver
   - comment-to-DM session service
   - webhook processor
   - polling service
5. Keep compatibility wrappers in `backend/server.py` until all tests and Railway startup confirm the new modules are stable.

## Frontend Refactor Phases

1. Keep current routes stable while introducing route constants.
2. Extract API clients by domain:
   - auth
   - dashboard
   - Instagram accounts/media
   - automations
   - admin
3. Split large pages after route/API extraction:
   - `Automations.jsx`
   - `AdminConsole.jsx`
   - `Settings.jsx`
4. Keep app shell, auth redirect, and account switching behavior unchanged.

## Deferred Items

- Full `backend/server.py` decomposition is deferred because the file contains intertwined production-critical Instagram automation, webhook, token refresh, and billing-adjacent logic.
- `AdminConsole.jsx` and `Automations.jsx` component splits are deferred until route/API constants are introduced.
- Backend admin diagnostic APIs are retained because they are useful for support and remain admin-auth protected.

## Safety Gates

Any future structural move must pass:

- `python -m py_compile backend/server.py`
- `python -m pytest backend/tests -x --tb=short -q`
- `cd frontend && CI=true yarn test --watchAll=false`
- `cd frontend && yarn build`
- route grep confirming no production frontend route exposes removed diagnostics UI

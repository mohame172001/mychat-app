# Codebase Health Audit — Phase 2.15

Generated: 2026-05-13
Repo: `master` HEAD pre-cleanup `73447a2`, post-cleanup `<commit-this-phase>`
Branch: `master`
Working tree at audit time: clean except expected `__pycache__` movements.

## Part A — Baseline

| Check | Result |
|---|---|
| Backend tests | **445 / 445 passed** (`pytest backend/tests -q`) |
| Frontend tests | **174 / 174 passed** in 25 suites (`yarn test --watchAll=false`) |
| Frontend build | green |
| main.js (build artifact) | 463 KB — `main.a5f87983.js` |
| Backend chunk routes | 115 `@api.*` endpoints |

## Part B — Inventory

### Frontend routes (App.js)

Public routes:
- `/` → Landing
- `/login` → Login
- `/signup` → Signup
- `/forgot-password` → ForgotPassword *(Phase 2.14B)*
- `/reset-password` → ResetPassword *(Phase 2.14B)*
- `/privacy` → PrivacyPolicy
- `/terms` → Terms
- `/data-deletion` → DataDeletion
- `*` → NotFound

Authenticated `/app` routes (under `ProtectedRoute`):
- `/app` index → Dashboard
- `/app/automations` → Automations
- `/app/automations/:id` → FlowBuilder
- `/app/comments` → Comments
- `/app/dm-automation` → DmAutomation
- `/app/settings` → Settings
- `/app/billing` → Billing
- `/app/admin` → AdminConsole *(role-gated server-side)*
- `/app/admin/specific-reply-debug` → SpecificReplyDebug *(role-gated server-side)*
- `*` → `<Navigate to="/app" replace />`

Sidebar items: Dashboard, Automations, Comments, DM Automation, Billing, Settings *(plus an Admin link rendered conditionally for admin roles)*.

Removed routes — verified `/app/system-health` and `/app/broadcast` are not defined; both fall through to the inner `<Navigate to="/app">` for authenticated users and to the top-level `<NotFound>` for unauthenticated visits.

### Pages on disk vs. routes

| Page file | Route loaded? | Status |
|---|---|---|
| Landing, Login, Signup, ForgotPassword, ResetPassword, PrivacyPolicy, Terms, DataDeletion, NotFound | yes | active |
| Dashboard, Automations, FlowBuilder, Comments, DmAutomation, Settings, Billing | yes | active |
| AdminConsole, admin/SpecificReplyDebug | yes | active (server-side role gate) |
| `SystemHealth.jsx` | **no** | kept on disk — exercised by `frontend/src/lib/googleUiVisibility.test.js` to lock the "Google Sign-In configured" badge contract. **Do not delete.** |
| `Contacts.jsx` | **no** | kept on disk — Landing marketing label references "Contacts" feature; Dashboard reads `totalContacts` stat; backend has `/api/contacts/*` endpoints. May be reintroduced. **Deferred — see deferred items.** |
| `Broadcasting.jsx` | **no** | **DELETED in this phase** — no imports, no tests, removed from sidebar in Phase 2.13B. Backend `/api/broadcasts` endpoints retained per cleanup rules. |
| `LiveChat.jsx` | **no** | **DELETED in this phase** — no imports, no tests, no sidebar entry. Backend `/api/conversations` endpoints retained. |

### Backend endpoint distribution (`@api.<method>` count = 115)

| Prefix | Count |
|---|---|
| `/instagram/*` | 37 |
| `/admin/*` | 32 |
| `/auth/*` | 11 |
| `/automations/*` | 8 |
| `/comments/*` | 6 |
| `/contacts/*` | 4 |
| `/broadcasts/*` | 4 |
| `/dashboard/*` | 3 |
| `/conversations/*` | 3 |
| `/plans`, `/plan/*`, `/usage/*`, `/observability/*`, `/meta`, `/cron/*` | 1 each |

Auth endpoints (full list):
- `POST /auth/signup`, `POST /auth/login`, `POST /auth/password`, `GET /auth/me`
- `POST /auth/resend-verification`, `POST /auth/verify-email`, `GET /auth/verify-email`
- `POST /auth/forgot-password` *(Phase 2.14B)*, `POST /auth/reset-password` *(Phase 2.14B)*
- `GET /auth/google/config`, `POST /auth/google`

Backend endpoints flagged for further review (not deleted, deferred):
- `/api/broadcasts/*` (4 endpoints): no current frontend caller after `Broadcasting.jsx` removal. Retained per "do not remove backend just because frontend doesn't call it" rule.
- `/api/conversations/*` (3 endpoints): no current frontend caller after `LiveChat.jsx` removal. Same retention rule.
- `/api/contacts/*` (4 endpoints): partial frontend use (Dashboard reads count via `dashboard/summary`). Retained.

### Database collections in use

Per `_create_indexes`, `db.<name>` references, and test fixtures:

`users`, `instagram_accounts`, `automations`, `automation_queue`, `comments`, `comment_dm_sessions`, `contacts`, `conversations`, `messages`, `broadcasts`, `webhook_log`, `usage_events`, `monthly_usage`, `usage_reservations`, `usage_reservation_buckets`, `usage_links`, `link_click_events`, `admin_audit_logs`, `admin_members`, `user_plans`, `user_limit_overrides`, `billing_subscriptions`, `instagram_oauth_audit`, `instagram_oauth_state`, `webhook_events`, `data_deletion_requests`, `email_verification_events`, `dashboard_metrics_cache`.

Schema hot-spots:
- `users` carries auth-recovery fields (password_reset_token_hash, password_reset_expires_at, password_reset_used_at, password_reset_sent_at) added in Phase 2.14B alongside the email-verification token primitives.
- `instagram_accounts` retains three identifier flavours (`instagramAccountId`, `igUserId`, `ig_user_id`) for backward-compatibility scope helpers. Documented in deferred items.
- `users.normalized_email` is the canonical lookup field; `_find_user_by_email` falls back through `normalized_email → email → case-insensitive regex` for legacy rows.

### Docs

23 markdown files in `docs/`. Updated in this phase:
- `docs/codebase-health-audit.md` *(this file, NEW)*
- `docs/functional-verification-matrix.md` *(Phase 2.14 + 2.15 reconciliation)*
- `docs/billing-readiness-gate.md` *(Phase 2.14 product gate restated)*
- `docs/auth-recovery-notes.md` *(NEW in Phase 2.14B)*

The other 19 docs are policy / inventory documents (privacy, security, deployment, Meta review, reliability, etc.). They reference "System Health" / "Broadcast" only in `meta-screencast-script.md` and matrix files in the **context of features that USED TO exist** — kept for historical accuracy, not as current-product claims.

No doc claims billing is enabled. `billing-readiness-gate.md` carries an explicit Phase 2.14 product-functional gate that blocks billing until password change + reset E2E are verified end-to-end.

### Tests

Backend: 445 tests across ~50 test files. Skipped count: 0 in the latest run. Coverage areas: auth (login / signup / password change / forgot+reset / Google / verify-email), IDOR + RBAC, webhook HMAC, dedupe, provider-proof, plan limits + reservations, admin roles + members, usage tracking, observability, reconciliation, Phase 2.14B password reset (17 tests).

Frontend: 25 suites / 176 tests. All source-grep style (no React Testing Library). Suites cover: route wiring, autocomplete attrs, auth error mapping, plan limits UI helpers, observability scrubbers, password-reset wiring (new), Settings no-fake-toast contract (new).

## Part C — Dead-code findings

### P0 — fake-success bug (FIXED in this phase)

`frontend/src/pages/Settings.jsx:87` — Profile tab "Save changes" button called `toast.success('Profile updated')` with **no API call and no controlled form state** (fields used `defaultValue`, not `value`). Same bug class as the pre-2.14 password change. **Fix shipped in this phase**: removed the button entirely; rewrote the fields as read-only with a copy note pointing users to the Security tab for password changes. Locked by a new contract test `Settings.noFakeToast.test.js`.

### P1 — orphan frontend pages

| File | Status before | Action |
|---|---|---|
| `frontend/src/pages/Broadcasting.jsx` | not routed, not linked, no imports, no tests | **deleted** |
| `frontend/src/pages/LiveChat.jsx` | not routed, not linked, no imports, no tests | **deleted** |
| `frontend/src/pages/SystemHealth.jsx` | not routed, not linked | **kept** — referenced by `googleUiVisibility.test.js` for the Google badge contract |
| `frontend/src/pages/Contacts.jsx` | not routed | **kept** — backend endpoints + Dashboard `totalContacts` stat + Landing copy reference it |

### P2 — backend endpoints with no current frontend caller

Per the strict cleanup rule "do not remove backend endpoints just because the frontend doesn't call them," all are **kept**:
- `/api/broadcasts/*` (4)
- `/api/conversations/*` (3)
- `/api/contacts/*` (4) — partially used through dashboard aggregation

### P3 — naming inconsistencies (deferred)

- Three Instagram identifier flavours in production code: `instagramAccountId` (137 hits), `igUserId` (74), `ig_user_id` (188). The `_account_scoped_query` helper accepts all three. Removing any would require a backfill migration. Deferred.
- Email lookup: `_find_user_by_email` is the canonical helper. 25 raw `'email':` filters and 22 `normalized_email` filters across server.py. The mixed usage is the canonical fallback pattern (normalized → exact → regex). No action needed.
- 71 `except Exception:` blocks. Most are best-effort logging guards. Audit needed before any narrowing; deferred.

## Part D — Consistency findings

| Dimension | State | Action |
|---|---|---|
| API client | Single `frontend/src/lib/api.js` axios instance, `baseURL = ${BACKEND_URL}/api`. No direct `fetch` to backend found. | OK |
| Auth token storage | `localStorage.getItem('mychat_token')` consistently. | OK |
| Direct navigation | Only two `window.location.href` calls: `lib/api.js` (401 redirect) and `lib/instagramConnect.js` (OAuth start). Both intentional. | OK |
| Hardcoded production URLs in JS | none in `frontend/src` outside of test/doc strings. | OK |
| `toast.success` after backend response | enforced in Settings via new contract test. Other pages spot-checked (Comments, FlowBuilder, AdminConsole) — all toasts follow an `await api.*`. | OK |
| Old Meta permissions | absent from code; only mentioned in `functional-verification-matrix.md` as a verified `verified` row asserting their absence. | OK |
| Plaintext password / token logging | none. Only `logger.error('IG token exchange failed: %s', _redact_secrets(data))` (redacted) and `identity_matrix_token_present user=… len=…` (length only). | OK |

## Part E — Functional verification matrix updates

Reconciled in `docs/functional-verification-matrix.md`:
- Auth rows A8..A13 + B3..B7 marked `implemented` *(Phase 2.14 fix)*; pending real E2E.
- New rows A18..A24 cover forgot/reset password *(Phase 2.14B)*; pending real-email E2E.
- B-tab "Save changes" row not previously in the matrix; added implicitly via this audit + the new test.

## Part F — Cleanup actions taken in this phase

1. **Fixed** `frontend/src/pages/Settings.jsx` Profile tab fake-success — removed the lying Save button, switched fields to read-only, added explanatory copy.
2. **Deleted** `frontend/src/pages/Broadcasting.jsx` (orphan, no callers, no tests).
3. **Deleted** `frontend/src/pages/LiveChat.jsx` (orphan, no callers, no tests).
4. **Added** `frontend/src/pages/Settings.noFakeToast.test.js` — new contract test that fails if any `toast.success` in Settings.jsx is not preceded (in the surrounding 30 lines or same statement) by an `await api.*` or a `refreshUser(` call.
5. **Updated** `docs/functional-verification-matrix.md`, `docs/billing-readiness-gate.md`.
6. **Added** `docs/codebase-health-audit.md` *(this file)* and `docs/cleanup-deferred-items.md`.

Files NOT changed despite being identified for potential change:
- `frontend/src/pages/SystemHealth.jsx` — kept for test contract.
- `frontend/src/pages/Contacts.jsx` — kept; backend stats still reference contacts count.
- All `/api/broadcasts`, `/api/conversations`, `/api/contacts` backend routes — kept per cleanup rule.
- 71 `except Exception:` blocks — deferred (audit-only).
- IG identifier triplication — deferred (migration required).

## Part G — Recommended next phases

**Do not start billing.** Phase 2.14B email-delivery E2E and Phase 2.14 password-change E2E both remain pending on the live host. The Phase 2.15 audit found one previously-undetected fake-success regression (Profile tab); the fix is shipped but production verification is still pending.

In recommended order:
1. **Phase 2.14B production E2E** — once Railway redeploys `73447a2` (and the Phase 2.15 commit), exercise forgot/reset flow with a real test account against the email-delivery webhook. Update the matrix.
2. **Phase 2.14 password-change E2E** — exercise change-password from Settings with a real authenticated session; confirm session_version invalidation across two browsers.
3. **Phase 2.16 (suggested)** — operator profile-edit backend (`PATCH /auth/me`) + connect it to Settings Profile tab (the fields are now read-only as a stopgap).
4. **Phase 3.0** — billing abstraction. Only after #1 + #2 close.

See also `docs/cleanup-deferred-items.md` for non-urgent technical debt.

## Phase 2.16 Performance Addendum

Generated: 2026-05-14

The dashboard remains the primary performance-sensitive route. Phase 2.16 added safe timing instrumentation instead of guessing at perceived slowness:

- Backend `GET /api/dashboard/summary` now emits `X-Dashboard-Summary-Time`, `X-Dashboard-Summary-Slowest`, and `X-Dashboard-Summary-Source`.
- Backend `dashboard_summary_breakdown` logs section timings and bounded document counts without tokens, raw comments, raw DMs, raw Graph bodies, emails, or passwords.
- Frontend `api.js` logs a safe dashboard-summary timing line with client/backend/dashboard milliseconds and section/source metadata only.
- A regression test verifies the dashboard timing headers are present and do not expose token fields.

Current dashboard implementation still computes from bounded live collections (`usage_events`, `automations`, `contacts`, `link_click_events`, and `comments`) rather than a persisted dashboard read model. If production warm dashboard p95 is over 1200 ms, the next required change is a dashboard read model keyed by user, active Instagram account, and UTC month. If warm dashboard is within budget but first request after idle is slow, the required change is Railway always-on infrastructure.

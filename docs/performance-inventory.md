# Performance Inventory

Phase 2.13 documents route data loading, cache policy, and known bottlenecks. Cache keys must remain user/account/filter scoped and must never cache 401/403/404/500/HTML responses.

## Freshness Policy

- Dashboard summary: render cached user/account data immediately when available, TTL 45s, allow stale display up to 5m with background refresh and a small refresh message.
- Automations summary: render cached user/account summary immediately, TTL 45s, allow stale display up to 5m with background refresh. Mutations invalidate `automations:` keys.
- Comments: render cached user/account/filter/page data immediately, TTL 15s, allow stale display up to 2m with background refresh. Manual Refresh always bypasses cache.
- Billing/plan: render cached plan data briefly, TTL 60s for current plan and 5m for plan catalog, refresh in background. Billing remains provider-free.
- Admin overview/users/detail/members/metrics: short cache with stale-while-revalidate up to 2m. Mutations invalidate affected admin detail/list sections.
- System Health: no long-lived cache; it should reflect current backend configuration and safe status booleans.
- Public/legal routes: static/lazy-loaded pages, no app API dependency.

## Route Inventory

| Route | Component | Mount API calls | Refresh/tab behavior | Cache key / TTL | Loading behavior | Backend boundedness | Suspected bottleneck / fix |
|---|---|---|---|---|---|---|---|
| `/` | `frontend/src/pages/Landing.jsx` | None | None | Browser asset cache | Lazy route shell | N/A | Public bundle cost only |
| `/login` | `frontend/src/pages/Login.jsx` | Auth submit only | N/A | No data cache | Lazy route shell | Auth rate-limited | Keep Google script isolated to auth page |
| `/signup` | `frontend/src/pages/Signup.jsx` | Signup/resend only | N/A | No data cache | Lazy route shell | Auth rate-limited | Keep verification flow explicit |
| `/privacy` | `PrivacyPolicy.jsx` | None | None | Static | Lazy route shell | N/A | None |
| `/terms` | `Terms.jsx` | None | None | Static | Lazy route shell | N/A | None |
| `/data-deletion` | `DataDeletion.jsx` | Optional form submit only | N/A | No data cache | Lazy route shell | Callback non-destructive unless verified | None |
| `/app` dashboard | `Dashboard.jsx` | `GET /dashboard/summary` | Refresh forces refetch | `dashboard-summary:user:account`, 45s | Shows cached stats immediately; background refresh | Summary endpoint bounded and account-scoped | Fixed blocking first load by SWR cache |
| `/app/automations` | `Automations.jsx` | `GET /automations/summary`, profile/media only when needed | Summary cached; modal detail fetched on demand | `automations:summary:user:account`, 45s | List shell and cached summary first | Summary endpoint avoids full automation details | Fixed full-list refetch on navigation |
| `/app/comments` | `Comments.jsx` | `GET /comments?page&limit=30&unreplied` plus websocket | Refresh invalidates list and force refetches | `comments:list:user:account:filter:status:page`, 15s | Current page cache first; load-more incremental | Page size capped at 30 | Fixed repeated blocking page reloads |
| `/app/dm-automation` | `DmAutomation.jsx` | rules, logs limit 50, diagnostics in parallel | Manual refresh through page controls | Not changed in this phase | Page shell plus section loading | Logs bounded by limit | Candidate for future section cache if usage grows |
| `/app/billing` | `Billing.jsx` | `GET /plan/current`, `GET /plans` | Refresh via page reload/navigation uses SWR | `billing:current:user`, `billing:plans`, 60s/5m | Cached plan first, background refresh | Small bounded plan endpoints | Fixed route navigation wait on plan calls |
| `/app/settings` | `Settings.jsx` | Auth/profile context plus settings actions | Save actions only | Auth context cache | Standard page shell | User-scoped | No major bottleneck observed |
| `/app/system-health` | `SystemHealth.jsx` | automation health + observability status | Manual refresh should call live endpoints | No long cache | Section status cards | Read-only safe summaries | Keep uncached by design |
| `/app/admin` overview | `AdminConsole.jsx` | `/admin/me`, `/admin/overview` | Tab change uses cached data when fresh | `admin:overview`, 60s | Admin shell first | Overview bounded summary | Fixed blocking admin overview reload |
| `/app/admin` users | `AdminConsole.jsx` | `GET /admin/users?page&page_size&search` | Search/page key scoped; short SWR | `admin:users:...`, 30s | Table shell, cached rows first | Page size bounded | Fixed repeated tab reloads |
| `/app/admin` user detail | `AdminConsole.jsx` | `GET /admin/users/{id}/detail` | Mutations invalidate detail/user list | `admin:user-detail:id`, 30s | Detail tab cache first; page does not blank on refresh | Admin detail excludes raw tokens/text | Still one backend payload; future split sections if detail grows |
| `/app/admin` members | `AdminConsole.jsx` | `GET /admin/members` | Mutations invalidate admin member cache | `admin:members`, 30s | Cached list first | Small bounded list | Fixed tab reload wait |
| `/app/admin` metrics | `AdminConsole.jsx` | `GET /admin/metrics/reconciliation` | Manual/admin-only refresh | `admin:metrics:reconciliation`, 30s | Cached diagnostic counts first | Read-only and bounded | Future pagination if metric scope grows |

## Backend Endpoint Notes

- `/dashboard/summary`, `/automations/summary`, `/comments`, `/admin/users`, admin diagnostics, and reconciliation were already bounded during security hardening.
- Comments remain paginated and account-scoped. Admin user detail remains sanitized and excludes raw tokens/text.
- No new request timing logs were added in this phase to avoid increasing log volume; if added later, log only route name, status, duration, and safe IDs/hashes.
- The next backend performance step, if needed, is section endpoints for Admin User Detail rather than larger payloads.


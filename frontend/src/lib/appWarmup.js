import api from './api';
import { cachedApiGetSWR } from './apiCache';
import { preloadRoutes } from './routePreloader';

// Phase 2.18F: TTL = "how long stale data is shown without ANY refresh"
// maxStale = "how long stale data is shown WHILE we refresh silently".
// Longer maxStale = the user almost always sees data instantly on
// return, even after hours away, with the latest numbers landing
// silently within the first second.
const DASHBOARD_TTL_MS = 60 * 1000;
const DASHBOARD_MAX_STALE_MS = 6 * 60 * 60 * 1000;       // 6h
const AUTOMATIONS_TTL_MS = 90 * 1000;
const AUTOMATIONS_MAX_STALE_MS = 6 * 60 * 60 * 1000;     // 6h
const ACCOUNTS_TTL_MS = 180 * 1000;
const ACCOUNTS_MAX_STALE_MS = 24 * 60 * 60 * 1000;       // 24h — account list rarely changes
const ADMIN_TTL_MS = 60 * 1000;
const ADMIN_MAX_STALE_MS = 30 * 60 * 1000;               // 30m

let scheduledScope = '';
let scheduledPromise = null;

function getActiveAccountKey(user) {
  return user?.activeInstagramAccountId || user?.activeInstagramIgUserId || 'active';
}

export function getCoreWarmupCacheKeys(user) {
  const userId = user?.id || 'anon';
  const activeAccountKey = getActiveAccountKey(user);
  return {
    dashboard: `dashboard-summary:${userId}:${activeAccountKey}`,
    automations: `automations-summary:${userId}:${activeAccountKey}`,
    accounts: `instagram-accounts:${userId}`,
  };
}

export function scheduleCoreAppWarmup(user, options = {}) {
  if (!user?.id) return null;

  const isAdmin = Boolean(options.isAdmin ?? user.isAdmin ?? (user.role === 'admin' || user.role === 'owner'));
  const scope = `${user.id}:${getActiveAccountKey(user)}:${isAdmin ? 'admin' : 'user'}`;
  if (scheduledScope === scope && scheduledPromise) return scheduledPromise;

  scheduledScope = scope;
  preloadRoutes(isAdmin ? ['Dashboard', 'Automations', 'Admin'] : ['Dashboard', 'Automations']);

  const keys = getCoreWarmupCacheKeys(user);
  // Phase 2.18F: prefetch fires IMMEDIATELY rather than at idle. The
  // previous idle delay meant the Dashboard component sometimes
  // mounted BEFORE the prefetch even kicked off. We now race the
  // network in parallel with the React route transition so by the
  // time the user sees /app, the /dashboard/summary request is
  // already in flight and cachedApiGetSWR de-dupes the second call
  // from the Dashboard mount.
  scheduledPromise = (async () => {
    const tasks = [
      cachedApiGetSWR(keys.dashboard, () => api.get('/dashboard/summary'), {
        ttlMs: DASHBOARD_TTL_MS,
        maxStaleMs: DASHBOARD_MAX_STALE_MS,
        persist: true,
      }),
      cachedApiGetSWR(keys.automations, () => api.get('/automations/summary', { timeout: 8000 }), {
        ttlMs: AUTOMATIONS_TTL_MS,
        maxStaleMs: AUTOMATIONS_MAX_STALE_MS,
        persist: true,
      }),
      cachedApiGetSWR(keys.accounts, () => api.get('/instagram/accounts'), {
        ttlMs: ACCOUNTS_TTL_MS,
        maxStaleMs: ACCOUNTS_MAX_STALE_MS,
        persist: true,
      }),
    ];
    // Admins also see Admin Overview by default; prefetch the
    // top-level admin sections so the console renders instantly
    // when they click into it. Per-section caches still revalidate
    // in the background when the admin opens each tab.
    if (isAdmin) {
      tasks.push(
        cachedApiGetSWR(
          `admin:overview:${user.id}`,
          () => api.get('/admin/overview'),
          { ttlMs: ADMIN_TTL_MS, maxStaleMs: ADMIN_MAX_STALE_MS, persist: true },
        ),
        cachedApiGetSWR(
          `admin:members:${user.id}`,
          () => api.get('/admin/members'),
          { ttlMs: ADMIN_TTL_MS, maxStaleMs: ADMIN_MAX_STALE_MS, persist: true },
        ),
      );
    }
    await Promise.allSettled(tasks);
  })();

  return scheduledPromise;
}

export function resetAppWarmupForTests() {
  scheduledScope = '';
  scheduledPromise = null;
}

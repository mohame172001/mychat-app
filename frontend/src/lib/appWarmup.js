import api from './api';
import { cachedApiGetSWR } from './apiCache';
import { preloadRoutes } from './routePreloader';

const DASHBOARD_TTL_MS = 60 * 1000;
const DASHBOARD_MAX_STALE_MS = 5 * 60 * 1000;
const AUTOMATIONS_TTL_MS = 90 * 1000;
const AUTOMATIONS_MAX_STALE_MS = 5 * 60 * 1000;
const COMMENTS_TTL_MS = 20 * 1000;
const COMMENTS_MAX_STALE_MS = 2 * 60 * 1000;
const ACCOUNTS_TTL_MS = 180 * 1000;
const ACCOUNTS_MAX_STALE_MS = 10 * 60 * 1000;

let scheduledScope = '';
let scheduledPromise = null;

function getActiveAccountKey(user) {
  return user?.activeInstagramAccountId || user?.activeInstagramIgUserId || 'active';
}

function runWhenIdle(callback) {
  if (typeof window !== 'undefined' && typeof window.requestIdleCallback === 'function') {
    return window.requestIdleCallback(callback, { timeout: 2000 });
  }
  return setTimeout(callback, 250);
}

export function getCoreWarmupCacheKeys(user) {
  const userId = user?.id || 'anon';
  const activeAccountKey = getActiveAccountKey(user);
  return {
    dashboard: `dashboard-summary:${userId}:${activeAccountKey}`,
    automations: `automations-summary:${userId}:${activeAccountKey}`,
    comments: `comments:list:${userId}:${activeAccountKey}:unreplied:1`,
    accounts: `instagram-accounts:${userId}`,
  };
}

export function scheduleCoreAppWarmup(user, options = {}) {
  if (!user?.id) return null;

  const isAdmin = Boolean(options.isAdmin ?? user.isAdmin ?? (user.role === 'admin' || user.role === 'owner'));
  const scope = `${user.id}:${getActiveAccountKey(user)}:${isAdmin ? 'admin' : 'user'}`;
  if (scheduledScope === scope && scheduledPromise) return scheduledPromise;

  scheduledScope = scope;
  preloadRoutes(isAdmin ? ['Dashboard', 'Automations', 'Comments', 'Admin'] : ['Dashboard', 'Automations', 'Comments']);

  const keys = getCoreWarmupCacheKeys(user);
  scheduledPromise = new Promise((resolve) => {
    runWhenIdle(async () => {
      await Promise.allSettled([
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
        cachedApiGetSWR(keys.comments, async () => {
          const response = await api.get('/comments', {
            params: { page: 1, limit: 30, unreplied: true },
          });
          return response.data;
        }, {
          ttlMs: COMMENTS_TTL_MS,
          maxStaleMs: COMMENTS_MAX_STALE_MS,
          persist: true,
        }),
        cachedApiGetSWR(keys.accounts, () => api.get('/instagram/accounts'), {
          ttlMs: ACCOUNTS_TTL_MS,
          maxStaleMs: ACCOUNTS_MAX_STALE_MS,
          persist: true,
        }),
      ]);
      resolve();
    });
  });

  return scheduledPromise;
}

export function resetAppWarmupForTests() {
  scheduledScope = '';
  scheduledPromise = null;
}

const cache = new Map();
const STORAGE_PREFIX = 'mychat_api_snapshot:';
const PERSISTABLE_PREFIXES = [
  'dashboard-summary:',
  'automations-summary:',
  'instagram-accounts:',
  // Admin console snapshots — same user as the cache key, sanitized of
  // tokens / passwords / credentials by FORBIDDEN_PERSIST_KEYS. Stored
  // per-admin so non-admin users never get an admin snapshot.
  'admin:overview:',
  'admin:users:',
  'admin:user-detail:',
  'admin:members:',
  'admin:metrics:',
];
const FORBIDDEN_PERSIST_KEYS = new Set([
  'access_token',
  'accessToken',
  'meta_access_token',
  'metaAccessToken',
  'authorization',
  'password',
  'password_hash',
  'passwordHash',
  'password_reset_token',
  'passwordResetToken',
  'password_reset_token_hash',
  'passwordResetTokenHash',
  'token',
  'credential',
  'raw',
  'payload',
  'provider_payload',
  'providerPayload',
  'graph_body',
  'graphBody',
]);

const now = () => Date.now();

function isJsonResponse(response) {
  if (!response || !response.headers) return true;
  const contentType = String(response.headers['content-type'] || response.headers.get?.('content-type') || '').toLowerCase();
  return contentType.includes('application/json') || contentType.includes('+json');
}

function isCacheableResponse(response) {
  if (!response || response.data === undefined) return true;
  const status = Number(response.status || 200);
  return status >= 200 && status < 300 && isJsonResponse(response);
}

function isAuthError(error) {
  const status = Number(error?.response?.status || error?.status || 0);
  return status === 401 || status === 403;
}

function canUseStorage() {
  try {
    return typeof window !== 'undefined' && Boolean(window.localStorage);
  } catch {
    return false;
  }
}

function isPersistableKey(key) {
  return typeof key === 'string' && PERSISTABLE_PREFIXES.some(prefix => key.startsWith(prefix));
}

function storageKey(key) {
  return `${STORAGE_PREFIX}${key}`;
}

function sanitizeForPersistence(value, depth = 0) {
  if (depth > 8) return undefined;
  if (value === null) return null;
  if (['string', 'number', 'boolean'].includes(typeof value)) return value;
  if (Array.isArray(value)) {
    return value.map(item => sanitizeForPersistence(item, depth + 1)).filter(item => item !== undefined);
  }
  if (typeof value === 'object') {
    const out = {};
    Object.entries(value).forEach(([key, item]) => {
      if (FORBIDDEN_PERSIST_KEYS.has(key)) return;
      if (key.startsWith('$') || key.includes('.')) return;
      const clean = sanitizeForPersistence(item, depth + 1);
      if (clean !== undefined) out[key] = clean;
    });
    return out;
  }
  return undefined;
}

function loadPersistentEntry(key) {
  if (!canUseStorage() || !isPersistableKey(key)) return null;
  try {
    const raw = window.localStorage.getItem(storageKey(key));
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || parsed.data === undefined || !Number(parsed.updatedAt)) return null;
    return { data: parsed.data, updatedAt: parsed.updatedAt, promise: null, persisted: true };
  } catch {
    try { window.localStorage.removeItem(storageKey(key)); } catch (_) {}
    return null;
  }
}

function persistEntry(key, data, updatedAt, persist) {
  if (!persist || !canUseStorage() || !isPersistableKey(key)) return;
  try {
    const clean = sanitizeForPersistence(data);
    window.localStorage.setItem(storageKey(key), JSON.stringify({ data: clean, updatedAt }));
  } catch (_) {}
}

function removePersistentEntry(key) {
  if (!canUseStorage() || !isPersistableKey(key)) return;
  try { window.localStorage.removeItem(storageKey(key)); } catch (_) {}
}

function getEntry(key) {
  const existing = cache.get(key);
  if (existing) return existing;
  const persisted = loadPersistentEntry(key);
  if (persisted) cache.set(key, persisted);
  return persisted;
}

function isEntryWithinMaxStale(entry, maxStaleMs) {
  if (!entry) return false;
  if (!Number.isFinite(Number(maxStaleMs)) || Number(maxStaleMs) <= 0) return true;
  return now() - Number(entry.updatedAt || 0) < Number(maxStaleMs);
}

export function getCachedApiData(key, options = {}) {
  const entry = getEntry(key);
  if (!isEntryWithinMaxStale(entry, options.maxStaleMs)) return undefined;
  return entry?.data;
}

export function getApiCacheEntry(key) {
  const entry = getEntry(key);
  if (!entry) return null;
  return {
    data: entry.data,
    updatedAt: entry.updatedAt || 0,
    hasInFlightRequest: Boolean(entry.promise),
    ageMs: entry.updatedAt ? now() - entry.updatedAt : null,
  };
}

export function invalidateApiCache(prefix = '') {
  for (const key of cache.keys()) {
    if (!prefix || key.startsWith(prefix)) {
      cache.delete(key);
    }
  }
  if (canUseStorage()) {
    try {
      for (let i = window.localStorage.length - 1; i >= 0; i -= 1) {
        const key = window.localStorage.key(i);
        if (!key?.startsWith(STORAGE_PREFIX)) continue;
        const apiKey = key.slice(STORAGE_PREFIX.length);
        if (!prefix || apiKey.startsWith(prefix)) window.localStorage.removeItem(key);
      }
    } catch (_) {}
  }
}

export function clearApiCache() {
  cache.clear();
  invalidateApiCache();
}

export function clearApiMemoryCacheForTests() {
  cache.clear();
}

export async function cachedApiGet(key, fetcher, options = {}) {
  const ttlMs = Number(options.ttlMs ?? 30000);
  const force = Boolean(options.force);
  const allowStaleOnError = options.allowStaleOnError !== false;
  const persist = Boolean(options.persist);
  const existing = getEntry(key);
  const fresh = existing?.data && now() - existing.updatedAt < ttlMs;

  if (!force && fresh) {
    return { data: existing.data, cached: true, stale: false };
  }
  if (!force && existing?.promise) {
    return existing.promise;
  }

  const promise = Promise.resolve()
    .then(fetcher)
    .then((response) => {
      if (!isCacheableResponse(response)) {
        const error = new Error('api_cache_uncacheable_response');
        error.code = 'api_cache_uncacheable_response';
        error.response = response;
        throw error;
      }
      const data = response?.data ?? response;
      const updatedAt = now();
      cache.set(key, { data, updatedAt, promise: null });
      persistEntry(key, data, updatedAt, persist);
      return { data, cached: false, stale: false };
    })
    .catch((error) => {
      if (isAuthError(error)) {
        cache.delete(key);
        removePersistentEntry(key);
        throw error;
      }
      if (allowStaleOnError && existing?.data) {
        cache.set(key, { data: existing.data, updatedAt: existing.updatedAt, promise: null });
        return { data: existing.data, cached: true, stale: true, error };
      }
      cache.delete(key);
      removePersistentEntry(key);
      throw error;
    });

  cache.set(key, {
    data: existing?.data,
    updatedAt: existing?.updatedAt || 0,
    promise,
  });
  return promise;
}

export async function cachedApiGetSWR(key, fetcher, options = {}) {
  const ttlMs = Number(options.ttlMs ?? 30000);
  const maxStaleMs = Number(options.maxStaleMs ?? 300000);
  const force = Boolean(options.force);
  const onUpdate = typeof options.onUpdate === 'function' ? options.onUpdate : null;
  const persist = Boolean(options.persist);
  const existing = getEntry(key);
  const hasData = existing?.data !== undefined;
  const ageMs = existing?.updatedAt ? now() - existing.updatedAt : Number.POSITIVE_INFINITY;
  const fresh = hasData && ageMs < ttlMs;
  const allowedStale = hasData && (maxStaleMs <= 0 || ageMs < maxStaleMs);

  if (force || !allowedStale || !hasData) {
    return cachedApiGet(key, fetcher, { ttlMs, force, allowStaleOnError: allowedStale, persist });
  }
  if (fresh) {
    return { data: existing.data, cached: true, stale: false, refreshing: false };
  }

  if (!existing.promise) {
    const promise = cachedApiGet(key, fetcher, { ttlMs, force: true, allowStaleOnError: true, persist })
      .then((result) => {
        if (onUpdate) onUpdate(result.data, result);
        return result;
      })
      .catch((error) => {
        if (isAuthError(error)) {
          cache.delete(key);
          removePersistentEntry(key);
          if (onUpdate) onUpdate(undefined, { data: undefined, cached: false, stale: false, error });
          return { data: undefined, cached: false, stale: false, error };
        }
        if (onUpdate) onUpdate(existing.data, { data: existing.data, cached: true, stale: true, error });
        return { data: existing.data, cached: true, stale: true, error };
      });
    cache.set(key, { ...existing, promise });
  }

  return {
    data: existing.data,
    cached: true,
    stale: true,
    refreshing: true,
    promise: cache.get(key)?.promise,
  };
}

export function resetApiCacheForTests() {
  clearApiCache();
}

const cache = new Map();

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

export function getCachedApiData(key) {
  const entry = cache.get(key);
  return entry?.data;
}

export function getApiCacheEntry(key) {
  const entry = cache.get(key);
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
}

export function clearApiCache() {
  cache.clear();
}

export async function cachedApiGet(key, fetcher, options = {}) {
  const ttlMs = Number(options.ttlMs ?? 30000);
  const force = Boolean(options.force);
  const allowStaleOnError = options.allowStaleOnError !== false;
  const existing = cache.get(key);
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
      cache.set(key, { data, updatedAt: now(), promise: null });
      return { data, cached: false, stale: false };
    })
    .catch((error) => {
      if (isAuthError(error)) {
        cache.delete(key);
        throw error;
      }
      if (allowStaleOnError && existing?.data) {
        cache.set(key, { data: existing.data, updatedAt: existing.updatedAt, promise: null });
        return { data: existing.data, cached: true, stale: true, error };
      }
      cache.delete(key);
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
  const existing = cache.get(key);
  const hasData = existing?.data !== undefined;
  const ageMs = existing?.updatedAt ? now() - existing.updatedAt : Number.POSITIVE_INFINITY;
  const fresh = hasData && ageMs < ttlMs;
  const allowedStale = hasData && (maxStaleMs <= 0 || ageMs < maxStaleMs);

  if (force || !allowedStale || !hasData) {
    return cachedApiGet(key, fetcher, { ttlMs, force, allowStaleOnError: allowedStale });
  }
  if (fresh) {
    return { data: existing.data, cached: true, stale: false, refreshing: false };
  }

  if (!existing.promise) {
    const promise = cachedApiGet(key, fetcher, { ttlMs, force: true, allowStaleOnError: true })
      .then((result) => {
        if (onUpdate) onUpdate(result.data, result);
        return result;
      })
      .catch((error) => {
        if (isAuthError(error)) {
          cache.delete(key);
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

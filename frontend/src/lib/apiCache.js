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

export function getCachedApiData(key) {
  const entry = cache.get(key);
  return entry?.data;
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
      if (existing?.data) {
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

export function resetApiCacheForTests() {
  clearApiCache();
}

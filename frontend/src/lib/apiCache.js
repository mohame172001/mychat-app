const cache = new Map();

const now = () => Date.now();

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
  cache.clear();
}

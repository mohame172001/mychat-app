import {
  cachedApiGet,
  cachedApiGetSWR,
  clearApiCache,
  clearApiMemoryCacheForTests,
  getCachedApiData,
  invalidateApiCache,
  resetApiCacheForTests,
} from './apiCache';

describe('cachedApiGet', () => {
  beforeEach(() => {
    resetApiCacheForTests();
    window.localStorage.clear();
    jest.useRealTimers();
  });

  test('dedupes simultaneous identical requests', async () => {
    const fetcher = jest.fn(() => Promise.resolve({ data: { ok: true } }));

    const [a, b] = await Promise.all([
      cachedApiGet('dashboard-summary:u1:acc1', fetcher),
      cachedApiGet('dashboard-summary:u1:acc1', fetcher),
    ]);

    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(a.data).toEqual({ ok: true });
    expect(b.data).toEqual({ ok: true });
  });

  test('returns cached data inside ttl', async () => {
    const fetcher = jest.fn(() => Promise.resolve({ data: { count: 1 } }));

    await cachedApiGet('dashboard-summary:u1:acc1', fetcher, { ttlMs: 60000 });
    const second = await cachedApiGet('dashboard-summary:u1:acc1', fetcher, { ttlMs: 60000 });

    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(second.cached).toBe(true);
    expect(getCachedApiData('dashboard-summary:u1:acc1')).toEqual({ count: 1 });
  });

  test('force refresh bypasses cached data', async () => {
    const fetcher = jest
      .fn()
      .mockResolvedValueOnce({ data: { count: 1 } })
      .mockResolvedValueOnce({ data: { count: 2 } });

    await cachedApiGet('dashboard-summary:u1:acc1', fetcher, { ttlMs: 60000 });
    const refreshed = await cachedApiGet('dashboard-summary:u1:acc1', fetcher, {
      ttlMs: 60000,
      force: true,
    });

    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(refreshed.data).toEqual({ count: 2 });
  });

  test('invalidate clears matching keys only', async () => {
    await cachedApiGet('dashboard-summary:u1:acc1', () => ({ data: { a: 1 } }));
    await cachedApiGet('comments:u1:acc1', () => ({ data: { b: 2 } }));

    invalidateApiCache('dashboard-summary');

    expect(getCachedApiData('dashboard-summary:u1:acc1')).toBeUndefined();
    expect(getCachedApiData('comments:u1:acc1')).toEqual({ b: 2 });
  });

  test('clearApiCache removes all user-scoped cached data', async () => {
    await cachedApiGet('dashboard-summary:u1:acc1', () => ({ data: { a: 1 } }));
    await cachedApiGet('comments:u2:acc2', () => ({ data: { b: 2 } }));

    clearApiCache();

    expect(getCachedApiData('dashboard-summary:u1:acc1')).toBeUndefined();
    expect(getCachedApiData('comments:u2:acc2')).toBeUndefined();
  });

  test('does not cache non-json html responses', async () => {
    const fetcher = jest.fn(() => Promise.resolve({
      status: 200,
      headers: { 'content-type': 'text/html; charset=utf-8' },
      data: '<html>not api json</html>',
    }));

    await expect(cachedApiGet('html-response:u1', fetcher)).rejects.toMatchObject({
      code: 'api_cache_uncacheable_response',
    });

    expect(getCachedApiData('html-response:u1')).toBeUndefined();
  });

  test('does not cache 403 responses returned by a fetcher', async () => {
    const fetcher = jest.fn(() => Promise.resolve({
      status: 403,
      headers: { 'content-type': 'application/json' },
      data: { detail: 'Forbidden' },
    }));

    await expect(cachedApiGet('forbidden:u1', fetcher)).rejects.toMatchObject({
      code: 'api_cache_uncacheable_response',
    });

    expect(getCachedApiData('forbidden:u1')).toBeUndefined();
  });

  test('does not preserve cached data after a 401 refresh failure', async () => {
    const authError = new Error('unauthorized');
    authError.response = { status: 401 };
    const fetcher = jest
      .fn()
      .mockResolvedValueOnce({ data: { count: 1 } })
      .mockRejectedValueOnce(authError);

    await cachedApiGet('auth-sensitive:u1', fetcher, { ttlMs: 1 });
    await expect(cachedApiGet('auth-sensitive:u1', fetcher, { ttlMs: 1, force: true })).rejects.toBe(authError);

    expect(getCachedApiData('auth-sensitive:u1')).toBeUndefined();
  });

  test('SWR returns stale cached data immediately and refreshes in background', async () => {
    jest.useFakeTimers();
    jest.setSystemTime(1000);
    const fetcher = jest
      .fn()
      .mockResolvedValueOnce({ data: { count: 1 } })
      .mockResolvedValueOnce({ data: { count: 2 } });
    const onUpdate = jest.fn();

    await cachedApiGetSWR('dashboard-summary:u1:acc1', fetcher, { ttlMs: 1000 });
    jest.setSystemTime(2500);
    const result = await cachedApiGetSWR('dashboard-summary:u1:acc1', fetcher, {
      ttlMs: 1000,
      maxStaleMs: 10000,
      onUpdate,
    });

    expect(result.data).toEqual({ count: 1 });
    expect(result.cached).toBe(true);
    expect(result.stale).toBe(true);
    expect(result.refreshing).toBe(true);
    await result.promise;
    expect(onUpdate).toHaveBeenCalledWith({ count: 2 }, expect.objectContaining({ cached: false }));
    expect(getCachedApiData('dashboard-summary:u1:acc1')).toEqual({ count: 2 });
    jest.useRealTimers();
  });

  test('SWR dedupes background refreshes for the same stale key', async () => {
    jest.useFakeTimers();
    jest.setSystemTime(1000);
    const fetcher = jest
      .fn()
      .mockResolvedValueOnce({ data: { count: 1 } })
      .mockResolvedValueOnce({ data: { count: 2 } });

    await cachedApiGetSWR('comments:u1:acc1', fetcher, { ttlMs: 1000 });
    jest.setSystemTime(2500);
    const [a, b] = await Promise.all([
      cachedApiGetSWR('comments:u1:acc1', fetcher, { ttlMs: 1000, maxStaleMs: 10000 }),
      cachedApiGetSWR('comments:u1:acc1', fetcher, { ttlMs: 1000, maxStaleMs: 10000 }),
    ]);

    expect(a.data).toEqual({ count: 1 });
    expect(b.data).toEqual({ count: 1 });
    expect(fetcher).toHaveBeenCalledTimes(2);
    await a.promise;
    jest.useRealTimers();
  });

  test('SWR expired cache forces foreground refresh instead of returning stale data', async () => {
    jest.useFakeTimers();
    jest.setSystemTime(1000);
    const fetcher = jest
      .fn()
      .mockResolvedValueOnce({ data: { count: 1 } })
      .mockResolvedValueOnce({ data: { count: 2 } });

    await cachedApiGetSWR('dashboard-summary:u1:acc1', fetcher, { ttlMs: 1000, maxStaleMs: 2000 });
    jest.setSystemTime(5000);
    const result = await cachedApiGetSWR('dashboard-summary:u1:acc1', fetcher, {
      ttlMs: 1000,
      maxStaleMs: 2000,
    });

    expect(result.data).toEqual({ count: 2 });
    expect(result.cached).toBe(false);
    expect(fetcher).toHaveBeenCalledTimes(2);
    jest.useRealTimers();
  });

  test('persistent safe snapshots survive an in-memory cache reset', async () => {
    const fetcher = jest.fn(() => Promise.resolve({ data: { totalContacts: 4 } }));

    await cachedApiGetSWR('dashboard-summary:u1:acc1', fetcher, {
      ttlMs: 1000,
      maxStaleMs: 300000,
      persist: true,
    });
    clearApiMemoryCacheForTests();

    expect(getCachedApiData('dashboard-summary:u1:acc1', { maxStaleMs: 300000 })).toEqual({ totalContacts: 4 });
    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  test('expired persistent snapshots are not returned as usable data', async () => {
    jest.useFakeTimers();
    jest.setSystemTime(1000);
    await cachedApiGetSWR('dashboard-summary:u1:acc1', () => ({ data: { totalContacts: 4 } }), {
      ttlMs: 1000,
      maxStaleMs: 2000,
      persist: true,
    });
    clearApiMemoryCacheForTests();
    jest.setSystemTime(5000);

    expect(getCachedApiData('dashboard-summary:u1:acc1', { maxStaleMs: 2000 })).toBeUndefined();
    jest.useRealTimers();
  });

  test('failed and html responses are never persisted', async () => {
    await expect(cachedApiGetSWR('dashboard-summary:u1:acc1', () => ({
      status: 500,
      headers: { 'content-type': 'application/json' },
      data: { detail: 'server_error' },
    }), { persist: true })).rejects.toMatchObject({ code: 'api_cache_uncacheable_response' });

    await expect(cachedApiGetSWR('automations-summary:u1:acc1', () => ({
      status: 200,
      headers: { 'content-type': 'text/html' },
      data: '<html />',
    }), { persist: true })).rejects.toMatchObject({ code: 'api_cache_uncacheable_response' });

    clearApiMemoryCacheForTests();

    expect(getCachedApiData('dashboard-summary:u1:acc1')).toBeUndefined();
    expect(getCachedApiData('automations-summary:u1:acc1')).toBeUndefined();
  });

  test('logout-style clear removes persisted snapshots', async () => {
    await cachedApiGetSWR('dashboard-summary:u1:acc1', () => ({ data: { count: 1 } }), { persist: true });
    await cachedApiGetSWR('instagram-accounts:u1', () => ({ data: { accounts: [] } }), { persist: true });

    clearApiCache();

    clearApiMemoryCacheForTests();
    expect(getCachedApiData('dashboard-summary:u1:acc1')).toBeUndefined();
    expect(getCachedApiData('instagram-accounts:u1')).toBeUndefined();
  });

  test('account-scoped invalidation removes only matching persisted snapshots', async () => {
    await cachedApiGetSWR('dashboard-summary:u1:acc1', () => ({ data: { count: 1 } }), { persist: true });
    await cachedApiGetSWR('dashboard-summary:u1:acc2', () => ({ data: { count: 2 } }), { persist: true });

    invalidateApiCache('dashboard-summary:u1:acc1');
    clearApiMemoryCacheForTests();

    expect(getCachedApiData('dashboard-summary:u1:acc1')).toBeUndefined();
    expect(getCachedApiData('dashboard-summary:u1:acc2')).toEqual({ count: 2 });
  });

  test('forbidden token-like fields are redacted before persistence', async () => {
    await cachedApiGetSWR('dashboard-summary:u1:acc1', () => ({
      data: {
        ok: true,
        access_token: 'secret-token',
        nested: { password: 'secret-password', count: 1 },
      },
    }), { persist: true });

    clearApiMemoryCacheForTests();

    expect(getCachedApiData('dashboard-summary:u1:acc1')).toEqual({ ok: true, nested: { count: 1 } });
  });
});

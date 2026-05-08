import {
  cachedApiGet,
  getCachedApiData,
  invalidateApiCache,
  resetApiCacheForTests,
} from './apiCache';

describe('cachedApiGet', () => {
  beforeEach(() => {
    resetApiCacheForTests();
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
});

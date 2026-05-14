const fs = require('fs');
const path = require('path');

const source = fs.readFileSync(path.join(__dirname, 'Dashboard.jsx'), 'utf8');

describe('Dashboard performance wiring', () => {
  test('uses compact summary endpoint instead of heavy stats plus automations waterfall', () => {
    expect(source).toContain("/dashboard/summary");
    expect(source).not.toContain("/dashboard/stats");
    expect(source).not.toContain("api.get('/automations')");
  });

  test('uses cache and localized loading states', () => {
    expect(source).toContain('cachedApiGet');
    expect(source).toContain('persist: true');
    expect(source).toContain('maxStaleMs: DASHBOARD_MAX_STALE_MS');
    expect(source).not.toContain('timeout: 8000');
    expect(source).toContain('dashboard-skeleton');
    expect(source).toContain('dashboard-chart-skeleton');
    expect(source).toContain('dashboard-refresh');
  });

  test('keeps account-scoped cache keys', () => {
    expect(source).toContain('activeInstagramAccountId');
    expect(source).toContain('activeInstagramIgUserId');
    expect(source).toContain('dashboard-summary');
  });

  test('shows a connection-specific error for Instagram account problems', () => {
    expect(source).toContain('Connect or reconnect Instagram to load dashboard data.');
    expect(source).toContain('instagramConnectionValid');
    expect(source).toContain('no instagram account');
  });

  test('uses silent background refresh without visible updated-time labels', () => {
    expect(source).toContain("Couldn't refresh. Showing the latest available data.");
    expect(source).not.toContain('Showing cached dashboard data. Refresh failed.');
    expect(source).not.toMatch(/updated .* ago/i);
    expect(source).not.toMatch(/last updated/i);
    expect(source).not.toContain('تم التحديث منذ');
  });
});

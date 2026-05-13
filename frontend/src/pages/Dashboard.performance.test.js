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
});

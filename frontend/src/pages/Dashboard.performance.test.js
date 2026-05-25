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
    expect(source).toContain('range');
  });

  test('keeps dashboard focused on four primary KPIs', () => {
    expect(source).toContain('statsCards = [');
    expect(source).toContain('dashboard-more-stats-toggle');
    expect(source).toContain('showMoreStats &&');
    // Top Automations: active-first sort + cap at 3.
    expect(source).toContain('.slice(0, 3)');
    expect(source).toMatch(/aActive\s*-\s*bActive/);
  });

  test('chart title is range-aware', () => {
    expect(source).toContain("dashboard.performanceTitles.${range}");
    expect(source).toContain('dashboard-chart-title');
  });

  test('More stats button uses i18n labels and chevron icon', () => {
    expect(source).toContain("t('dashboard.lessStats')");
    expect(source).toContain("t('dashboard.moreStats')");
    expect(source).toMatch(/ChevronUp|ChevronDown/);
  });

  test('Top Automations row de-emphasizes paused entries', () => {
    expect(source).toContain('dashboard-top-automation-row');
    expect(source).toMatch(/isActive\s*\?\s*''\s*:\s*'opacity-70'/);
  });

  test('chart tooltip forces locale instead of using browser default', () => {
    expect(source).toContain('chartLocale');
    expect(source).toContain('formatChartTooltipTitle(d.date || d.day, range, chartLocale)');
  });

  test('uses sparse range-aware x-axis labels', () => {
    expect(source).toContain('dashboardTickIndexes');
    expect(source).toContain("rangeKey === '24h'");
    expect(source).toContain("rangeKey === '30d'");
    expect(source).toContain('dashboard-x-axis');
    expect(source).not.toContain('grid grid-cols-7 gap-2 sm:gap-3');
  });

  test('keeps previous range data visible while refreshing', () => {
    expect(source).toContain('Keep the previous range visible');
    expect(source).toContain('dashboard-range-refreshing');
    expect(source).toContain('nextRange === range');
    expect(source).not.toContain('setStats(null);');
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

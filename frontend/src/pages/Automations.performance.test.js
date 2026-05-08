const fs = require('fs');
const path = require('path');

const source = fs.readFileSync(path.join(__dirname, 'Automations.jsx'), 'utf8');

describe('Automations performance wiring', () => {
  test('list page uses compact summary endpoint with cache', () => {
    expect(source).toContain('/automations/summary');
    expect(source).toContain('cachedApiGet');
    expect(source).toContain('automations-summary');
    expect(source).toContain('AUTOMATIONS_TTL_MS');
    expect(source).not.toContain("api.get('/automations')");
  });

  test('full automation is fetched lazily only for editing', () => {
    expect(source).toContain('loadAutomationDetail');
    expect(source).toContain('automation-detail:${automation.id}');
    expect(source).toContain('api.get(`/automations/${automation.id}`');
  });

  test('mutations invalidate only automation caches and refresh intentionally', () => {
    expect(source).toContain("invalidateApiCache('automations-summary')");
    expect(source).toContain('invalidateApiCache(`automation-detail:${a.id}`)');
    expect(source).toContain('refresh({ force: true })');
  });

  test('page has localized skeleton and manual refresh state', () => {
    expect(source).toContain('automations-skeleton');
    expect(source).toContain('automations-refresh');
    expect(source).toContain('Showing cached automations. Refresh failed.');
  });
});

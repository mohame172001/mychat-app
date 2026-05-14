const fs = require('fs');
const path = require('path');

const source = fs.readFileSync(path.join(__dirname, 'appWarmup.js'), 'utf8');

describe('app boot warmup wiring', () => {
  test('preloads core route chunks and not admin data by default', () => {
    expect(source).toContain("['Dashboard', 'Automations', 'Comments']");
    expect(source).toContain("'Admin'");
    expect(source).not.toContain('/api/admin');
  });

  test('prefetches core page data with persistent safe snapshots', () => {
    expect(source).toContain('/dashboard/summary');
    expect(source).toContain('/automations/summary');
    expect(source).toContain('/comments');
    expect(source).toContain('/instagram/accounts');
    expect((source.match(/persist: true/g) || []).length).toBeGreaterThanOrEqual(4);
  });

  test('runs low-priority and dedupes repeated warmups by user account scope', () => {
    expect(source).toContain('requestIdleCallback');
    expect(source).toContain('setTimeout');
    expect(source).toContain('scheduledScope === scope');
    expect(source).toContain('activeInstagramAccountId');
    expect(source).toContain('activeInstagramIgUserId');
  });
});

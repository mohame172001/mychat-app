const fs = require('fs');
const path = require('path');

const source = fs.readFileSync(path.join(__dirname, 'appWarmup.js'), 'utf8');

describe('app boot warmup wiring', () => {
  test('preloads core route chunks and not admin data by default', () => {
    expect(source).toContain("['Dashboard', 'Automations']");
    expect(source).toContain("'Admin'");
    expect(source).not.toContain('/api/admin');
  });

  test('prefetches core page data with persistent safe snapshots', () => {
    expect(source).toContain('/dashboard/summary');
    expect(source).toContain('/automations/summary');
    expect(source).toContain('/instagram/accounts');
    // Comments page was removed; comments must not be warmed up.
    expect(source).not.toContain('/comments');
    expect((source.match(/persist: true/g) || []).length).toBeGreaterThanOrEqual(3);
  });

  test('admin users also warm admin overview + admin members', () => {
    expect(source).toContain('/admin/overview');
    expect(source).toContain('/admin/members');
    expect(source).toContain('if (isAdmin)');
  });

  test('fires prefetch immediately (no idle delay) and dedupes repeated warmups by scope', () => {
    // Phase 2.18F: prefetch must not be wrapped in requestIdleCallback /
    // setTimeout — that delayed the critical first /dashboard/summary
    // call until after the Dashboard mounted.
    expect(source).not.toContain('requestIdleCallback');
    expect(source).not.toContain('runWhenIdle');
    expect(source).toContain('scheduledScope === scope');
    expect(source).toContain('activeInstagramAccountId');
    expect(source).toContain('activeInstagramIgUserId');
  });
});

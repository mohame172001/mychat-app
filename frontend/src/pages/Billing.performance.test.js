const fs = require('fs');
const path = require('path');

const source = fs.readFileSync(path.join(__dirname, 'Billing.jsx'), 'utf8');

describe('Billing responsiveness wiring', () => {
  test('uses shared cache for plan and plan catalog requests', () => {
    expect(source).toContain('cachedApiGet');
    expect(source).toContain('billing:plan-current');
    expect(source).toContain('billing:plans');
    expect(source).toContain("${user?.id || 'anon'}");
    expect(source).toContain('force: true');
  });
});

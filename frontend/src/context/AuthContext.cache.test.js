const fs = require('fs');
const path = require('path');

describe('auth cache isolation wiring', () => {
  test('auth transitions clear user-scoped API cache', () => {
    const source = fs.readFileSync(path.join(__dirname, 'AuthContext.jsx'), 'utf8');

    // Both clearApiCache (logout / login / signup / google / parse-fail)
    // and seedApiCacheEntry (bootstrap response → cache) come from
    // ../lib/apiCache. The import may bundle them together.
    expect(source).toMatch(/from\s+'\.\.\/lib\/apiCache'/);
    expect(source).toContain('clearApiCache');
    expect(source).toContain("import { scheduleCoreAppWarmup } from '../lib/appWarmup';");
    expect((source.match(/clearApiCache\(\);/g) || []).length).toBeGreaterThanOrEqual(5);
    expect((source.match(/scheduleCoreAppWarmup\(/g) || []).length).toBeGreaterThanOrEqual(4);
  });

  test('401 interceptor clears user-scoped API cache before redirecting', () => {
    const source = fs.readFileSync(path.join(__dirname, '../lib/api.js'), 'utf8');

    expect(source).toContain("import { clearApiCache } from './apiCache';");
    expect(source).toContain('if (status === 401)');
    expect(source).toContain('clearApiCache();');
    expect(source).toContain("window.location.href = '/login'");
  });
});

const fs = require('fs');
const path = require('path');

describe('production route surface', () => {
  const appSource = fs.readFileSync(path.join(__dirname, 'App.js'), 'utf8');
  const routesSource = fs.readFileSync(path.join(__dirname, 'constants', 'routes.js'), 'utf8');

  test('does not expose the Instagram diagnostics UI route', () => {
    expect(appSource).not.toContain('admin/instagram-diagnostics');
    expect(routesSource).not.toContain('admin/instagram-diagnostics');
    expect(appSource).not.toContain('InstagramDiagnostics');
    expect(appSource).not.toContain("import('./pages/admin/InstagramDiagnostics')");
  });

  test('keeps the normal app and admin routes stable', () => {
    expect(routesSource).toContain("APP: '/app'");
    expect(routesSource).toContain("APP_AUTOMATIONS: '/app/automations'");
    expect(routesSource).toContain("APP_DM_AUTOMATION: '/app/dm-automation'");
    expect(routesSource).toContain("APP_SETTINGS: '/app/settings'");
    expect(routesSource).toContain("APP_BILLING: '/app/billing'");
    expect(routesSource).toContain("APP_ADMIN: '/app/admin'");
    expect(appSource).toContain('path={ROUTES.APP}');
    expect(appSource).toContain('path={APP_CHILD_ROUTES.AUTOMATIONS}');
  });
});

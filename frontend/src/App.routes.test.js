const fs = require('fs');
const path = require('path');

describe('production route surface', () => {
  const appSource = fs.readFileSync(path.join(__dirname, 'App.js'), 'utf8');

  test('does not expose the Instagram diagnostics UI route', () => {
    expect(appSource).not.toContain('admin/instagram-diagnostics');
    expect(appSource).not.toContain('InstagramDiagnostics');
    expect(appSource).not.toContain("import('./pages/admin/InstagramDiagnostics')");
  });

  test('keeps the normal app and admin routes stable', () => {
    expect(appSource).toContain('<Route path="/app"');
    expect(appSource).toContain('path="automations"');
    expect(appSource).toContain('path="dm-automation"');
    expect(appSource).toContain('path="settings"');
    expect(appSource).toContain('path="billing"');
    expect(appSource).toContain('path="admin"');
  });
});

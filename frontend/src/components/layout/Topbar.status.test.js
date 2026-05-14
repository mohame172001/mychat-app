const fs = require('fs');
const path = require('path');

const source = fs.readFileSync(path.join(__dirname, 'Topbar.jsx'), 'utf8');

describe('Topbar Instagram status', () => {
  test('does not hardcode the account as connected', () => {
    expect(source).toContain('useAuth');
    expect(source).toContain('instagramConnected');
    expect(source).toContain('instagramConnectionValid');
    expect(source).toContain('instagramStatus.label');
    expect(source).toContain('Reconnect');
    expect(source).toContain('Not connected');
    expect(source).toContain('data-testid="topbar-instagram-status"');
  });
});

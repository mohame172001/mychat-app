import { instagramConnectUrlPath } from '../../lib/instagramConnect';
const fs = require('fs');
const path = require('path');

test('connect another account uses Instagram OAuth start flow', () => {
  const path = instagramConnectUrlPath({ mode: 'add_account', returnTo: '/app' });

  expect(path).toContain('/instagram/auth-url?');
  expect(path).toContain('mode=add_account');
  expect(path).toContain('returnTo=%2Fapp');
  expect(path).not.toContain('/app/settings');
});

test('account switch uses client navigation instead of full page reload', () => {
  const source = fs.readFileSync(path.join(__dirname, 'Sidebar.jsx'), 'utf8');

  expect(source).toContain('useNavigate');
  expect(source).toContain("invalidateApiCache('dashboard-summary')");
  expect(source).not.toContain('window.location.assign');
});

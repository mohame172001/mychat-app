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

test('default Instagram connect path is not treated as adding another account', () => {
  const path = instagramConnectUrlPath({ returnTo: '/app/settings?tab=instagram' });

  expect(path).toContain('/instagram/auth-url?');
  expect(path).toContain('mode=connect');
  expect(path).not.toContain('mode=add_account');
});

test('account switch uses client navigation instead of full page reload', () => {
  const source = fs.readFileSync(path.join(__dirname, 'Sidebar.jsx'), 'utf8');

  expect(source).toContain('useNavigate');
  expect(source).toContain('cachedApiGetSWR');
  expect(source).toContain('instagram-accounts');
  expect(source).toContain("connectMode === 'reconnect'");
  expect(source).toContain('Reconnect Instagram');
  expect(source).toContain('Connect Instagram');
  expect(source).toContain('setInstagramAccounts([])');
  expect(source).toContain('invalidateApiCache(accountsCacheKey)');
  expect(source).toContain("invalidateApiCache('dashboard-summary')");
  expect(source).toContain("invalidateApiCache('automations-summary')");
  // Comments page was removed; no comments cache prefix to invalidate.
  expect(source).not.toContain("invalidateApiCache('comments");
  expect(source).toContain('scheduleCoreAppWarmup');
  expect(source).not.toContain('window.location.assign');
});

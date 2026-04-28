import { instagramConnectUrlPath } from '../../lib/instagramConnect';

test('connect another account uses Instagram OAuth start flow', () => {
  const path = instagramConnectUrlPath({ mode: 'add_account', returnTo: '/app' });

  expect(path).toContain('/instagram/auth-url?');
  expect(path).toContain('mode=add_account');
  expect(path).toContain('returnTo=%2Fapp');
  expect(path).not.toContain('/app/settings');
});

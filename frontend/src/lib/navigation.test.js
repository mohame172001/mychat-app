import { authDestination, safeAppReturnTo } from './navigation';

test.each([
  '/app', '/app/settings?tab=instagram', '/app/billing',
  '/app/automations?create=1', '/app/automations/rule-123',
])('keeps the requested product destination: %s', path => {
  expect(safeAppReturnTo(path)).toBe(path);
  expect(new URLSearchParams(authDestination('/login', path).split('?')[1]).get('next')).toBe(path);
});

test.each([
  null, '', 'https://evil.example', '//evil.example', '/app\\evil',
  '/app/../../elsewhere', '/app%2f%2fevil.example', '/app/unknown',
  '/login', '/app\n/settings', '/application', '/app/automations/../../login',
])('rejects an unsafe or unknown destination: %s', path => {
  expect(safeAppReturnTo(path)).toBe('/app');
});

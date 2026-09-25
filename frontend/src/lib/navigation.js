import { ROUTES } from '../constants/routes';

const appPaths = new Set([
  '/app', '/app/automations', '/app/dm-automation', '/app/settings',
  '/app/billing', '/app/admin', '/app/admin/specific-reply-debug',
]);

export function safeAppReturnTo(value) {
  if (typeof value !== 'string' || !value.startsWith('/app')
      || /[\\\u0000-\u0020]/.test(value) || /%2f|%5c/i.test(value)) return ROUTES.APP;
  try {
    const url = new URL(value, 'https://navigation.invalid');
    if (url.origin !== 'https://navigation.invalid') return ROUTES.APP;
    if (!appPaths.has(url.pathname) && !/^\/app\/automations\/[A-Za-z0-9_-]+$/.test(url.pathname)) return ROUTES.APP;
    return url.pathname + url.search + url.hash;
  } catch {
    return ROUTES.APP;
  }
}

export function authDestination(path, returnTo) {
  return `${path}?next=${encodeURIComponent(safeAppReturnTo(returnTo))}`;
}

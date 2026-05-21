import { instagramApi } from '../api/instagramApi';

export const instagramConnectUrlPath = ({ mode = 'connect', returnTo = '/app' } = {}) => {
  const params = new URLSearchParams({ mode, returnTo });
  return `/instagram/auth-url?${params.toString()}`;
};

// Allowlist of hosts we'll actually navigate to during the OAuth flow.
// The backend should only ever return a Meta-owned URL here, but this
// extra guard means a compromised backend (or a future code path that
// accidentally proxies the value somewhere else) cannot redirect us to
// an attacker's site.
const ALLOWED_OAUTH_HOSTS = new Set([
  'www.instagram.com',
  'instagram.com',
  'api.instagram.com',
  'www.facebook.com',
  'facebook.com',
  'm.facebook.com',
]);

function isSafeOAuthRedirect(url) {
  try {
    const parsed = new URL(url);
    if (parsed.protocol !== 'https:') return false;
    return ALLOWED_OAUTH_HOSTS.has(parsed.hostname.toLowerCase());
  } catch (_) {
    return false;
  }
}

export const startInstagramConnect = async ({ mode = 'connect', returnTo = '/app' } = {}) => {
  const { data } = await instagramApi.authUrl(instagramConnectUrlPath({ mode, returnTo }));
  if (!data?.url) throw new Error('Instagram OAuth URL was not returned');
  if (!isSafeOAuthRedirect(data.url)) {
    throw new Error('refusing_unsafe_oauth_redirect');
  }
  window.location.href = data.url;
  return data.url;
};

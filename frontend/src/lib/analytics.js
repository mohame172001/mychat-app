/**
 * Phase 2.5 — frontend product analytics shim (PostHog).
 *
 * Optional. If REACT_APP_POSTHOG_KEY is unset OR posthog-js isn't
 * installed, every export below is a safe no-op. Build does not break.
 *
 * Privacy contract:
 *   - Never sends raw comment / reply / DM / message text.
 *   - Never sends tokens / Authorization values.
 *   - Strips OAuth code/state from page paths.
 *   - User identification uses user_id only; email is optional and
 *     only sent when explicitly passed.
 *
 * Pure helpers (`redactProperties`, `safePagePath`) are unit-tested
 * without any SDK dependency.
 */

// Property keys that must NEVER be sent to PostHog as event properties.
const FORBIDDEN_PROPERTY_KEYS = new Set([
  'access_token', 'meta_access_token', 'authorization', 'token', 'jwt',
  'code', 'state', 'cookie', 'set-cookie',
  'comment_text', 'comment', 'comment_message',
  'reply_text', 'reply_message',
  'dm_text', 'dm_message',
  'message_text', 'message', 'private_message',
  'caption', 'text', 'raw', 'body', 'payload', 'graph_error', 'error_body',
  'password',
  // Phase 2.7 Google Sign-In.
  'credential', 'id_token', 'google_id_token', 'google_credential',
  'refresh_token',
]);

// Tokens in URL query strings that signal we should drop the whole query.
const QUERY_STRIP_TOKENS = ['code=', 'state=', 'token=', 'access_token=', 'jwt='];

function lower(s) { return String(s || '').toLowerCase(); }

/**
 * Strip forbidden keys (one level deep) from a properties object.
 * Returns a new object — never mutates input. Safe on null/undefined.
 */
export function redactProperties(props) {
  if (!props || typeof props !== 'object') return {};
  const cleaned = {};
  for (const [k, v] of Object.entries(props)) {
    if (FORBIDDEN_PROPERTY_KEYS.has(lower(k))) continue;
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      // shallow scrub of nested
      const inner = {};
      for (const [k2, v2] of Object.entries(v)) {
        if (FORBIDDEN_PROPERTY_KEYS.has(lower(k2))) continue;
        inner[k2] = v2;
      }
      cleaned[k] = inner;
    } else {
      cleaned[k] = v;
    }
  }
  return cleaned;
}

/**
 * Return the route path with OAuth-sensitive query params dropped.
 * Pure function — tested without browser globals.
 */
export function safePagePath(input) {
  try {
    const url = new URL(input, 'http://localhost');
    return url.pathname;
  } catch (_) {
    const s = String(input || '/');
    if (QUERY_STRIP_TOKENS.some((t) => lower(s).includes(t))) {
      return s.split('?')[0] || '/';
    }
    return s;
  }
}


// ---- runtime state --------------------------------------------------------

let _posthog = null;        // resolved SDK instance or null
let _initPromise = null;    // dedupe init across React strict-mode double mounts
let _enabled = false;

function _key() {
  return (typeof process !== 'undefined' && process.env && process.env.REACT_APP_POSTHOG_KEY) || '';
}
function _host() {
  return (typeof process !== 'undefined' && process.env && process.env.REACT_APP_POSTHOG_HOST)
    || 'https://app.posthog.com';
}

/**
 * Return sanitized analytics status for SystemHealth UI. Never echoes
 * the API key value.
 */
export function analyticsStatus() {
  return {
    posthog_configured: Boolean(_key()),
    posthog_initialized: _enabled,
    host_set: Boolean((typeof process !== 'undefined' && process.env && process.env.REACT_APP_POSTHOG_HOST)),
    service: 'frontend',
  };
}

/**
 * Initialize PostHog if a key is present and posthog-js loads. No-ops
 * cleanly otherwise. Idempotent.
 */
export async function init() {
  if (_initPromise) return _initPromise;
  _initPromise = (async () => {
    const key = _key();
    if (!key) return false;
    try {
      const mod = await import(/* webpackChunkName: "posthog" */ 'posthog-js').catch(() => null);
      const posthog = mod?.default || mod;
      if (!posthog || typeof posthog.init !== 'function') return false;
      posthog.init(key, {
        api_host: _host(),
        autocapture: false,                  // we send explicit events only
        capture_pageview: false,             // we drive page_view ourselves
        disable_session_recording: true,
        persistence: 'localStorage+cookie',
        // Light sanitiser at the SDK boundary too.
        sanitize_properties: (props /* , event */) => redactProperties(props || {}),
      });
      _posthog = posthog;
      _enabled = true;
      return true;
    } catch (_e) {
      return false;
    }
  })();
  return _initPromise;
}

/**
 * Identify the current user. user_id is the distinct id. Email is
 * optional and only sent when the caller explicitly passes it.
 */
export function identify(user) {
  if (!_enabled || !_posthog || !user || !user.id) return;
  try {
    const props = redactProperties({
      ...(user.email ? { email: user.email } : {}),
      plan_key: user.plan_key,
    });
    _posthog.identify(String(user.id), props);
  } catch (_e) { /* ignore */ }
}

/**
 * Capture an event. Properties are redacted before send. Forbidden keys
 * are dropped silently.
 */
export function capture(eventName, properties) {
  if (!_enabled || !_posthog || !eventName) return;
  try {
    _posthog.capture(eventName, redactProperties(properties || {}));
  } catch (_e) { /* ignore */ }
}

/**
 * Reset on logout / account switch.
 */
export function reset() {
  if (!_enabled || !_posthog) return;
  try { _posthog.reset(); } catch (_e) { /* ignore */ }
}

/**
 * Capture a page_view event for the current route. Strips OAuth params.
 */
export function pageView(pathOrUrl) {
  if (!_enabled || !_posthog) return;
  capture('page_view', { route: safePagePath(pathOrUrl) });
}


export default {
  init,
  identify,
  capture,
  reset,
  pageView,
  redactProperties,
  safePagePath,
  analyticsStatus,
};

/**
 * Phase 2.5 — frontend Sentry shim.
 *
 * Optional. If REACT_APP_SENTRY_DSN is unset or @sentry/react isn't
 * installed, this module silently no-ops. Build does not break.
 *
 * The pure functions (`redactSentryEvent`, `safeRouteFromUrl`) are
 * unit-tested without any SDK dependency.
 */

const FORBIDDEN_HEADER_KEYS = new Set([
  'authorization', 'cookie', 'set-cookie', 'access_token',
  'meta_access_token', 'x-hub-signature', 'x-hub-signature-256',
  'x-api-key', 'jwt', 'csrf-token', 'x-csrf-token',
]);

const FORBIDDEN_BODY_KEYS = new Set([
  'access_token', 'meta_access_token', 'authorization', 'token', 'jwt',
  'code', 'state', 'raw', 'body', 'payload', 'graph_error', 'error_body',
  'comment_text', 'comment', 'comment_message',
  'reply_text', 'reply_message',
  'dm_text', 'dm_message',
  'message_text', 'message', 'private_message',
  'text', 'caption',
  // Phase 2.7 Google Sign-In: never let an ID token / credential reach Sentry.
  'credential', 'id_token', 'google_id_token', 'google_credential',
  'refresh_token',
]);

const REDACT_BODY_PATH_PREFIXES = [
  '/api/instagram/webhook',
  '/api/comments/',
  '/api/dm-rules/',
  '/api/dm-automation',
  '/api/auth/google',
];

const QUERY_REDACT_TOKENS = ['token', 'code=', 'state=', 'access_token', 'jwt'];

function lower(s) { return String(s || '').toLowerCase(); }

function scrubMapping(value, depth = 0) {
  if (value === null || value === undefined || depth > 5) return value;
  if (Array.isArray(value)) return value.map((v) => scrubMapping(v, depth + 1));
  if (typeof value !== 'object') return value;
  const cleaned = {};
  for (const [k, v] of Object.entries(value)) {
    if (FORBIDDEN_BODY_KEYS.has(lower(k))) {
      cleaned[k] = '[redacted]';
      continue;
    }
    cleaned[k] = scrubMapping(v, depth + 1);
  }
  return cleaned;
}

function scrubRequest(request) {
  if (!request || typeof request !== 'object') return request;
  const out = { ...request };
  if (out.headers && typeof out.headers === 'object') {
    out.headers = Object.fromEntries(
      Object.entries(out.headers).map(([k, v]) => [
        k,
        FORBIDDEN_HEADER_KEYS.has(lower(k)) ? '[redacted]' : v,
      ]),
    );
  }
  if (out.cookies) out.cookies = '[redacted]';
  const qs = String(out.query_string || '');
  if (qs && QUERY_REDACT_TOKENS.some((t) => lower(qs).includes(t))) {
    out.query_string = '[redacted]';
  }
  const url = String(out.url || '');
  if (REDACT_BODY_PATH_PREFIXES.some((p) => url.includes(p))) {
    out.data = '[redacted]';
  } else if ('data' in out) {
    out.data = scrubMapping(out.data);
  }
  return out;
}

/**
 * Pure scrubber suitable for Sentry's `beforeSend`.
 * Returns the sanitized event (never returns null — we always send it).
 */
export function redactSentryEvent(event) {
  if (!event || typeof event !== 'object') return event;
  const out = { ...event };
  if (out.request) out.request = scrubRequest(out.request);
  if (out.extra) out.extra = scrubMapping(out.extra);
  if (out.contexts) out.contexts = scrubMapping(out.contexts);
  if (out.user && typeof out.user === 'object') {
    const safe = {};
    if (out.user.id !== undefined) safe.id = out.user.id;
    if (out.user.username !== undefined) safe.username = out.user.username;
    out.user = safe;
  }
  // Breadcrumbs
  if (out.breadcrumbs && Array.isArray(out.breadcrumbs.values)) {
    out.breadcrumbs = {
      ...out.breadcrumbs,
      values: out.breadcrumbs.values.map((c) => {
        if (!c || typeof c !== 'object') return c;
        const cc = { ...c };
        if (cc.data && typeof cc.data === 'object') cc.data = scrubMapping(cc.data);
        if (typeof cc.message === 'string'
            && /access_token=|authorization:/i.test(cc.message)) {
          cc.message = '[redacted]';
        }
        return cc;
      }),
    };
  }
  return out;
}

/**
 * Strip OAuth code / state / token query params from a URL so we don't
 * report them to Sentry / PostHog as part of the page path.
 */
export function safeRouteFromUrl(input) {
  try {
    const url = new URL(input, 'http://localhost');
    return url.pathname;
  } catch (_) {
    return String(input || '/');
  }
}


// ---- Init -----------------------------------------------------------------

let _initialized = false;
let _config = null;

/**
 * Returns sanitized status for the System Health UI.
 * Never echoes the DSN.
 */
export function sentryStatus() {
  return {
    sentry_configured: Boolean((typeof process !== 'undefined' && process.env && process.env.REACT_APP_SENTRY_DSN) || ''),
    sentry_initialized: _initialized,
    environment: (typeof process !== 'undefined' && process.env && process.env.REACT_APP_SENTRY_ENVIRONMENT) || 'unknown',
    build_sha: ((typeof process !== 'undefined' && process.env && (process.env.REACT_APP_BUILD_SHA || process.env.REACT_APP_GIT_SHA)) || '').slice(0, 12) || null,
    service: 'frontend',
  };
}

/**
 * Lazy-init Sentry. No-ops if DSN missing or @sentry/react not installed.
 * Returns true on success, false otherwise.
 */
export async function initSentry() {
  if (_initialized) return true;
  const dsn = (typeof process !== 'undefined' && process.env && process.env.REACT_APP_SENTRY_DSN) || '';
  if (!dsn) return false;
  try {
    // Dynamic import so missing dep doesn't break the build.
    const Sentry = await import(/* webpackChunkName: "sentry" */ '@sentry/react').catch(() => null);
    if (!Sentry || typeof Sentry.init !== 'function') return false;
    Sentry.init({
      dsn,
      environment: (process.env.REACT_APP_SENTRY_ENVIRONMENT || 'unknown'),
      release: (process.env.REACT_APP_BUILD_SHA || process.env.REACT_APP_GIT_SHA || undefined),
      tracesSampleRate: 0.0,
      sendDefaultPii: false,
      beforeSend: (event) => redactSentryEvent(event),
    });
    Sentry.setTag?.('service', 'frontend');
    _config = { dsn: '[present]', environment: process.env.REACT_APP_SENTRY_ENVIRONMENT || 'unknown' };
    _initialized = true;
    return true;
  } catch (_e) {
    return false;
  }
}

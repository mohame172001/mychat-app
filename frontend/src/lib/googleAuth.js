/**
 * Phase 2.7 — frontend Google Sign-In integration helper.
 *
 * Loads Google Identity Services on demand (CDN script tag), then
 * uses `window.google.accounts.id` to render a button or call the
 * One-Tap-style prompt.
 *
 * Privacy:
 *   - The credential JWT is forwarded to /api/auth/google and is never
 *     logged client-side.
 *   - When REACT_APP_GOOGLE_CLIENT_ID is unset, the helper returns
 *     {enabled: false} and the UI renders a disabled not-configured state.
 *   - Pure helpers (`isGoogleAuthEnabled`, `googleStatus`) are unit-tested
 *     without any browser globals.
 */

import api from './api';

const GIS_SCRIPT_SRC = 'https://accounts.google.com/gsi/client';

let _scriptPromise = null;
let _runtimeClientId = '';
let _configPromise = null;
let _lastConfigDiagnostics = {
  google_config_request_attempted: false,
  google_config_request_ok: false,
  google_config_response_enabled: false,
  google_config_response_was_json: false,
  google_config_error_code: '',
};

function setConfigDiagnostics(patch = {}) {
  _lastConfigDiagnostics = {
    google_config_request_attempted: false,
    google_config_request_ok: false,
    google_config_response_enabled: false,
    google_config_response_was_json: false,
    google_config_error_code: '',
    ...patch,
  };
  return { ..._lastConfigDiagnostics };
}

function responseHeader(headers, name) {
  if (!headers) return '';
  if (typeof headers.get === 'function') return headers.get(name) || '';
  return headers[name] || headers[name.toLowerCase()] || headers[name.toUpperCase()] || '';
}

function parseGoogleConfigResponse(response) {
  const contentType = String(responseHeader(response?.headers, 'content-type')).toLowerCase();
  const raw = response?.data;
  const isJsonContent = contentType.includes('application/json');

  if (raw && typeof raw === 'object' && !Array.isArray(raw)) {
    return { data: raw, responseWasJson: true };
  }

  if (typeof raw !== 'string') {
    const err = new Error('config_fetch_invalid_response');
    err.code = 'config_fetch_invalid_response';
    err.responseWasJson = isJsonContent;
    throw err;
  }

  const trimmed = raw.trim();
  if (!isJsonContent && !(trimmed.startsWith('{') || trimmed.startsWith('['))) {
    const err = new Error('config_fetch_invalid_response');
    err.code = 'config_fetch_invalid_response';
    err.responseWasJson = false;
    throw err;
  }

  try {
    return { data: JSON.parse(trimmed), responseWasJson: true };
  } catch (parseErr) {
    const err = new Error('config_fetch_invalid_response');
    err.code = 'config_fetch_invalid_response';
    err.responseWasJson = false;
    throw err;
  }
}

export function resetGoogleRuntimeConfigForTests() {
  _runtimeClientId = '';
  _configPromise = null;
  setConfigDiagnostics();
}

export function googleClientId() {
  return (typeof process !== 'undefined' && process.env && process.env.REACT_APP_GOOGLE_CLIENT_ID) || _runtimeClientId || '';
}

export function isGoogleAuthEnabled() {
  return Boolean(googleClientId());
}

export function googleStatus() {
  return {
    enabled: isGoogleAuthEnabled(),
    client_id_present: Boolean(googleClientId()),
    sdk_loaded: !!(typeof window !== 'undefined' && window.google && window.google.accounts && window.google.accounts.id),
    ..._lastConfigDiagnostics,
  };
}

export async function loadGoogleRuntimeConfig() {
  const envClientId = googleClientId();
  if (envClientId) {
    const diagnostics = setConfigDiagnostics({
      google_config_request_ok: true,
      google_config_response_enabled: true,
      google_config_response_was_json: true,
    });
    return {
      enabled: true,
      client_id: envClientId,
      source: _runtimeClientId ? 'backend' : 'env',
      ...diagnostics,
    };
  }
  if (!_configPromise) {
    setConfigDiagnostics({ google_config_request_attempted: true });
    _configPromise = api.get('/auth/google/config', {
      headers: { Accept: 'application/json' },
      transformResponse: [(data) => data],
    })
      .then((res) => {
        const { data, responseWasJson } = parseGoogleConfigResponse(res);
        const clientId = typeof data?.client_id === 'string' ? data.client_id.trim() : '';
        if (data?.enabled && clientId) {
          _runtimeClientId = clientId;
          const diagnostics = setConfigDiagnostics({
            google_config_request_attempted: true,
            google_config_request_ok: true,
            google_config_response_enabled: true,
            google_config_response_was_json: responseWasJson,
          });
          return {
            enabled: true,
            client_id: clientId,
            source: 'backend',
            ...diagnostics,
          };
        }
        const diagnostics = setConfigDiagnostics({
          google_config_request_attempted: true,
          google_config_request_ok: true,
          google_config_response_enabled: Boolean(data?.enabled),
          google_config_response_was_json: responseWasJson,
        });
        return {
          enabled: false,
          client_id: '',
          source: 'backend',
          ...diagnostics,
        };
      })
      .catch((err) => {
        const code = err?.code === 'config_fetch_invalid_response'
          ? 'config_fetch_invalid_response'
          : 'config_fetch_failed';
        const diagnostics = setConfigDiagnostics({
          google_config_request_attempted: true,
          google_config_request_ok: false,
          google_config_response_enabled: false,
          google_config_response_was_json: Boolean(err?.responseWasJson),
          google_config_error_code: code,
        });
        return {
          enabled: false,
          client_id: '',
          source: 'backend_error',
          ...diagnostics,
        };
      });
  }
  return _configPromise;
}

/**
 * Lazily inject the Google Identity Services script. Resolves with
 * `window.google` once available. Resolves with `null` if disabled or
 * if the script fails to load.
 */
export async function loadGoogleSdk() {
  if (typeof window === 'undefined') return null;
  const cfg = await loadGoogleRuntimeConfig();
  if (!cfg.enabled) return null;
  if (window.google && window.google.accounts && window.google.accounts.id) {
    return window.google;
  }
  if (!_scriptPromise) {
    _scriptPromise = new Promise((resolve) => {
      const existing = document.querySelector(`script[src="${GIS_SCRIPT_SRC}"]`);
      if (existing) {
        existing.addEventListener('load', () => resolve(window.google || null), { once: true });
        existing.addEventListener('error', () => resolve(null), { once: true });
        if (window.google && window.google.accounts && window.google.accounts.id) {
          resolve(window.google);
        }
        return;
      }
      const script = document.createElement('script');
      script.src = GIS_SCRIPT_SRC;
      script.async = true;
      script.defer = true;
      script.onload = () => resolve(window.google || null);
      script.onerror = () => {
        _scriptPromise = null;
        resolve(null);
      };
      document.head.appendChild(script);
    });
  }
  return _scriptPromise;
}

/**
 * Initialize the Google Identity Services client and render a button
 * inside `targetEl`. `onCredential` is called with the raw JWT string
 * when Google returns a credential. The caller is responsible for
 * POSTing it to /api/auth/google — this helper does not log it.
 *
 * Returns `true` on success, `false` when disabled or when the SDK
 * couldn't load.
 */
export async function renderGoogleButton(targetEl, { onCredential, onError } = {}) {
  if (!targetEl) return false;
  const cfg = await loadGoogleRuntimeConfig();
  if (!cfg.enabled) return false;
  const sdk = await loadGoogleSdk();
  if (!sdk || !sdk.accounts || !sdk.accounts.id) {
    if (onError) onError(new Error('google_sdk_unavailable'));
    return false;
  }
  try {
    sdk.accounts.id.initialize({
      client_id: googleClientId(),
      callback: (response) => {
        // The credential is the Google ID token JWT. Forward it,
        // do NOT log it.
        if (response && response.credential && typeof onCredential === 'function') {
          onCredential(response.credential);
        }
      },
      auto_select: false,
      ux_mode: 'popup',
    });
    sdk.accounts.id.renderButton(targetEl, {
      type: 'standard',
      size: 'large',
      theme: 'outline',
      text: 'continue_with',
      shape: 'rectangular',
      logo_alignment: 'left',
      width: targetEl.clientWidth || 320,
    });
    return true;
  } catch (err) {
    if (onError) onError(err);
    return false;
  }
}

/**
 * Map a backend HTTP error from /api/auth/google to a user-facing
 * message. Pure function — testable without React.
 */
export function googleErrorMessage(detail) {
  const s = String(detail || '').toLowerCase();
  if (!s) return 'Google sign-in failed';
  if (s.includes('google_auth_not_configured')) return 'Google sign-in is not configured';
  if (s.includes('google_auth_sdk_not_installed')) return 'Google sign-in is temporarily unavailable';
  if (s.includes('google_email_not_verified')) return 'Email not verified by Google';
  if (s.includes('google_account_already_linked')) return 'This Google account is already linked to another user';
  if (s.includes('google_account_conflict')) return 'A conflicting account already exists for this email or Google account';
  if (s.includes('google_credential_invalid:token_expired')) return 'Google sign-in expired, try again';
  if (s.includes('google_credential_invalid')) return 'Google sign-in failed';
  return 'Google sign-in failed';
}

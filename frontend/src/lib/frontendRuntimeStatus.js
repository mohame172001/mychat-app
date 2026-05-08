import { googleStatus, loadGoogleRuntimeConfig } from './googleAuth';

export function frontendRuntimeStatus() {
  const google = googleStatus();
  return {
    sentry_configured: Boolean(process.env.REACT_APP_SENTRY_DSN),
    posthog_configured: Boolean(process.env.REACT_APP_POSTHOG_KEY),
    google_sign_in_configured: google.enabled,
    google_sign_in_source: google.enabled ? 'available' : 'not_configured',
    google_config_request_attempted: google.google_config_request_attempted,
    google_config_request_ok: google.google_config_request_ok,
    google_config_response_enabled: google.google_config_response_enabled,
    google_config_response_was_json: google.google_config_response_was_json,
    google_config_error_code: google.google_config_error_code || null,
    environment: process.env.REACT_APP_SENTRY_ENVIRONMENT || 'unknown',
    build_sha: (process.env.REACT_APP_BUILD_SHA || process.env.REACT_APP_GIT_SHA || '').slice(0, 12) || null,
  };
}

export async function loadFrontendRuntimeStatus() {
  await loadGoogleRuntimeConfig();
  return frontendRuntimeStatus();
}

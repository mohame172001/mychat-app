import { googleStatus } from './googleAuth';

export function frontendRuntimeStatus() {
  return {
    sentry_configured: Boolean(process.env.REACT_APP_SENTRY_DSN),
    posthog_configured: Boolean(process.env.REACT_APP_POSTHOG_KEY),
    google_sign_in_configured: googleStatus().enabled,
    environment: process.env.REACT_APP_SENTRY_ENVIRONMENT || 'unknown',
    build_sha: (process.env.REACT_APP_BUILD_SHA || process.env.REACT_APP_GIT_SHA || '').slice(0, 12) || null,
  };
}

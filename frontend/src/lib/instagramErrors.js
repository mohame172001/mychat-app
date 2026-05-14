// Phase 2.18L: clear, actionable messages for every Instagram-connect
// failure mode the backend can return. Replaces the previous generic
// "server_error" / unknown-reason fallback that the user reported
// seeing as "Server Error".
//
// Every backend OAuth-callback redirect lands here via
//   /app/settings?ig=error&reason=<reason>
// so this is the single source of truth for what the user sees.

const MESSAGES = {
  // The owner-protection guard fired — the IG account is currently
  // linked to a DIFFERENT MyChat account with an active connection.
  instagram_account_already_connected:
    'This Instagram account is already linked to a different MyChat account. ' +
    'Sign in to that account and disconnect it first, or contact support if you ' +
    'own this Instagram account and lost access to the original MyChat account.',

  // Meta returned an OAuth error (most often the user clicked Cancel /
  // declined the permissions on Instagram).
  oauth_denied:
    'You declined the Instagram permissions, so we could not finish linking the account. ' +
    'Click Connect again and accept the requested permissions.',
  access_denied:
    'You declined the Instagram permissions. Click Connect and accept the requested permissions to continue.',

  // Our state cookie / signed payload was missing or invalid — usually
  // caused by opening the OAuth callback URL out of order or after a
  // long delay.
  invalid_state:
    'The Instagram sign-in link expired before you finished. Click Connect again to start a fresh link.',

  // Meta did not include the authorization code in the redirect.
  missing_code:
    'Instagram did not return an authorization code. Click Connect again — this almost always fixes it.',

  // We exchanged the code with api.instagram.com/oauth/access_token but
  // got a non-2xx back. Most often: the Instagram app credentials no
  // longer match, or Instagram revoked the previous grant.
  token_exchange_failed:
    'Instagram refused to issue an access token. Click Connect again, and make sure you log in with the SAME Instagram username you intended to link.',

  // We got a token but graph.instagram.com/me rejected it. Usually means
  // the account is not a Business / Creator account, or the granted
  // permissions did not include the required basic-profile scopes.
  token_cannot_call_graph_me:
    'Instagram gave us a token but rejected our profile lookup. The account must be a Business or Creator account with the requested permissions accepted.',

  // The catch-all 500 path inside the callback.
  server_error:
    'Something went wrong while finishing the Instagram link. Click Connect again. If it still fails, sign out, sign back in, and retry — and contact support if it persists.',
};

export const instagramErrorMessage = (reason) => {
  if (!reason) return 'Instagram connection failed. Please try again.';
  const exact = MESSAGES[reason];
  if (exact) return exact;
  // Meta-returned passthroughs like "access_denied" etc. — keep the
  // reason visible so support can debug, but in human-friendly framing.
  return `Instagram connection failed (${reason}). Click Connect again to retry.`;
};

export const instagramConnectExceptionMessage = (error, fallback = 'Failed') => {
  const detail = error?.response?.data?.detail;
  if (detail?.code) return instagramErrorMessage(detail.code);
  if (typeof detail === 'string') return instagramErrorMessage(detail);
  if (typeof detail?.message === 'string') return detail.message;
  return fallback;
};

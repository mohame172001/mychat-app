export const instagramErrorMessage = (reason) => {
  if (reason === 'instagram_account_already_connected') {
    return 'This Instagram account is already connected to another MyChat account. Contact support if you own this account.';
  }
  if (reason === 'token_cannot_call_graph_me') {
    return 'Instagram connected failed: token cannot call /me';
  }
  if (reason === 'missing_code') return 'Instagram connection failed: OAuth code was missing';
  if (reason === 'token_exchange_failed') return 'Instagram connection failed: token exchange failed';
  return `Instagram connection failed: ${reason || 'unknown'}`;
};

export const instagramConnectExceptionMessage = (error, fallback = 'Failed') => {
  const detail = error?.response?.data?.detail;
  if (detail?.code) return instagramErrorMessage(detail.code);
  if (typeof detail === 'string') return instagramErrorMessage(detail);
  return fallback;
};

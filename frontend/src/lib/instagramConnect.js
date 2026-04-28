import api from './api';

export const instagramConnectUrlPath = ({ mode = 'add_account', returnTo = '/app' } = {}) => {
  const params = new URLSearchParams({ mode, returnTo });
  return `/instagram/auth-url?${params.toString()}`;
};

export const startInstagramConnect = async ({ mode = 'add_account', returnTo = '/app' } = {}) => {
  const { data } = await api.get(instagramConnectUrlPath({ mode, returnTo }));
  if (!data?.url) throw new Error('Instagram OAuth URL was not returned');
  window.location.href = data.url;
  return data.url;
};

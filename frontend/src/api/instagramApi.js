import api from '../lib/api';

export const instagramApi = {
  listAccounts: () => api.get('/instagram/accounts'),
  activateAccount: (accountId) => api.post(`/instagram/accounts/${accountId}/activate`),
  authUrl: (path) => api.get(path),
  profile: (config) => api.get('/instagram/profile', config),
  media: (config) => api.get('/instagram/media', config),
  dmRules: () => api.get('/instagram/dm/rules'),
  dmLogs: (limit = 50) => api.get(`/instagram/dm/logs?limit=${limit}`),
  dmDiagnostics: () => api.get('/instagram/dm/diagnostics'),
  createDmRule: (body) => api.post('/instagram/dm/rules', body),
  updateDmRule: (ruleId, body) => api.patch(`/instagram/dm/rules/${ruleId}`, body),
  deleteDmRule: (ruleId) => api.delete(`/instagram/dm/rules/${ruleId}`),
};

import api from '../lib/api';

export const automationsApi = {
  summary: (config) => api.get('/automations/summary', config),
  get: (automationId, config) => api.get(`/automations/${automationId}`, config),
  update: (automationId, body) => api.patch(`/automations/${automationId}`, body),
  remove: (automationId) => api.delete(`/automations/${automationId}`),
  createQuickCommentRule: (body) => api.post('/automations/quick-comment-rule', body),
};

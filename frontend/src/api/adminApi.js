import api from '../lib/api';

export const adminApi = {
  toolsEnabled: () => api.get('/admin/tools-enabled'),
  overview: () => api.get('/admin/overview'),
  members: () => api.get('/admin/members'),
  users: (config) => api.get('/admin/users', config),
};

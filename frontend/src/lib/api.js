import axios from 'axios';
import { clearApiCache } from './apiCache';

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL;
export const API_BASE = `${BACKEND_URL}/api`;

const api = axios.create({ baseURL: API_BASE, timeout: 20000 });

api.interceptors.request.use((config) => {
  const token = localStorage.getItem('mychat_token');
  if (token) config.headers.Authorization = `Bearer ${token}`;
  config.metadata = { startTime: Date.now() };
  return config;
});

api.interceptors.response.use(
  (r) => {
    const start = r.config?.metadata?.startTime;
    if (start) {
      const duration = Date.now() - start;
      const backendTime = r.headers['x-response-time'];
      const isDashboardSummary = String(r.config.url || '').includes('/dashboard/summary');
      if (isDashboardSummary) {
        console.log(
          `[perf] api ${r.config.method?.toUpperCase()} ${r.config.url} ` +
          `client=${duration}ms backend=${backendTime || '?'}ms ` +
          `dashboard=${r.headers['x-dashboard-summary-time'] || '?'}ms ` +
          `source=${r.headers['x-dashboard-summary-source'] || '?'} ` +
          `slowest=${r.headers['x-dashboard-summary-slowest'] || '?'} ` +
          `status=${r.status}`
        );
      } else if (duration > 2000 || (backendTime && Number(backendTime) > 3000)) {
        console.log(
          `[api] SLOW ${r.config.method?.toUpperCase()} ${r.config.url} ` +
          `client=${duration}ms backend=${backendTime || '?'}ms status=${r.status}`
        );
      }
    }
    return r;
  },
  (err) => {
    const start = err?.config?.metadata?.startTime;
    const duration = start ? Date.now() - start : 0;
    const resp = err?.response;
    const status = resp?.status || 'network';
    const backendTime = resp?.headers?.['x-response-time'];
    if (duration > 1000 || status === 'network') {
      console.warn(
        `[api] FAIL ${err?.config?.method?.toUpperCase()} ${err?.config?.url} ` +
        `client=${duration}ms backend=${backendTime || '?'}s status=${status}` +
        (resp?.data?.detail ? ` body=${String(resp.data.detail).slice(0, 100)}` : '')
      );
    }
    if (status === 401) {
      localStorage.removeItem('mychat_token');
      localStorage.removeItem('mychat_user');
      clearApiCache();
      if (window.location.pathname.startsWith('/app')) window.location.href = '/login';
    }
    return Promise.reject(err);
  }
);

export default api;

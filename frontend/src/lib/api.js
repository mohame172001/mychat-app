import axios from 'axios';
import { clearApiCache } from './apiCache';

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL;
export const API_BASE = `${BACKEND_URL}/api`;

const api = axios.create({ baseURL: API_BASE, timeout: 20000 });

// Phase 2.18Y cold-start: fire a no-DB warmup ping at module-load time
// so the Railway container is already awake by the time the first
// authenticated call (auth/me, auth/bootstrap, dashboard/summary)
// arrives. Worst case it costs one round-trip; best case (cold
// container) it parallelizes the cold-start spin-up with React boot
// instead of stacking them serially behind /auth/me. Errors are
// intentionally swallowed — this is a best-effort hint.
if (typeof window !== 'undefined' && BACKEND_URL) {
  try {
    fetch(`${API_BASE}/health`, { method: 'GET', cache: 'no-store', credentials: 'omit' })
      .catch(() => {});
  } catch (_) { /* ignore */ }
}

api.interceptors.request.use((config) => {
  const token = localStorage.getItem('mychat_token');
  if (token) config.headers.Authorization = `Bearer ${token}`;
  config.metadata = { startTime: Date.now() };
  return config;
});

// Phase 2.19: console noise hurts perceived professionalism when
// power-users open DevTools on a live site. Gate the verbose perf
// chatter behind explicit opt-in (?debug=1 in the URL or
// localStorage.mychat_debug=1) so production stays silent by default
// while ops/staff can still flip it on when needed.
const _devLogsEnabled = () => {
  try {
    if (typeof window !== 'undefined') {
      const params = new URLSearchParams(window.location.search);
      if (params.get('debug') === '1') return true;
    }
    if (typeof localStorage !== 'undefined' && localStorage.getItem('mychat_debug') === '1') return true;
  } catch (_) { /* ignore */ }
  return process.env.NODE_ENV !== 'production';
};

api.interceptors.response.use(
  (r) => {
    const start = r.config?.metadata?.startTime;
    if (start && _devLogsEnabled()) {
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
    if ((duration > 1000 || status === 'network') && _devLogsEnabled()) {
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

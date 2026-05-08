/**
 * Phase 2.4 — pure-logic helpers for the Admin Console UI.
 *
 * Kept in /lib so they can be unit-tested without React Testing Library.
 *
 * Backend endpoints (all admin-only via ADMIN_EMAILS):
 *   GET  /api/admin/me                          { is_admin, email }
 *   GET  /api/admin/overview                    aggregates
 *   GET  /api/admin/users?...                   paginated user list
 *   GET  /api/admin/users/{user_id}/detail      sanitized profile
 *   POST /api/admin/users/{user_id}/plan        plan assignment
 *   POST /api/admin/automations/{id}/disable    pause + audit
 *   GET  /api/admin/audit-log                   recent admin actions
 */

export const PLAN_KEYS = Object.freeze(['free', 'starter', 'pro', 'business']);

export const PLAN_DISPLAY = Object.freeze({
  free: 'Free',
  starter: 'Starter',
  pro: 'Pro',
  business: 'Business',
});

/**
 * Decide whether the user has any limit exceeded for the row badge in
 * the users table. `exceeded` is the dict returned by the backend.
 */
export function hasAnyExceeded(exceeded) {
  if (!exceeded || typeof exceeded !== 'object') return false;
  return Object.values(exceeded).some((v) => v === true);
}

/**
 * Pretty-print plan_distribution as an ordered array, in plan tier order,
 * so the admin overview always shows free → starter → pro → business.
 */
export function planDistributionRows(distribution) {
  const dist = distribution && typeof distribution === 'object' ? distribution : {};
  return PLAN_KEYS.map((key) => ({
    key,
    label: PLAN_DISPLAY[key],
    count: Number(dist[key] || 0),
  }));
}

/**
 * Fields returned by /api/admin/users/{id}/detail that we explicitly
 * REFUSE to render (or expect to be absent). Used in a contract test
 * to lock the privacy boundary.
 */
export const FORBIDDEN_DETAIL_FIELDS = Object.freeze([
  'access_token',
  'meta_access_token',
  'authorization',
  'comment_text',
  'reply_text',
  'dm_text',
  'message_text',
  'raw',
  'graph_error',
]);

/**
 * Returns true if a backend response payload contains any forbidden raw
 * field at the top level OR inside any first-level child object.
 */
export function containsForbiddenField(payload) {
  if (!payload || typeof payload !== 'object') return false;
  const lower = (s) => String(s || '').toLowerCase();
  const forbidden = new Set(FORBIDDEN_DETAIL_FIELDS.map(lower));
  const check = (obj) => {
    if (!obj || typeof obj !== 'object') return false;
    for (const key of Object.keys(obj)) {
      if (forbidden.has(lower(key))) return true;
    }
    return false;
  };
  if (check(payload)) return true;
  for (const value of Object.values(payload)) {
    if (Array.isArray(value)) {
      for (const item of value) {
        if (check(item)) return true;
      }
    } else if (value && typeof value === 'object') {
      if (check(value)) return true;
    }
  }
  return false;
}

/**
 * Format an ISO timestamp safely. Returns '—' when null/missing/invalid.
 */
export function formatTimestamp(value) {
  if (!value) return '—';
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) return String(value);
  return d.toLocaleString();
}

/**
 * Build the plan options array for an HTML <select> in the assignment UI.
 */
export function planOptions() {
  return PLAN_KEYS.map((key) => ({ value: key, label: PLAN_DISPLAY[key] }));
}

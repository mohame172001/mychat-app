/**
 * Phase 2.3 — pure-logic helpers for the Usage / Billing placeholder page.
 *
 * Backend shape (from /api/plan/current and /api/usage/current):
 *   {
 *     plan_key, display_name, billing_enabled: false,
 *     limits: { monthly_*_limit: N | null },
 *     counters: { comments_processed, public_replies_sent, dms_sent,
 *                 links_clicked, queue_jobs_processed, ... },
 *     remaining: { monthly_*_limit: N | null },
 *     exceeded:  { monthly_*_limit: bool },
 *     max_instagram_accounts, max_active_automations,
 *     connectedInstagramAccountsCount, activeAutomationsCount,
 *     event_month
 *   }
 *
 * billing_enabled is always false in the placeholder phase. There is no
 * Stripe integration.
 */

export const NEAR_LIMIT_THRESHOLD = 0.8;

/** ---- Counter rows ------------------------------------------------------- */

// (label, counter field on monthly_usage, limit field on plan)
export const USAGE_ROWS = Object.freeze([
  {
    key: 'comments_processed',
    label: 'Comments processed',
    counter: 'comments_processed',
    limitKey: 'monthly_comments_processed_limit',
  },
  {
    key: 'public_replies_sent',
    label: 'Public replies sent',
    counter: 'public_replies_sent',
    limitKey: 'monthly_public_replies_sent_limit',
  },
  {
    key: 'dms_sent',
    label: 'DMs sent',
    counter: 'dms_sent',
    limitKey: 'monthly_dms_sent_limit',
  },
  {
    key: 'links_clicked',
    label: 'Link clicks',
    counter: 'links_clicked',
    limitKey: 'monthly_links_clicked_limit',
  },
  {
    key: 'queue_jobs_processed',
    label: 'Queue jobs processed',
    counter: 'queue_jobs_processed',
    limitKey: 'queue_jobs_processed_limit',
  },
]);

/** ---- Per-row computation ------------------------------------------------ */

/**
 * Given a usage payload from /api/usage/current and a row descriptor,
 * compute { used, limit, remaining, percent, status } where:
 *   - status is one of 'unlimited' | 'normal' | 'near_limit' | 'exceeded'
 *   - percent is 0..100, or null when unlimited
 *
 * Safe against missing/null limits (renders as 'unlimited').
 */
export function computeUsageRow(payload, row) {
  const counters = (payload && payload.counters) || {};
  const limits = (payload && payload.limits) || {};
  const remaining = (payload && payload.remaining) || {};
  const exceeded = (payload && payload.exceeded) || {};

  const used = Number(counters[row.counter] || 0);
  const rawLimit = limits[row.limitKey];
  const limit = rawLimit === null || rawLimit === undefined ? null : Number(rawLimit);
  const rem = remaining[row.limitKey];

  if (limit === null || Number.isNaN(limit)) {
    return {
      key: row.key,
      label: row.label,
      used,
      limit: null,
      remaining: null,
      percent: null,
      status: 'unlimited',
    };
  }

  const remainingValue = rem === null || rem === undefined ? Math.max(0, limit - used) : Number(rem);
  const percent = limit > 0 ? Math.min(100, Math.round((used / limit) * 100)) : 0;
  const isExceeded = exceeded[row.limitKey] === true || used >= limit;

  let status;
  if (isExceeded) status = 'exceeded';
  else if (percent >= Math.round(NEAR_LIMIT_THRESHOLD * 100)) status = 'near_limit';
  else status = 'normal';

  return {
    key: row.key,
    label: row.label,
    used,
    limit,
    remaining: remainingValue,
    percent,
    status,
  };
}

/**
 * Convenience: compute all rows for a payload. Filters out rows whose
 * counter is undefined AND whose limit is null (e.g. queue_jobs_processed
 * when the plan has no cap and no usage yet) so the UI stays clean.
 */
export function computeAllUsageRows(payload) {
  return USAGE_ROWS.map(row => computeUsageRow(payload, row)).filter(row => {
    if (row.status === 'unlimited' && row.used === 0) return false;
    return true;
  });
}

/** ---- Account / automation snapshot rows -------------------------------- */

export function computeAccountRow(payload) {
  const used = Number((payload && payload.connectedInstagramAccountsCount) || 0);
  const limit = (payload && payload.max_instagram_accounts) ?? null;
  return _computeSnapshotRow('Connected Instagram accounts', used, limit);
}

export function computeAutomationRow(payload) {
  const used = Number((payload && payload.activeAutomationsCount) || 0);
  const limit = (payload && payload.max_active_automations) ?? null;
  return _computeSnapshotRow('Active automations', used, limit);
}

function _computeSnapshotRow(label, used, limit) {
  if (limit === null || limit === undefined) {
    return { label, used, limit: null, remaining: null, percent: null, status: 'unlimited' };
  }
  const lim = Number(limit);
  const remaining = Math.max(0, lim - used);
  const percent = lim > 0 ? Math.min(100, Math.round((used / lim) * 100)) : 0;
  let status = 'normal';
  if (used >= lim) status = 'exceeded';
  else if (percent >= Math.round(NEAR_LIMIT_THRESHOLD * 100)) status = 'near_limit';
  return { label, used, limit: lim, remaining, percent, status };
}

/** ---- Plan card helpers -------------------------------------------------- */

/**
 * Decide whether a plan card should be highlighted as the current plan.
 * Falls back to 'free' when the user payload omits a plan_key.
 */
export function isCurrentPlan(planCard, currentPayload) {
  const current = (currentPayload && currentPayload.plan_key) || 'free';
  return Boolean(planCard && planCard.plan_key === current);
}

/**
 * Status pill color helper. Centralized so tests lock the contract.
 */
export function statusToTone(status) {
  switch (status) {
    case 'exceeded':
      return 'rose';
    case 'near_limit':
      return 'amber';
    case 'unlimited':
      return 'slate';
    case 'normal':
    default:
      return 'emerald';
  }
}

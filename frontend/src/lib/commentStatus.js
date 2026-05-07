/**
 * Pure-logic helpers for the Comments page status filtering.
 *
 * Extracted from pages/Comments.jsx so tests can import them without
 * pulling axios / sonner / browser-only deps. Also imported by the
 * page itself.
 */

export const STATUS_FILTERS = Object.freeze([
  { key: 'all',          label: 'All' },
  { key: 'pending',      label: 'Pending / queued' },
  { key: 'retryable',    label: 'Retryable failed' },
  { key: 'permanent',    label: 'Permanent failed' },
  { key: 'partial',      label: 'Partial (DM failed)' },
  { key: 'plan_limited', label: 'Plan limited' },
  { key: 'success',      label: 'Success' },
  { key: 'skipped',      label: 'Skipped' },
]);

export const PERMANENT_FAILURE_REASONS = new Set([
  'recipient_unavailable',
  'messaging_not_allowed',
  'user_blocked_messages',
  'permission_error',
]);

export function normalizeStatus(comment) {
  const action = String(comment?.action_status || comment?.actionStatus || '').toLowerCase();
  const reply = String(comment?.reply_status || comment?.replyStatus || '').toLowerCase();
  const dm = String(comment?.dm_status || comment?.dmStatus || '').toLowerCase();
  const skip = String(comment?.skip_reason || comment?.skipReason || '').toLowerCase();

  if (action === 'partial_success' || (reply === 'success' && dm === 'failed')) return 'partial';
  if (
    action === 'plan_limited'
    || reply === 'plan_limited'
    || dm === 'plan_limited'
    || skip === 'skipped_plan_limit'
  ) return 'plan_limited';
  if (action === 'pending' || action === 'processing' || reply === 'pending' || dm === 'pending') return 'pending';
  if (
    action === 'failed_retryable'
    || comment?.reply_failure_retryable
    || comment?.dm_failure_retryable
  ) return 'retryable';
  if (action === 'failed_permanent' || action === 'failed_retry_exhausted') return 'permanent';
  if (action === 'skipped' || action === 'skipped_ineligible' || skip) return 'skipped';
  if (action === 'success' || reply === 'success' || comment?.replied) return 'success';
  return 'pending';
}

export function statusLabel(status) {
  return STATUS_FILTERS.find(item => item.key === status)?.label || status;
}

export function canRetryReply(c) {
  if (c?.reply_provider_response_ok === true) return false;
  if (String(c?.reply_status || '').toLowerCase() === 'success' && c?.replied === true) return false;
  if (PERMANENT_FAILURE_REASONS.has(c?.reply_failure_reason)) return false;
  return true;
}

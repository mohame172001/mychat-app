/**
 * Phase 1.4H: pure logic helpers for the specific-reply admin debug page.
 *
 * Kept in /lib (not /pages) so they can be unit-tested without any React
 * Testing Library dependency.
 */

/**
 * Decide PASS/FAIL for a diagnosis payload returned by
 * GET /api/admin/comments/{ig_comment_id}/specific-reply-diagnosis.
 *
 * PASS criteria (all must hold):
 *   - public_reply_required === true
 *   - reply_status === 'success'
 *   - reply_provider_response_ok === true
 *   - if dm_required is true, dm_status === 'success'
 *   - forbidden_state_detected === false
 *
 * FAIL is the negation. The most common forbidden state from the
 * production bug — public_reply_required=true + reply_status='disabled' +
 * dm_status='success' — is captured by forbidden_state_detected.
 */
export function diagnosisPassFail(diag) {
  if (!diag || typeof diag !== 'object') {
    return { pass: false, reason: 'no_data' };
  }
  const reply = String(diag.reply_status || '').toLowerCase();
  const dm = String(diag.dm_status || '').toLowerCase();

  if (diag.forbidden_state_detected === true) {
    return { pass: false, reason: 'forbidden_state_detected' };
  }
  if (diag.public_reply_required !== true) {
    // Truly DM-only rules are valid as PASS only when DM succeeded.
    if (diag.dm_required && dm !== 'success') {
      return { pass: false, reason: 'dm_required_but_dm_not_success' };
    }
    return { pass: true, reason: 'dm_only_rule_passed' };
  }
  if (reply !== 'success') {
    return { pass: false, reason: `reply_status_not_success:${reply || 'empty'}` };
  }
  if (diag.reply_provider_response_ok !== true) {
    return { pass: false, reason: 'reply_provider_response_ok_false' };
  }
  if (diag.dm_required && dm !== 'success') {
    return { pass: false, reason: 'dm_required_but_dm_not_success' };
  }
  return { pass: true, reason: 'all_checks_passed' };
}

/**
 * Whitelist of fields the debug UI is allowed to render.
 * Anything outside this list could leak raw text — refuse to render it.
 *
 * Kept here so tests can lock the contract: every key in this list is
 * sanitized (lengths/hashes/booleans/ids/timestamps) and no raw comment,
 * reply, or DM text key is allowed.
 */
export const SAFE_DIAGNOSIS_FIELDS = Object.freeze([
  'comment_id',
  'ig_comment_id',
  'media_id',
  'user_id',
  'instagram_account_id',
  'automation_id',
  'matched_rule_id',
  'matched_rule_scope',
  'reply_status',
  'dm_status',
  'action_status',
  'reply_attempted_at',
  'replied_at',
  'reply_provider_response_ok',
  'reply_provider_comment_id_exists',
  'reply_skip_reason',
  'dm_attempted_at',
  'finalDmSentAt',
  'next_retry_at',
  'attempts',
  'queue_lock_until',
  'public_reply_required',
  'public_reply_source',
  'public_reply_text_length',
  'public_reply_text_hash',
  'dm_required',
  'dm_text_length',
  'dm_text_hash',
  'repairable',
  'repair_reason',
  'forbidden_state_detected',
]);

const FORBIDDEN_RAW_TEXT_FIELDS = Object.freeze([
  'public_reply_text',
  'comment_text',
  'comment_reply',
  'comment_reply_2',
  'comment_reply_3',
  'dm_text',
  'reply_text',
  'access_token',
  'meta_access_token',
  'authorization',
]);

/**
 * Returns true if the diagnosis payload contains any forbidden raw-text
 * field. Used in tests to assert that the safe-fields contract holds.
 */
export function diagnosisLeaksRawText(diag) {
  if (!diag || typeof diag !== 'object') return false;
  for (const key of Object.keys(diag)) {
    if (FORBIDDEN_RAW_TEXT_FIELDS.includes(key.toLowerCase())) return true;
  }
  return false;
}

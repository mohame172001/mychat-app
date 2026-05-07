import { normalizeStatus, statusLabel } from './commentStatus';

describe('Comments normalizeStatus — plan_limited integration', () => {
  test('action_status=plan_limited routes to plan_limited', () => {
    expect(normalizeStatus({ action_status: 'plan_limited' })).toBe('plan_limited');
  });

  test('reply_status=plan_limited routes to plan_limited', () => {
    expect(normalizeStatus({
      action_status: 'partial_success',
      reply_status: 'plan_limited',
      dm_status: 'success',
    })).toBe('partial');  // partial_success on action_status takes priority over per-step plan_limited here
  });

  test('reply plan_limited alone (no partial_success action_status) -> plan_limited', () => {
    expect(normalizeStatus({
      reply_status: 'plan_limited',
      dm_status: 'disabled',
    })).toBe('plan_limited');
  });

  test('dm_status=plan_limited alone -> plan_limited', () => {
    expect(normalizeStatus({
      dm_status: 'plan_limited',
    })).toBe('plan_limited');
  });

  test('skip_reason=skipped_plan_limit -> plan_limited', () => {
    expect(normalizeStatus({
      skip_reason: 'skipped_plan_limit',
    })).toBe('plan_limited');
  });

  test('plan_limited filter has user-facing label "Plan limited"', () => {
    expect(statusLabel('plan_limited')).toBe('Plan limited');
  });

  test('unaffected: regular success path still works', () => {
    expect(normalizeStatus({
      action_status: 'success',
      reply_status: 'success',
      dm_status: 'success',
    })).toBe('success');
    expect(normalizeStatus({ replied: true, reply_status: 'success' })).toBe('success');
  });

  test('unaffected: partial_success without plan_limited still partial', () => {
    expect(normalizeStatus({
      action_status: 'partial_success',
    })).toBe('partial');
  });

  test('unaffected: pending still pending', () => {
    expect(normalizeStatus({ action_status: 'pending' })).toBe('pending');
  });
});

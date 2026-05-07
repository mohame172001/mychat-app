import {
  diagnosisPassFail,
  SAFE_DIAGNOSIS_FIELDS,
  diagnosisLeaksRawText,
} from './specificReplyDiagnosis';

const fullPass = {
  public_reply_required: true,
  reply_status: 'success',
  reply_provider_response_ok: true,
  dm_required: true,
  dm_status: 'success',
  forbidden_state_detected: false,
};

describe('diagnosisPassFail', () => {
  test('full success returns PASS with reply_and_dm_succeeded reason', () => {
    const r = diagnosisPassFail(fullPass);
    expect(r.pass).toBe(true);
    expect(r.reason).toBe('reply_and_dm_succeeded');
    expect(r.label).toMatch(/Reply \+ DM/);
  });

  test('DM-only rule with DM success is consistent (not a misleading PASS)', () => {
    const r = diagnosisPassFail({
      public_reply_required: false,
      reply_status: 'disabled',
      dm_required: true,
      dm_status: 'success',
      forbidden_state_detected: false,
    });
    expect(r.pass).toBe(true);
    expect(r.reason).toBe('dm_only_rule_consistent');
    // Label must clearly state this is DM-only, not "all green".
    expect(r.label).toMatch(/DM-only/);
  });

  test('forbidden state (disabled+success+required) is FAIL — production bug case', () => {
    const r = diagnosisPassFail({
      public_reply_required: true,
      reply_status: 'disabled',
      dm_status: 'success',
      dm_required: true,
      forbidden_state_detected: true,
    });
    expect(r.pass).toBe(false);
    expect(r.reason).toBe('forbidden_state_detected');
  });

  test('reply_status not success is FAIL', () => {
    const r = diagnosisPassFail({
      ...fullPass,
      reply_status: 'failed_retryable',
      forbidden_state_detected: false,
    });
    expect(r.pass).toBe(false);
    expect(r.reason).toContain('reply_status_not_success');
  });

  test('reply success but no provider proof is FAIL', () => {
    const r = diagnosisPassFail({
      ...fullPass,
      reply_provider_response_ok: false,
    });
    expect(r.pass).toBe(false);
    expect(r.reason).toBe('reply_provider_response_ok_false');
  });

  test('dm required but failed is FAIL', () => {
    const r = diagnosisPassFail({
      ...fullPass,
      dm_status: 'failed',
    });
    expect(r.pass).toBe(false);
    expect(r.reason).toBe('dm_required_but_dm_not_success');
  });

  test('null payload is FAIL with reason no_data', () => {
    const r = diagnosisPassFail(null);
    expect(r.pass).toBe(false);
    expect(r.reason).toBe('no_data');
    expect(diagnosisPassFail(undefined).reason).toBe('no_data');
  });
});

describe('safe-fields contract', () => {
  test('SAFE_DIAGNOSIS_FIELDS has the canonical fields', () => {
    expect(SAFE_DIAGNOSIS_FIELDS).toContain('public_reply_text_length');
    expect(SAFE_DIAGNOSIS_FIELDS).toContain('public_reply_text_hash');
    expect(SAFE_DIAGNOSIS_FIELDS).toContain('forbidden_state_detected');
    expect(SAFE_DIAGNOSIS_FIELDS).toContain('repairable');
    expect(SAFE_DIAGNOSIS_FIELDS).toContain('reply_provider_response_ok');
    // No raw text field is in the safe list.
    expect(SAFE_DIAGNOSIS_FIELDS).not.toContain('public_reply_text');
    expect(SAFE_DIAGNOSIS_FIELDS).not.toContain('comment_text');
    expect(SAFE_DIAGNOSIS_FIELDS).not.toContain('dm_text');
    expect(SAFE_DIAGNOSIS_FIELDS).not.toContain('access_token');
  });

  test('diagnosisLeaksRawText catches forbidden keys', () => {
    expect(diagnosisLeaksRawText({ public_reply_text_length: 5 })).toBe(false);
    expect(diagnosisLeaksRawText({ public_reply_text: 'oops' })).toBe(true);
    expect(diagnosisLeaksRawText({ comment_text: 'oops' })).toBe(true);
    expect(diagnosisLeaksRawText({ dm_text: 'oops' })).toBe(true);
    expect(diagnosisLeaksRawText({ access_token: 'oops' })).toBe(true);
    expect(diagnosisLeaksRawText({ Authorization: 'oops' })).toBe(true);
    expect(diagnosisLeaksRawText(null)).toBe(false);
  });
});

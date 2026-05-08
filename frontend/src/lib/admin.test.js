import {
  PLAN_KEYS,
  PLAN_DISPLAY,
  hasAnyExceeded,
  planDistributionRows,
  FORBIDDEN_DETAIL_FIELDS,
  containsForbiddenField,
  formatTimestamp,
  planOptions,
} from './admin';

describe('admin helpers — plan vocabulary', () => {
  test('PLAN_KEYS in expected order', () => {
    expect(PLAN_KEYS).toEqual(['free', 'starter', 'pro', 'business']);
  });

  test('PLAN_DISPLAY covers every key', () => {
    for (const k of PLAN_KEYS) expect(PLAN_DISPLAY[k]).toBeTruthy();
  });

  test('planOptions builds value/label pairs', () => {
    const opts = planOptions();
    expect(opts.map(o => o.value)).toEqual(['free', 'starter', 'pro', 'business']);
    expect(opts[0].label).toBe('Free');
    expect(opts[3].label).toBe('Business');
  });
});

describe('hasAnyExceeded', () => {
  test('returns true when any flag is true', () => {
    expect(hasAnyExceeded({ a: false, b: true })).toBe(true);
  });
  test('returns false when no flag is true', () => {
    expect(hasAnyExceeded({ a: false, b: false })).toBe(false);
  });
  test('safe on null/empty', () => {
    expect(hasAnyExceeded(null)).toBe(false);
    expect(hasAnyExceeded(undefined)).toBe(false);
    expect(hasAnyExceeded({})).toBe(false);
  });
});

describe('planDistributionRows', () => {
  test('orders rows free -> business and fills missing with 0', () => {
    const rows = planDistributionRows({ pro: 3, free: 5 });
    expect(rows.map(r => r.key)).toEqual(['free', 'starter', 'pro', 'business']);
    expect(rows.find(r => r.key === 'free').count).toBe(5);
    expect(rows.find(r => r.key === 'pro').count).toBe(3);
    expect(rows.find(r => r.key === 'starter').count).toBe(0);
    expect(rows.find(r => r.key === 'business').count).toBe(0);
  });
  test('safe on missing payload', () => {
    const rows = planDistributionRows(null);
    expect(rows.length).toBe(4);
    expect(rows.every(r => r.count === 0)).toBe(true);
  });
});

describe('FORBIDDEN_DETAIL_FIELDS / containsForbiddenField', () => {
  test('forbidden list does not change without a deliberate update', () => {
    expect(FORBIDDEN_DETAIL_FIELDS).toContain('access_token');
    expect(FORBIDDEN_DETAIL_FIELDS).toContain('meta_access_token');
    expect(FORBIDDEN_DETAIL_FIELDS).toContain('authorization');
    expect(FORBIDDEN_DETAIL_FIELDS).toContain('comment_text');
    expect(FORBIDDEN_DETAIL_FIELDS).toContain('reply_text');
    expect(FORBIDDEN_DETAIL_FIELDS).toContain('dm_text');
  });

  test('detects forbidden field at top level', () => {
    expect(containsForbiddenField({ access_token: 'X' })).toBe(true);
    expect(containsForbiddenField({ Authorization: 'Bearer Y' })).toBe(true);
  });

  test('detects forbidden field one level deep', () => {
    expect(containsForbiddenField({
      profile: { email: 'a@b' },
      instagram_accounts: [{ access_token: 'X' }],
    })).toBe(true);
  });

  test('clean payload returns false', () => {
    expect(containsForbiddenField({
      profile: { email: 'a@b' },
      plan: { plan_key: 'pro' },
      instagram_accounts: [{ username: 'handle', connectionValid: true }],
    })).toBe(false);
  });

  test('null/undefined safe', () => {
    expect(containsForbiddenField(null)).toBe(false);
    expect(containsForbiddenField(undefined)).toBe(false);
  });
});

describe('formatTimestamp', () => {
  test('handles null gracefully', () => {
    expect(formatTimestamp(null)).toBe('—');
    expect(formatTimestamp(undefined)).toBe('—');
    expect(formatTimestamp('')).toBe('—');
  });
  test('formats valid ISO string', () => {
    const result = formatTimestamp('2026-05-08T12:00:00Z');
    expect(result).toBeTruthy();
    expect(result).not.toBe('—');
  });
  test('returns input on invalid date', () => {
    expect(formatTimestamp('not-a-date')).toBe('not-a-date');
  });
});

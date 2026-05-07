import {
  computeUsageRow,
  computeAllUsageRows,
  computeAccountRow,
  computeAutomationRow,
  isCurrentPlan,
  statusToTone,
  USAGE_ROWS,
  NEAR_LIMIT_THRESHOLD,
} from './usage';

const baseFreePayload = {
  plan_key: 'free',
  display_name: 'Free',
  billing_enabled: false,
  event_month: '2026-05',
  limits: {
    monthly_comments_processed_limit: 250,
    monthly_public_replies_sent_limit: 100,
    monthly_dms_sent_limit: 100,
    monthly_links_clicked_limit: 100,
    queue_jobs_processed_limit: null,
  },
  counters: {
    comments_processed: 0,
    public_replies_sent: 0,
    dms_sent: 0,
    links_clicked: 0,
    queue_jobs_processed: 0,
  },
  remaining: {
    monthly_comments_processed_limit: 250,
    monthly_public_replies_sent_limit: 100,
    monthly_dms_sent_limit: 100,
    monthly_links_clicked_limit: 100,
    queue_jobs_processed_limit: null,
  },
  exceeded: {
    monthly_comments_processed_limit: false,
    monthly_public_replies_sent_limit: false,
    monthly_dms_sent_limit: false,
    monthly_links_clicked_limit: false,
    queue_jobs_processed_limit: false,
  },
  max_instagram_accounts: 1,
  max_active_automations: 2,
  connectedInstagramAccountsCount: 0,
  activeAutomationsCount: 0,
};

const dmsRow = USAGE_ROWS.find(r => r.key === 'dms_sent');

describe('computeUsageRow', () => {
  test('normal status when used is small', () => {
    const row = computeUsageRow({
      ...baseFreePayload,
      counters: { ...baseFreePayload.counters, dms_sent: 10 },
    }, dmsRow);
    expect(row.used).toBe(10);
    expect(row.limit).toBe(100);
    expect(row.percent).toBe(10);
    expect(row.status).toBe('normal');
  });

  test('near_limit at 80% used', () => {
    const row = computeUsageRow({
      ...baseFreePayload,
      counters: { ...baseFreePayload.counters, dms_sent: 80 },
    }, dmsRow);
    expect(row.percent).toBe(80);
    expect(row.status).toBe('near_limit');
  });

  test('near_limit at 95% used', () => {
    const row = computeUsageRow({
      ...baseFreePayload,
      counters: { ...baseFreePayload.counters, dms_sent: 95 },
    }, dmsRow);
    expect(row.status).toBe('near_limit');
  });

  test('exceeded at limit', () => {
    const row = computeUsageRow({
      ...baseFreePayload,
      counters: { ...baseFreePayload.counters, dms_sent: 100 },
      remaining: { ...baseFreePayload.remaining, monthly_dms_sent_limit: 0 },
      exceeded: { ...baseFreePayload.exceeded, monthly_dms_sent_limit: true },
    }, dmsRow);
    expect(row.status).toBe('exceeded');
    expect(row.remaining).toBe(0);
  });

  test('exceeded over limit', () => {
    const row = computeUsageRow({
      ...baseFreePayload,
      counters: { ...baseFreePayload.counters, dms_sent: 250 },
    }, dmsRow);
    expect(row.status).toBe('exceeded');
  });

  test('unlimited limit renders safely', () => {
    const queueRow = USAGE_ROWS.find(r => r.key === 'queue_jobs_processed');
    const row = computeUsageRow({
      ...baseFreePayload,
      counters: { ...baseFreePayload.counters, queue_jobs_processed: 42 },
    }, queueRow);
    expect(row.status).toBe('unlimited');
    expect(row.limit).toBeNull();
    expect(row.percent).toBeNull();
  });

  test('missing payload does not crash', () => {
    expect(() => computeUsageRow(null, dmsRow)).not.toThrow();
    expect(() => computeUsageRow(undefined, dmsRow)).not.toThrow();
    expect(() => computeUsageRow({}, dmsRow)).not.toThrow();
  });

  test('NEAR_LIMIT_THRESHOLD is 0.8', () => {
    expect(NEAR_LIMIT_THRESHOLD).toBe(0.8);
  });
});

describe('computeAllUsageRows', () => {
  test('drops unlimited zero-used rows but keeps zero-used capped rows', () => {
    const rows = computeAllUsageRows(baseFreePayload);
    const keys = rows.map(r => r.key);
    // queue_jobs_processed is unlimited + 0 used -> filtered out.
    expect(keys).not.toContain('queue_jobs_processed');
    // Capped rows always shown even at 0.
    expect(keys).toContain('comments_processed');
    expect(keys).toContain('dms_sent');
  });
});

describe('computeAccountRow / computeAutomationRow', () => {
  test('account row shows used/limit/percent', () => {
    const row = computeAccountRow({
      ...baseFreePayload,
      connectedInstagramAccountsCount: 1,
      max_instagram_accounts: 1,
    });
    expect(row.used).toBe(1);
    expect(row.limit).toBe(1);
    expect(row.status).toBe('exceeded');  // at cap counts as exceeded
  });

  test('automation row at near_limit', () => {
    const row = computeAutomationRow({
      ...baseFreePayload,
      activeAutomationsCount: 8,
      max_active_automations: 10,
    });
    expect(row.percent).toBe(80);
    expect(row.status).toBe('near_limit');
  });

  test('null limit -> unlimited', () => {
    const row = computeAccountRow({
      ...baseFreePayload,
      connectedInstagramAccountsCount: 5,
      max_instagram_accounts: null,
    });
    expect(row.status).toBe('unlimited');
  });
});

describe('isCurrentPlan / statusToTone', () => {
  test('current plan highlighting', () => {
    expect(isCurrentPlan({ plan_key: 'free' }, { plan_key: 'free' })).toBe(true);
    expect(isCurrentPlan({ plan_key: 'pro' }, { plan_key: 'free' })).toBe(false);
    expect(isCurrentPlan({ plan_key: 'free' }, {})).toBe(true);  // defaults to free
    expect(isCurrentPlan(null, { plan_key: 'free' })).toBe(false);
  });

  test('statusToTone maps to expected tone names', () => {
    expect(statusToTone('exceeded')).toBe('rose');
    expect(statusToTone('near_limit')).toBe('amber');
    expect(statusToTone('unlimited')).toBe('slate');
    expect(statusToTone('normal')).toBe('emerald');
    expect(statusToTone('garbage')).toBe('emerald');
  });
});

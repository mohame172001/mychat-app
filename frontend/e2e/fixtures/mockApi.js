const API_ORIGIN = 'http://127.0.0.1:4010';

const permissions = [
  'admin.overview.view',
  'admin.users.view',
  'admin.users.manage',
  'admin.plans.assign',
  'admin.automations.disable',
  'admin.failures.view',
  'admin.audit.view',
  'admin.members.view',
  'admin.members.manage',
  'admin.owner.manage',
];

const user = {
  id: 'u_owner',
  username: 'owner',
  name: 'Owner',
  email: 'owner@example.com',
  avatar: 'https://example.com/avatar.png',
  instagramConnected: false,
  instagramConnectionValid: false,
  instagramHandle: '@owner',
};

const weeklyPerformance = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map((day, index) => ({
  day,
  date: `2026-05-${String(index + 1).padStart(2, '0')}`,
  messages: index + 1,
  conversions: index % 2,
}));

function json(body, status = 200) {
  return {
    status,
    contentType: 'application/json',
    body: JSON.stringify(body),
  };
}

function commentRows() {
  return [
    {
      id: 'c_retry',
      ig_comment_id: 'ig_c_retry',
      commenter_username: 'tester',
      text: 'Sample retryable comment',
      created: '2026-05-09T10:00:00Z',
      media_id: 'media_1',
      replied: false,
      reply_status: 'failed_retryable',
      action_status: 'failed_retryable',
      attempts: 2,
    },
    {
      id: 'c_success',
      ig_comment_id: 'ig_c_success',
      commenter_username: 'customer',
      text: 'Sample success comment',
      created: '2026-05-09T11:00:00Z',
      media_id: 'media_1',
      replied: true,
      reply_status: 'success',
      action_status: 'success',
      reply_provider_response_ok: true,
      reply_text: 'Thanks',
      attempts: 1,
    },
  ];
}

function currentPlan() {
  return {
    event_month: '2026-05',
    plan_key: 'free',
    display_name: 'Free',
    billing_enabled: false,
    counters: {
      comments_processed: 12,
      public_replies_sent: 8,
      dms_sent: 4,
      links_clicked: 2,
      queue_jobs_processed: 3,
    },
    limits: {
      monthly_comments_processed_limit: 250,
      monthly_public_replies_sent_limit: 250,
      monthly_dms_sent_limit: 250,
      monthly_links_clicked_limit: 1000,
      queue_jobs_processed_limit: null,
    },
    remaining: {
      monthly_comments_processed_limit: 238,
      monthly_public_replies_sent_limit: 242,
      monthly_dms_sent_limit: 246,
      monthly_links_clicked_limit: 998,
      queue_jobs_processed_limit: null,
    },
    exceeded: {},
    max_instagram_accounts: 1,
    max_active_automations: 2,
    connectedInstagramAccountsCount: 0,
    activeAutomationsCount: 1,
  };
}

function planCatalogue() {
  return {
    billing_enabled: false,
    plans: [
      {
        plan_key: 'free',
        display_name: 'Free',
        monthly_price_placeholder: 0,
        max_instagram_accounts: 1,
        max_active_automations: 2,
        monthly_comments_processed_limit: 250,
        monthly_public_replies_sent_limit: 250,
        monthly_dms_sent_limit: 250,
        features: ['Manual admin grants supported'],
      },
      {
        plan_key: 'pro',
        display_name: 'Pro',
        monthly_price_placeholder: 29,
        max_instagram_accounts: 3,
        max_active_automations: 25,
        monthly_comments_processed_limit: 10000,
        monthly_public_replies_sent_limit: 10000,
        monthly_dms_sent_limit: 10000,
        features: ['Higher limits'],
      },
    ],
  };
}

function adminOverview() {
  return {
    total_users: 2,
    users_created_today: 1,
    users_created_7d: 1,
    connected_instagram_accounts: 1,
    total_instagram_accounts: 1,
    active_automations: 1,
    total_automations: 2,
    plan_limited_counts: 0,
    retryable_failure_counts: 1,
    permanent_failure_counts: 0,
    queue_health: { pending: 1 },
    current_month_usage_totals: {
      comments_processed: 12,
      public_replies_sent: 8,
      dms_sent: 4,
      links_clicked: 2,
    },
    plan_distribution: { free: 1, pro: 1 },
  };
}

function adminUserDetail() {
  return {
    user_id: 'u_demo',
    profile: {
      user_id: 'u_demo',
      email: 'demo@example.com',
      username: 'demo',
      name: 'Demo User',
      status: 'active',
      google_linked: true,
      email_verified: true,
      created_at: '2026-05-01T10:00:00Z',
    },
    plan: { plan_key: 'free', display_name: 'Free', assignment_reason: 'smoke' },
    usage_current_month: {
      event_month: '2026-05',
      counters: {
        comments_processed: 12,
        public_replies_sent: 8,
        dms_sent: 4,
      },
    },
    instagram_accounts: [
      {
        id: 'acc_demo',
        username: 'demo_ig',
        instagram_account_id: 'ig_demo',
        connectionValid: true,
        active: true,
        tokenSource: 'page',
      },
    ],
    automations: [
      {
        automation_id: 'auto_1',
        name: 'Specific post rule',
        status: 'active',
        active: true,
        post_scope: 'specific',
        selected_media_id: 'media_1',
      },
    ],
    active_overrides: [
      {
        id: 'ov_1',
        type: 'additive_allowance',
        status: 'active',
        grant_name: 'Smoke allowance',
        metrics: { comments_processed_extra: 500 },
        starts_at: '2026-05-09T10:00:00Z',
        ends_at: '2026-05-16T10:00:00Z',
      },
    ],
    recent_failures: [
      {
        comment_id: 'c_retry',
        ig_comment_id: 'ig_c_retry',
        media_id: 'media_1',
        reply_status: 'failed_retryable',
        dm_status: 'disabled',
        action_status: 'failed_retryable',
        attempts: 2,
      },
    ],
  };
}

async function installBrowserGuards(page) {
  await page.addInitScript(() => {
    const originalAssign = window.location.assign.bind(window.location);
    window.__documentReloadCount = 0;
    window.__locationAssignCalls = [];
    try {
      window.location.assign = (url) => {
        window.__locationAssignCalls.push(String(url));
        return originalAssign(url);
      };
    } catch (_) {}
    window.WebSocket = class MockWebSocket {
      constructor() {
        setTimeout(() => {
          if (this.onclose) this.onclose({ code: 1000, reason: 'mocked' });
        }, 20);
      }
      close() {}
      send() {}
    };
  });
}

async function signInWithMockUser(page, overrides = {}) {
  const merged = { ...user, ...overrides };
  await page.addInitScript((storedUser) => {
    window.localStorage.setItem('mychat_token', 'e2e-token');
    window.localStorage.setItem('mychat_user', JSON.stringify(storedUser));
  }, merged);
}

async function setupMockApi(page, options = {}) {
  const googleEnabled = options.googleEnabled ?? false;
  let allowanceRequests = 0;

  await page.route(`${API_ORIGIN}/api/**`, async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname.replace(/^\/api/, '');
    const method = request.method();

    if (path === '/auth/google/config') {
      return route.fulfill(json({ enabled: googleEnabled, client_id: googleEnabled ? 'public-client-id' : '' }));
    }
    if (path === '/auth/login' && method === 'POST') {
      return route.fulfill(json({ detail: 'invalid_credentials' }, 401));
    }
    if (path === '/meta/data-deletion' && method === 'POST') {
      return route.fulfill(json({
        confirmation_code: 'mychat-del-e2e',
        url: '/data-deletion?confirmation_code=mychat-del-e2e',
      }));
    }
    if (path === '/auth/me') return route.fulfill(json(user));
    if (path === '/admin/me') {
      return route.fulfill(json({
        is_admin: true,
        role: 'owner',
        email: user.email,
        permissions,
      }));
    }
    if (path === '/instagram/accounts') return route.fulfill(json({ accounts: [] }));
    if (path === '/dashboard/summary') {
      return route.fulfill(json({
        selectedInstagramAccountId: 'ig_demo',
        totalContacts: 42,
        activeAutomations: 1,
        messagesSent: 12,
        conversionRate: 16.7,
        weeklyPerformance,
        topAutomations: [{ name: 'Specific post rule', sent: 12 }],
      }));
    }
    if (path === '/automations/summary') {
      return route.fulfill(json({
        items: [
          {
            id: 'auto_1',
            name: 'Specific post rule',
            status: 'active',
            post_scope: 'specific',
            media_id: 'media_1',
            mode: 'reply_and_dm',
            match: 'any',
            sent: 12,
            activationStartedAt: '2026-05-09T10:00:00Z',
            media_preview: { caption: 'Smoke post' },
          },
        ],
      }));
    }
    if (path === '/automations/auto_1') {
      return route.fulfill(json({
        id: 'auto_1',
        name: 'Specific post rule',
        status: 'active',
        post_scope: 'specific',
        media_id: 'media_1',
        comment_reply: 'Public reply',
        opening_dm_message: 'DM message',
      }));
    }
    if (path === '/automations/auto_1' && method === 'PATCH') return route.fulfill(json({ ok: true }));
    if (path === '/comments') {
      return route.fulfill(json({ comments: commentRows(), has_more: false }));
    }
    if (path.includes('/retry-reply') && method === 'POST') {
      return route.fulfill(json({ queued: true, reason: 'queued_by_smoke', reply_status: 'pending' }));
    }
    if (path.startsWith('/comments/') && path.endsWith('/reply') && method === 'POST') {
      return route.fulfill(json({ ok: true }));
    }
    if (path === '/plan/current') return route.fulfill(json(currentPlan()));
    if (path === '/plans') return route.fulfill(json(planCatalogue()));
    if (path === '/instagram/automation-health') {
      return route.fulfill(json({
        tasks: {
          comment_poller: { running: true, restarts: 0, consecutive_failures: 0 },
          automation_queue: { running: true, restarts: 0, consecutive_failures: 0 },
        },
        webhook: {
          last_received_at: '2026-05-09T10:00:00Z',
          last_processed_at: '2026-05-09T10:00:02Z',
        },
        accounts: [],
        jobs: { pending_comment_dm_sessions: 1, failed_comment_dm_sessions: 0 },
        config: { queue_interval_seconds: 5 },
      }));
    }
    if (path === '/observability/status') {
      return route.fulfill(json({
        backend: {
          sentry_configured: false,
          posthog_configured: false,
          environment: 'test',
          build_sha: 'e2e',
        },
      }));
    }
    if (path === '/admin/overview') return route.fulfill(json(adminOverview()));
    if (path === '/admin/users') {
      return route.fulfill(json({
        items: [
          {
            user_id: 'u_demo',
            email: 'demo@example.com',
            plan_key: 'free',
            instagram_accounts_count: 1,
            active_automations_count: 1,
            current_month_usage: {
              comments_processed: 12,
              public_replies_sent: 8,
              dms_sent: 4,
            },
            exceeded: {},
            created_at: '2026-05-01T10:00:00Z',
          },
        ],
        pagination: { page: 1, page_size: 25, total: 1, total_pages: 1 },
      }));
    }
    if (path === '/admin/users/u_demo/detail') return route.fulfill(json(adminUserDetail()));
    if (path === '/admin/users/u_demo/limit-overrides' && method === 'POST') {
      allowanceRequests += 1;
      await new Promise(resolve => setTimeout(resolve, 300));
      return route.fulfill(json({ ok: true, id: `ov_new_${allowanceRequests}` }));
    }
    if (path === '/admin/users/u_demo/limit-overrides/ov_1/revoke' && method === 'POST') {
      return route.fulfill(json({ ok: true }));
    }
    if (path === '/admin/users/u_demo/plan' && method === 'POST') return route.fulfill(json({ ok: true }));
    if (path === '/admin/users/u_demo/suspend' && method === 'POST') return route.fulfill(json({ status: 'suspended' }));
    if (path === '/admin/users/u_demo/unsuspend' && method === 'POST') return route.fulfill(json({ status: 'active' }));
    if (path === '/admin/users/u_demo/delete' && method === 'POST') return route.fulfill(json({ status: 'deleted' }));
    if (path === '/admin/automations/auto_1/disable' && method === 'POST') return route.fulfill(json({ ok: true }));
    if (path === '/admin/members') {
      return route.fulfill(json({
        items: [
          {
            user_id: 'u_owner',
            email: 'owner@example.com',
            role: 'owner',
            disabled_at: null,
          },
        ],
      }));
    }
    if (path === '/admin/metrics/reconciliation') {
      return route.fulfill(json({
        event_month: '2026-05',
        items: [
          {
            metric_name: 'public_replies_sent_month',
            dashboard_value: 8,
            recomputed_value: 8,
            difference: 0,
            status: 'ok',
            source: 'monthly_usage',
          },
        ],
        mismatch_count: 0,
      }));
    }

    return route.fulfill(json({ ok: true }));
  });
}

module.exports = {
  setupMockApi,
  signInWithMockUser,
  installBrowserGuards,
};

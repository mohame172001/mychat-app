const fs = require('fs');
const path = require('path');

const source = fs.readFileSync(
  path.join(__dirname, 'AdminConsole.jsx'),
  'utf8',
);

describe('Webhook Verification tab structural contract', () => {
  test('defaults the username filter to muhammad_gehad', () => {
    expect(source).toContain("WV_DEFAULT_USERNAME = 'muhammad_gehad'");
    expect(source).toContain('useState(WV_DEFAULT_USERNAME)');
  });

  test('renders the four quick-range chips: 10, 30, 60, 120 minutes', () => {
    expect(source).toContain('WV_QUICK_RANGES_MIN = [10, 30, 60, 120]');
    expect(source).toContain('webhook-verification-range-');
  });

  test('since_minutes input uses the shared rounded input class', () => {
    expect(source).toContain('WV_INPUT_CLASS');
    expect(source).toContain('rounded-md');
    expect(source).toContain('data-testid="webhook-verification-since"');
    expect(source).toContain('inputMode="numeric"');
  });

  test('shows the active filter / server-now / window bar', () => {
    expect(source).toContain('webhook-verification-active-filters');
    expect(source).toContain('server_now_utc');
    expect(source).toContain('window_start_utc');
    expect(source).toContain('window_end_utc');
    expect(source).toContain('applied_filters');
  });

  test('renders the no-username warning when filter is empty', () => {
    expect(source).toContain('webhook-verification-no-username-warning');
    expect(source).toContain('results may include unrelated or older events');
  });

  test('renders the Reload-after-test helper note', () => {
    expect(source).toContain(
      'After posting a fresh Instagram comment, wait 10–30 seconds, then click Reload.',
    );
  });

  test('renders the three view toggles', () => {
    expect(source).toContain('webhook-verification-toggle-webhook-only');
    expect(source).toContain('webhook-verification-toggle-success-only');
    expect(source).toContain('webhook-verification-toggle-hide-rescans');
  });

  test('renders badge palette for source / stage / skip_reason rows', () => {
    expect(source).toContain('WvBadge');
    // case statements in wvBadgeClasses
    expect(source).toMatch(/case 'webhook':/);
    expect(source).toMatch(/case 'polling':/);
    expect(source).toMatch(/case 'automation_success':/);
    expect(source).toMatch(/case 'public_reply_attempted':/);
    expect(source).toMatch(/case 'opening_dm_attempted':/);
    expect(source).toMatch(/case 'public_reply_failed':/);
    expect(source).toMatch(/case 'opening_dm_failed':/);
    expect(source).toMatch(/case 'opening_dm_skipped':/);
    expect(source).toMatch(/case 'opening_dm_already_sent_for_commenter_media':/);
    expect(source).toMatch(/case 'already_replied_success':/);
    expect(source).toMatch(/case 'bot_own_reply':/);
    expect(source).toMatch(/case 'webhook_comment_detected':/);
    // Source badge inline conditional.
    expect(source).toContain("ev.source === 'webhook'");
  });

  test('renders reply and dm provider proof fields', () => {
    for (const token of [
      'reply_provider_comment_id_partial',
      'provider_reply_id_partial',
      'reply_provider_response_ok',
      'opening_message_id_partial',
      'provider_message_id_partial',
      'dm_provider_message_id_partial',
      'dm_provider_response_ok',
      'dm_provider_message_id_missing',
      'dm_provider_response_shape',
      'reply_status',
      'dm_status',
      'action_status',
      'last_public_reply_error',
      'last_dm_error',
      'dm_skip_reason',
    ]) {
      expect(source).toContain(token);
    }
    expect(source).toContain('provider proof');
    expect(source).toContain('error / skip');
    expect(source).toContain('reply_id:');
    expect(source).toContain('dm_id:');
    expect(source).toContain('dm_success_without_provider_id');
    expect(source).toContain('graph_visibility_unknown');
  });

  test('renders read-only Graph reply visibility diagnostic control', () => {
    expect(source).toContain('webhook-verification-graph-check');
    expect(source).toContain('webhook-verification-graph-result');
    expect(source).toContain('webhook-verification-graph-error');
    expect(source).toContain('/admin/instagram/webhook-verification/reply-visibility');
    expect(source).toContain('Check Graph reply');
    expect(source).toContain('graph_reply_direct_fetch_ok');
    expect(source).toContain('graph_reply_found_under_original_comment');
  });

  test('Copy JSON wraps backend response with active_filters + browser_timezone + copied_at_local', () => {
    expect(source).toContain('copied_at_local');
    expect(source).toContain('browser_timezone');
    expect(source).toContain('active_filters');
    expect(source).toContain('ui_view_filters');
    expect(source).toContain('backend_response');
    // The backend response is copied intact, including provider-proof
    // fields once the endpoint returns them.
    expect(source).toContain('reply_provider_comment_id_partial');
    expect(source).toContain('provider_message_id_partial');
    expect(source).toContain('dm_provider_message_id_partial');
    expect(source).toContain('dm_provider_message_id_missing');
  });

  test('renders event created_at via local-time formatter with UTC tooltip', () => {
    expect(source).toContain('wvFormatLocal');
    // The created_at cell carries `title={ev.created_at || ''}` so the raw
    // UTC value is visible on hover.
    expect(source).toMatch(/title=\{ev\.created_at \|\| ''\}/);
  });

  test('defines parseBackendUtcTimestamp and uses it from wvFormatLocal', () => {
    // The helper must exist by name (operator can search the codebase).
    expect(source).toContain('function parseBackendUtcTimestamp');
    // It must defensively handle both Z-suffixed and naive ISO strings.
    expect(source).toMatch(/\/Z\$\/\.test/);
    expect(source).toMatch(/\[\+\-\]\\d\{2\}:\?\\d\{2\}\$/);
    // wvFormatLocal must consume the parser, not call `new Date(ts)` raw.
    expect(source).toContain('parseBackendUtcTimestamp(ts)');
    // Defensive: the formatter must not bypass the parser by calling
    // new Date directly on the timestamp string.
    expect(source).not.toMatch(/new Date\(ts\)/);
  });

  test('Phase 2F: subscription_status panel renders and is gated by data', () => {
    expect(source).toContain('webhook-verification-subscription');
    expect(source).toContain('subscriptionStatus.overall_ready');
    expect(source).toContain('expected_fields');
    expect(source).toContain('webhook_comment_delivery_configured');
    expect(source).toContain('missing_fields');
    expect(source).toContain('last_webhook_repair_attempt_at');
  });

  test('Phase 2F: Repair button calls the admin repair endpoint', () => {
    expect(source).toContain('webhook-verification-repair');
    expect(source).toContain('/admin/instagram/repair-comment-webhooks');
    // Reload after repair so the operator sees the new state.
    expect(source).toMatch(/onRepairWebhooks[\s\S]{0,1500}await load\(\)/);
  });

  test('Phase 2F: Repair result block renders actionable_error', () => {
    expect(source).toContain('webhook-verification-repair-result');
    expect(source).toContain('repairResult.actionable_error');
    expect(source).toContain('subscribe_status');
  });

  test('Phase 2G: summary surfaces polling-seen / polling-send-disabled counters', () => {
    expect(source).toContain('polling_seen_count');
    expect(source).toContain('polling_send_disabled_count');
    expect(source).toContain('Polling: send disabled');
  });

  test('Phase 2G: polling_mode chip rendered with color cues', () => {
    expect(source).toContain('webhook-verification-polling-mode');
    expect(source).toContain('emergency_fallback_enabled');
    expect(source).toContain('polling_interval_seconds');
  });

  test('does not introduce any username-specific automation logic', () => {
    // Allowed: ui default WV_DEFAULT_USERNAME = 'muhammad_gehad'; the
    // placeholder string; and the input value default. Forbidden:
    // any conditional that branches automation behavior on the
    // string. Static scan: zero `if (username === 'muhammad_gehad'`
    // and zero `if (username === 'mogehad17'` patterns.
    expect(source).not.toMatch(/if\s*\(\s*username\s*===?\s*['"]muhammad_gehad['"]/);
    expect(source).not.toMatch(/if\s*\(\s*username\s*===?\s*['"]mogehad17['"]/);
  });
});

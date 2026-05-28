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
    expect(source).toMatch(/case 'already_replied_success':/);
    expect(source).toMatch(/case 'bot_own_reply':/);
    expect(source).toMatch(/case 'webhook_comment_detected':/);
    // Source badge inline conditional.
    expect(source).toContain("ev.source === 'webhook'");
  });

  test('Copy JSON wraps backend response with active_filters + browser_timezone + copied_at_local', () => {
    expect(source).toContain('copied_at_local');
    expect(source).toContain('browser_timezone');
    expect(source).toContain('active_filters');
    expect(source).toContain('ui_view_filters');
    expect(source).toContain('backend_response');
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

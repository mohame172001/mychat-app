import {
  formatDateTime,
  formatCompactDateTime,
  formatChartAxisLabel,
  formatChartTooltipTitle,
} from './dateTime';

const ISO_REGEX = /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;

describe('dateTime helper', () => {
  test('returns "-" for null/undefined/empty/invalid', () => {
    expect(formatDateTime(null)).toBe('-');
    expect(formatDateTime(undefined)).toBe('-');
    expect(formatDateTime('')).toBe('-');
    expect(formatDateTime('not-a-date')).toBe('-');
    expect(formatCompactDateTime(null)).toBe('-');
    expect(formatChartTooltipTitle(null, '24h')).toBe('-');
    expect(formatChartAxisLabel(null, '7d')).toBe('-');
  });

  test('formatDateTime never returns a raw ISO string', () => {
    const out = formatDateTime('2025-04-21T14:40:07Z');
    expect(out).not.toMatch(ISO_REGEX);
    expect(out).not.toMatch(/\d{4}-\d{2}-\d{2}T\d{2}/);
  });

  test('formatCompactDateTime never returns a raw ISO string', () => {
    const out = formatCompactDateTime('2025-04-21T14:40:07Z');
    expect(out).not.toMatch(ISO_REGEX);
    expect(out).not.toMatch(/\d{4}-\d{2}-\d{2}T\d{2}/);
  });

  test('formatChartTooltipTitle 24h does not include raw ISO', () => {
    const out = formatChartTooltipTitle('2026-05-24T21:00:00', '24h');
    expect(out).not.toMatch(/\d{4}-\d{2}-\d{2}T\d{2}/);
    expect(out).not.toContain('2026-05-24T21');
    // Should contain an hour-minute like "21:00".
    expect(out).toMatch(/\d{2}:\d{2}/);
  });

  test('formatChartTooltipTitle 7d shows weekday + month', () => {
    const out = formatChartTooltipTitle('2026-05-21', '7d');
    expect(out).not.toMatch(/\d{4}-\d{2}-\d{2}T\d{2}/);
    expect(out).not.toMatch(ISO_REGEX);
    // Loose regex — locales vary, but a 7d label should mention "May".
    expect(/May/i.test(out)).toBe(true);
  });

  test('formatChartTooltipTitle 30d shows month + day, no time', () => {
    const out = formatChartTooltipTitle('2026-05-21', '30d');
    expect(out).not.toMatch(/\d{4}-\d{2}-\d{2}T\d{2}/);
    expect(out).not.toMatch(/\d{2}:\d{2}/);
  });

  test('formatChartTooltipTitle all shows month + year', () => {
    const out = formatChartTooltipTitle('2026-05', 'all');
    expect(out).not.toMatch(/\d{4}-\d{2}-\d{2}T\d{2}/);
    expect(out).not.toMatch(/^\d{4}-\d{2}$/);
    expect(/May/i.test(out)).toBe(true);
    expect(out).toMatch(/2026/);
  });

  test('formatChartAxisLabel 24h shows hour only', () => {
    const out = formatChartAxisLabel('2026-05-24T21:00:00', '24h');
    expect(out).not.toMatch(/\d{4}-\d{2}-\d{2}T\d{2}/);
    expect(out).not.toContain('2026-05-24');
  });

  test('formatChartAxisLabel all returns month short string', () => {
    const out = formatChartAxisLabel('2026-05', 'all');
    expect(out).not.toContain('-');
    expect(out.length).toBeLessThanOrEqual(6);
  });

  test('formatChartAxisLabel 30d returns Month + day', () => {
    const out = formatChartAxisLabel('2026-05-21', '30d');
    expect(out).not.toMatch(/\d{4}-\d{2}-\d{2}T\d{2}/);
    expect(out).not.toContain('-');
    expect(/May/i.test(out)).toBe(true);
  });

  test('YYYY-MM monthly buckets do not drift to a previous month in local TZ', () => {
    // The default Date constructor parses 'YYYY-MM' as UTC midnight on
    // the 1st. In any timezone west of UTC, naive use would label May
    // as "Apr". The helper must keep the input month.
    const out = formatChartTooltipTitle('2026-05', 'all');
    expect(/May/i.test(out)).toBe(true);
  });
});

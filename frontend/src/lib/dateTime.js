/**
 * Centralized date/time formatting for visible UI.
 *
 * All helpers accept ISO strings, `Date` instances, numeric epoch ms, or
 * null/undefined. They never throw — invalid input yields `'-'`. All
 * formatting uses the browser's local timezone via `Intl.DateTimeFormat`
 * so UTC backend timestamps render in the user's wall-clock time.
 *
 * Keep this file the single source of truth for visible timestamps. Raw
 * ISO strings remain available in Copy JSON outputs; only what the user
 * SEES should be routed through here.
 */

const FALLBACK = '-';

function _toDate(value) {
  if (value === null || value === undefined || value === '') return null;
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value;
  // Month buckets from the dashboard `all` range come in as 'YYYY-MM'.
  // The default Date constructor parses these as UTC midnight on the
  // first of the month, which can shift the apparent month westward in
  // local timezone. Parse as local first-of-month instead.
  if (typeof value === 'string') {
    const monthOnly = /^(\d{4})-(\d{2})$/.exec(value);
    if (monthOnly) {
      const d = new Date(Number(monthOnly[1]), Number(monthOnly[2]) - 1, 1);
      return Number.isNaN(d.getTime()) ? null : d;
    }
    // 'YYYY-MM-DD' alone is also parsed as UTC midnight by default; force
    // local midnight so daily buckets don't drift across the date line.
    const dayOnly = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
    if (dayOnly) {
      const d = new Date(
        Number(dayOnly[1]),
        Number(dayOnly[2]) - 1,
        Number(dayOnly[3]),
      );
      return Number.isNaN(d.getTime()) ? null : d;
    }
  }
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

function _safeFormat(date, options, locale) {
  try {
    return new Intl.DateTimeFormat(locale || undefined, options).format(date);
  } catch (_) {
    return date.toLocaleString();
  }
}

function _isSameDay(a, b) {
  return (
    a.getFullYear() === b.getFullYear()
    && a.getMonth() === b.getMonth()
    && a.getDate() === b.getDate()
  );
}

/**
 * Full readable date+time. Used for headline timestamps (e.g. "May 25,
 * 2026, 14:40"). Falls back to "Today, 14:40" when the timestamp is
 * within the current calendar day so dense logs stay scannable.
 */
export function formatDateTime(value) {
  const d = _toDate(value);
  if (!d) return FALLBACK;
  const now = new Date();
  if (_isSameDay(d, now)) {
    return `Today, ${_safeFormat(d, { hour: '2-digit', minute: '2-digit', hour12: false })}`;
  }
  return _safeFormat(d, {
    year: 'numeric', month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit', hour12: false,
  });
}

/**
 * Compact form for dense logs (Flight Recorder rows etc).
 * Today: "14:40:07". Other days: "May 25, 14:40:07".
 */
export function formatCompactDateTime(value) {
  const d = _toDate(value);
  if (!d) return FALLBACK;
  const now = new Date();
  if (_isSameDay(d, now)) {
    return _safeFormat(d, { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false });
  }
  return _safeFormat(d, {
    month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  });
}

/**
 * Date-only short form: "May 25" or "May 25, 2026" (year shown only when
 * the year differs from the current one).
 */
export function formatDateShort(value) {
  const d = _toDate(value);
  if (!d) return FALLBACK;
  const now = new Date();
  if (d.getFullYear() === now.getFullYear()) {
    return _safeFormat(d, { month: 'short', day: 'numeric' });
  }
  return _safeFormat(d, { year: 'numeric', month: 'short', day: 'numeric' });
}

/**
 * Resolve the locale to use for chart labels. Chart text should follow
 * the app UI language (`ar` or `en`), NOT the browser's system locale,
 * so an English user on an Arabic-locale browser still sees
 * "May 24, 22:00" instead of "24 مايو 22:00". The optional locale arg
 * lets the caller pass `'en'` / `'ar'` / `'en-US'` etc explicitly.
 * When omitted the helper falls back to the browser default (used by
 * generic visible timestamps where browser locale is appropriate).
 */
function _chartLocale(localeOverride) {
  if (localeOverride) return localeOverride;
  return 'en';
}

/**
 * Compact x-axis label per dashboard range. Used by the chart bars
 * underneath the bucket. Always short, never includes year.
 */
export function formatChartAxisLabel(value, rangeKey, locale) {
  const d = _toDate(value);
  if (!d) return FALLBACK;
  const loc = _chartLocale(locale);
  if (rangeKey === '24h') {
    return _safeFormat(d, { hour: '2-digit', hour12: false }, loc);
  }
  if (rangeKey === 'all') {
    return _safeFormat(d, { month: 'short' }, loc);
  }
  if (rangeKey === '7d') {
    return _safeFormat(d, { weekday: 'short' }, loc);
  }
  return _safeFormat(d, { month: 'short', day: 'numeric' }, loc);
}

/**
 * Tooltip title for a dashboard chart bar. Each range has a specific
 * shape so the user reads the bucket in plain language:
 *   24h: "Today, 21:00" or "May 24, 21:00"
 *    7d: "Tue, May 21"
 *   30d: "May 21"   (year if different)
 *   all: "May 2026"
 *
 * Locale is forced to English by default so the tooltip stays
 * predictable for English UI users regardless of the browser
 * system locale. Pass `'ar'` when the app language is Arabic.
 */
export function formatChartTooltipTitle(value, rangeKey, locale) {
  const d = _toDate(value);
  if (!d) return FALLBACK;
  const now = new Date();
  const yearOpt = d.getFullYear() === now.getFullYear() ? {} : { year: 'numeric' };
  const loc = _chartLocale(locale);
  const isAr = String(loc).toLowerCase().startsWith('ar');
  const todayPrefix = isAr ? 'اليوم' : 'Today';
  if (rangeKey === '24h') {
    const hm = _safeFormat(d, { hour: '2-digit', minute: '2-digit', hour12: false }, loc);
    if (_isSameDay(d, now)) return `${todayPrefix}, ${hm}`;
    return `${_safeFormat(d, { month: 'short', day: 'numeric', ...yearOpt }, loc)}, ${hm}`;
  }
  if (rangeKey === '7d') {
    return _safeFormat(d, { weekday: 'short', month: 'short', day: 'numeric', ...yearOpt }, loc);
  }
  if (rangeKey === '30d') {
    return _safeFormat(d, { month: 'short', day: 'numeric', ...yearOpt }, loc);
  }
  if (rangeKey === 'all') {
    return _safeFormat(d, { year: 'numeric', month: 'short' }, loc);
  }
  return formatDateTime(value);
}

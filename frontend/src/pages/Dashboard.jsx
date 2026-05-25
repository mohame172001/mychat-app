import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import {
  Users, Zap, Send, TrendingUp, Plus, RefreshCw,
  MessageSquare, Reply, Mail, MousePointerClick, Instagram,
  ChevronDown, ChevronUp,
} from 'lucide-react';
import { Link } from 'react-router-dom';
import api from '../lib/api';
import { cachedApiGet, cachedApiGetSWR, getCachedApiData } from '../lib/apiCache';
import { ROUTES } from '../constants/routes';
import { useAuth } from '../context/AuthContext';
import { useTranslation } from '../lib/i18n';
import {
  formatChartAxisLabel as fmtChartAxisLabel,
  formatChartTooltipTitle,
} from '../lib/dateTime';

const DASHBOARD_TTL_MS = 60 * 1000;
// Phase 2.18Y cold-start fix: keep persisted dashboard data eligible
// for stale-while-revalidate render up to 24h. When a user opens the
// site for the first time of the day, the previous 5-min window forced
// a full loading state and made the app feel heavy. With 24h SWR, the
// UI paints from localStorage immediately and a fresh /dashboard/summary
// call refreshes the numbers in the background.
const DASHBOARD_MAX_STALE_MS = 24 * 60 * 60 * 1000;
const MSG = {
  en: {
    refreshFailed: "Couldn't refresh. Showing the latest available data.",
    igRequired: 'Connect or reconnect Instagram to load dashboard data.',
    cannotLoad: 'Dashboard data could not be loaded.',
    timeout: 'Dashboard request timed out. Please try again.',
    sessionExpired: 'Session expired. Please log in again.',
    serverError: 'Dashboard server error. Please try again later.',
    networkError: 'Network error. Please check your connection.',
  },
  ar: {
    refreshFailed: 'تعذّر التحديث. نعرض آخر البيانات المتاحة.',
    igRequired: 'اربط حساب Instagram أو أعد ربطه لتحميل بيانات لوحة التحكم.',
    cannotLoad: 'تعذّر تحميل بيانات لوحة التحكم.',
    timeout: 'انتهت مهلة الطلب. حاول مرة أخرى.',
    sessionExpired: 'انتهت صلاحية الجلسة. سجّل الدخول من جديد.',
    serverError: 'حدث خطأ في الخادم. حاول لاحقاً.',
    networkError: 'خطأ في الشبكة. تحقّق من اتصالك.',
  },
};

const classifyDashboardError = (err, user, m) => {
  if (!err) return m.cannotLoad;
  if (err.code === 'ECONNABORTED' || err.message?.includes?.('timeout')) return m.timeout;
  const status = err?.response?.status;
  if (status === 401 || status === 403) return m.sessionExpired;
  if (status && status >= 500) return m.serverError;
  const detail = String(err?.response?.data?.detail || err?.message || '').toLowerCase();
  const instagramConnectProblem = detail.includes('no instagram account')
    || detail.includes('instagram account')
    || detail.includes('instagram connection')
    || user?.instagramConnected === false
    || user?.instagramConnectionValid === false;
  if (instagramConnectProblem) return m.igRequired;
  if (!status && err.message?.includes?.('Network')) return m.networkError;
  return m.cannotLoad;
};

const RANGE_OPTIONS = ['24h', '7d', '30d', 'all'];
const DEFAULT_RANGE = '7d';

const formatXAxisLabel = (point, rangeKey, locale) => {
  if (!point) return '';
  // Route every range through the central helper so axis labels stay
  // in lockstep with tooltip titles and never leak raw ISO strings.
  const formatted = fmtChartAxisLabel(point.date || point.day, rangeKey, locale);
  if (formatted && formatted !== '-') return formatted;
  return point.day || '';
};

const dashboardTickIndexes = (count, rangeKey) => {
  if (count <= 0) return [];
  if (rangeKey === '7d') return Array.from({ length: count }, (_, i) => i);
  if (rangeKey === '24h') {
    const indexes = new Set([0, count - 1]);
    for (let i = 3; i < count - 1; i += 4) indexes.add(i);
    return [...indexes].sort((a, b) => a - b);
  }
  if (rangeKey === '30d') {
    const indexes = new Set([0, count - 1]);
    for (let i = 6; i < count - 1; i += 7) indexes.add(i);
    return [...indexes].sort((a, b) => a - b);
  }
  const indexes = new Set([0, count - 1]);
  for (let i = 2; i < count - 1; i += 3) indexes.add(i);
  return [...indexes].sort((a, b) => a - b);
};

const Dashboard = () => {
  const { user } = useAuth();
  const { t, lang } = useTranslation();
  const ar = lang === 'ar';
  const m = MSG[ar ? 'ar' : 'en'];
  const userInstagramConnected = user?.instagramConnected;
  const userInstagramConnectionValid = user?.instagramConnectionValid;
  const [range, setRange] = useState(() => {
    try {
      const stored = typeof localStorage !== 'undefined'
        ? localStorage.getItem('mychat_dashboard_range')
        : null;
      return RANGE_OPTIONS.includes(stored) ? stored : DEFAULT_RANGE;
    } catch (_) {
      return DEFAULT_RANGE;
    }
  });
  const cacheKey = [
    'dashboard-summary',
    user?.id || 'anon',
    user?.activeInstagramAccountId || user?.activeInstagramIgUserId || 'active',
    range,
  ].join(':');
  const summaryUrl = `/dashboard/summary?range=${encodeURIComponent(range)}`;
  const [stats, setStats] = useState(() => getCachedApiData(cacheKey, { maxStaleMs: DASHBOARD_MAX_STALE_MS }) || null);
  const [loading, setLoading] = useState(!stats);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState('');
  const [hoveredBar, setHoveredBar] = useState(null);
  const [showMoreStats, setShowMoreStats] = useState(false);
  const statsRef = useRef(stats);

  useEffect(() => {
    statsRef.current = stats;
  }, [stats]);

  useEffect(() => {
    let alive = true;
    const cached = getCachedApiData(cacheKey, { maxStaleMs: DASHBOARD_MAX_STALE_MS });
    if (cached) {
      setStats(cached);
      setLoading(false);
    } else {
      // Keep the previous range visible while the new range refreshes.
      // This avoids the heavy-feeling blank/skeleton transition when the
      // user switches between 24h / 7d / 30d / all.
      const hasPreviousStats = Boolean(statsRef.current);
      setLoading((currentLoading) => currentLoading && !hasPreviousStats);
      setRefreshing(hasPreviousStats);
    }

    const load = async () => {
      try {
        const result = await cachedApiGetSWR(
          cacheKey,
          () => api.get(summaryUrl),
          {
            ttlMs: DASHBOARD_TTL_MS,
            maxStaleMs: DASHBOARD_MAX_STALE_MS,
            persist: true,
            onUpdate: (data, updateResult) => {
              if (alive) {
                if (data) setStats(data);
                setError(updateResult?.error ? m.refreshFailed : '');
              }
            },
          }
        );
        if (!alive) return;
        setStats(result.data);
        if (result.error) {
          setError(m.refreshFailed);
        } else {
          setError('');
        }
      } catch (err) {
        console.error('[Dashboard] Failed to load data:', err);
        if (alive) {
          setError(classifyDashboardError(err, {
            instagramConnected: userInstagramConnected,
            instagramConnectionValid: userInstagramConnectionValid,
          }, m));
        }
      } finally {
        if (alive) {
          setLoading(false);
          setRefreshing(false);
        }
      }
    };
    load();
    return () => {
      alive = false;
    };
  }, [cacheKey, summaryUrl, m, userInstagramConnected, userInstagramConnectionValid]);

  const refreshDashboard = async () => {
    setRefreshing(true);
    setError('');
    try {
      const result = await cachedApiGet(
        cacheKey,
        () => api.get(summaryUrl),
        { ttlMs: DASHBOARD_TTL_MS, force: true, persist: true }
      );
      setStats(result.data);
    } catch (err) {
      console.error('[Dashboard] Refresh failed:', err);
      setError(stats ? m.refreshFailed : classifyDashboardError(err, {
        instagramConnected: userInstagramConnected,
        instagramConnectionValid: userInstagramConnectionValid,
      }, m));
    } finally {
      setRefreshing(false);
      setLoading(false);
    }
  };

  const onRangeChange = (nextRange) => {
    if (!RANGE_OPTIONS.includes(nextRange) || nextRange === range) return;
    setRange(nextRange);
    try {
      if (typeof localStorage !== 'undefined') {
        localStorage.setItem('mychat_dashboard_range', nextRange);
      }
    } catch (_) { /* ignore */ }
  };

  const chart = useMemo(() => stats?.weeklyPerformance || stats?.weekly_chart || [], [stats]);
  const chartLocale = ar ? 'ar' : 'en';
  const xAxisItems = useMemo(() => {
    const indexes = dashboardTickIndexes(chart.length, range);
    const maxIndex = Math.max(1, chart.length - 1);
    return indexes.map((index) => ({
      key: `${index}:${chart[index]?.date || chart[index]?.day || 'point'}`,
      label: formatXAxisLabel(chart[index], range, chartLocale),
      left: chart.length <= 1 ? 0 : (index / maxIndex) * 100,
    }));
  }, [chart, range, chartLocale]);
  const maxVal = Math.max(
    1,
    ...chart.map(d => Math.max(Number(d.messages || 0), Number(d.conversions || 0)))
  );
  const tickStep = maxVal <= 4 ? 1 : Math.ceil(maxVal / 4);
  const axisMax = Math.max(1, tickStep * 4);
  const yTicks = Array.from({ length: 5 }, (_, i) => axisMax - (tickStep * i));

  const connectedAccountsCount = Number(stats?.connectedAccounts ?? 0);
  const totalContactsSubtitle = connectedAccountsCount > 1
    ? t('dashboard.cards.subtitles.totalContacts')
    : null;
  const statsCards = [
    {
      label: t('dashboard.cards.totalContacts'),
      value: stats?.totalContacts ?? stats?.total_contacts ?? '-',
      icon: Users,
      subtitle: totalContactsSubtitle,
    },
    {
      label: t('dashboard.cards.activeAutomations'),
      value: stats?.activeAutomations ?? stats?.active_automations ?? '-',
      icon: Zap,
      subtitle: connectedAccountsCount > 1
        ? t('dashboard.cards.subtitles.activeAutomations')
        : null,
    },
    {
      label: t('dashboard.cards.messagesSent'),
      value: stats ? (stats?.messagesSent ?? stats?.messages_sent ?? 0).toLocaleString() : '-',
      icon: Send,
      subtitle: t('dashboard.cards.subtitles.messagesSent'),
    },
    {
      label: t('dashboard.cards.conversionRate'),
      value: `${stats?.conversionRate ?? stats?.conversion_rate ?? 0}%`,
      icon: TrendingUp,
      subtitle: t('dashboard.cards.subtitles.conversionRate'),
    },
  ];
  // Secondary KPI row — uses fields already present in the
  // /dashboard/summary payload. No backend change. Hidden until the
  // summary has loaded so we never render zero-everything skeletons.
  const secondaryKpis = stats
    ? [
        { label: t('dashboard.cards.secondary.commentsProcessed'),
          value: Number(stats?.commentsProcessed ?? 0).toLocaleString(),
          icon: MessageSquare },
        { label: t('dashboard.cards.secondary.publicReplies'),
          value: Number(stats?.publicRepliesSent ?? 0).toLocaleString(),
          icon: Reply },
        { label: t('dashboard.cards.secondary.openingDms'),
          value: Number(stats?.dmsSent ?? 0).toLocaleString(),
          icon: Mail },
        { label: t('dashboard.cards.secondary.linkClicks'),
          value: Number(stats?.linkClicks ?? 0).toLocaleString(),
          icon: MousePointerClick },
        { label: t('dashboard.cards.secondary.connectedAccounts'),
          value: Number(stats?.connectedAccounts ?? 0).toLocaleString(),
          icon: Instagram },
      ]
    : [];
  const topAutomations = stats?.topAutomations || [];
  // Main dashboard prefers active automations. Paused/draft drop to the
  // bottom and are slightly de-emphasized when shown. Within each tier
  // we keep the backend's existing sort (sent desc, created desc) by
  // using a stable sort. Cap at 3 for visual focus.
  const compactTopAutomations = [...topAutomations]
    .sort((a, b) => {
      const aActive = (a?.status || '').toLowerCase() === 'active' ? 0 : 1;
      const bActive = (b?.status || '').toLowerCase() === 'active' ? 0 : 1;
      return aActive - bActive;
    })
    .slice(0, 3);

  // Phase 2.18Z: first-run empty state — when a user has signed up
  // but hasn't connected Instagram OR has zero automations, the
  // dashboard otherwise shows '0' cards + an empty chart with no
  // sense of what to do next. The onboarding card below points them
  // to the two concrete actions that unlock everything else.
  const igConnected = Boolean(user?.instagramConnected);
  const totalAutomations = Number(stats?.activeAutomations ?? stats?.active_automations ?? 0);
  const isFirstRun = !loading && (!igConnected || totalAutomations === 0);

  return (
    <div className="p-4 sm:p-5 lg:p-6 max-w-7xl mx-auto" data-testid="dashboard-page">
      <div className="flex items-start justify-between flex-wrap gap-3">
        <div className="min-w-0">
          <h1 className="font-display text-2xl sm:text-3xl font-bold tracking-tight truncate">
            {t('dashboard.greeting').replace('{name}', user?.name || '')}
          </h1>
          <p className="mt-0.5 text-sm text-slate-500">{t('dashboard.subtitle')}</p>
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          {/* Range selector — compact pill group, accent on the active one */}
          <div
            className="inline-flex items-center rounded-full border border-slate-200 bg-white p-0.5"
            role="group"
            aria-label={t('dashboard.range.label')}
            data-testid="dashboard-range-selector"
          >
            {RANGE_OPTIONS.map((key) => {
              const active = key === range;
              return (
                <button
                  key={key}
                  type="button"
                  onClick={() => onRangeChange(key)}
                  data-testid={`dashboard-range-${key}`}
                  aria-pressed={active}
                  className={`px-3 py-1 text-xs font-medium rounded-full transition-colors ${
                    active
                      ? 'bg-slate-900 text-white'
                      : 'text-slate-600 hover:text-slate-900'
                  }`}
                >
                  {t(`dashboard.range.${key}`)}
                </button>
              );
            })}
          </div>
          {refreshing && (
            <span
              className="h-2 w-2 rounded-full bg-blue-500 animate-pulse"
              aria-label="Refreshing dashboard"
              data-testid="dashboard-range-refreshing"
            />
          )}
          <Button
            variant="ghost"
            size="sm"
            className="rounded-full text-slate-500 hover:text-slate-900"
            onClick={refreshDashboard}
            disabled={refreshing}
            data-testid="dashboard-refresh"
            aria-label={t('common.refresh')}
          >
            <RefreshCw className={`w-4 h-4 ${refreshing ? 'animate-spin' : ''}`} />
          </Button>
          <Link to={ROUTES.APP_AUTOMATIONS}>
            <Button size="sm" className="bg-slate-900 hover:bg-slate-800 text-white rounded-full">
              <Plus className="w-4 h-4 me-1.5" /> {t('common.newAutomation')}
            </Button>
          </Link>
        </div>
      </div>

      {error && (
        <div className="mt-4 rounded-xl border border-amber-100 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          {error}
        </div>
      )}

      {isFirstRun && (
        <Card className="mt-6 p-6 rounded-2xl border-blue-100 bg-gradient-to-br from-blue-50 to-cyan-50">
          <div className="flex items-start justify-between flex-wrap gap-4">
            <div>
              <h3 className="font-display font-bold text-lg text-slate-900">
                {igConnected ? t('dashboard.onboarding.titleConnected') : t('dashboard.onboarding.titleNew')}
              </h3>
              <p className="text-sm text-slate-600 mt-1 max-w-2xl">
                {igConnected
                  ? t('dashboard.onboarding.bodyConnected')
                  : t('dashboard.onboarding.bodyNew')}
              </p>
              <ol className="mt-4 space-y-2 text-sm text-slate-700">
                <li className="flex items-center gap-2">
                  <span className={`flex items-center justify-center w-5 h-5 rounded-full text-xs font-bold ${igConnected ? 'bg-emerald-500 text-white' : 'bg-blue-500 text-white'}`}>
                    {igConnected ? '✓' : '1'}
                  </span>
                  {t('dashboard.onboarding.step1')}
                </li>
                <li className="flex items-center gap-2">
                  <span className={`flex items-center justify-center w-5 h-5 rounded-full text-xs font-bold ${totalAutomations > 0 ? 'bg-emerald-500 text-white' : 'bg-slate-300 text-slate-700'}`}>
                    {totalAutomations > 0 ? '✓' : '2'}
                  </span>
                  {t('dashboard.onboarding.step2')}
                </li>
                <li className="flex items-center gap-2">
                  <span className="flex items-center justify-center w-5 h-5 rounded-full text-xs font-bold bg-slate-300 text-slate-700">3</span>
                  {t('dashboard.onboarding.step3')}
                </li>
              </ol>
            </div>
            <div className="flex flex-col gap-2 shrink-0">
              {!igConnected ? (
                <Link to="/app/settings">
                  <Button className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl">
                    {t('dashboard.onboarding.connectCta')}
                  </Button>
                </Link>
              ) : (
                <Link to={ROUTES.APP_AUTOMATIONS}>
                  <Button className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl">
                    <Plus className="w-4 h-4 me-1.5" /> {t('dashboard.onboarding.createCta')}
                  </Button>
                </Link>
              )}
            </div>
          </div>
        </Card>
      )}

      <div className="mt-5 grid grid-cols-2 lg:grid-cols-4 gap-3">
        {statsCards.map((s) => {
          const Icon = s.icon;
          return (
            <Card key={s.label} className="p-4 rounded-xl border-slate-100">
              <div className="flex items-start justify-between gap-2">
                <div className="text-xs font-medium text-slate-500 truncate">{s.label}</div>
                <Icon className="w-4 h-4 text-slate-400 shrink-0" />
              </div>
              <div className="mt-2 text-2xl font-bold font-display tabular-nums leading-tight">
                {loading && !stats ? (
                  <span className="block h-7 w-16 animate-pulse rounded bg-slate-100" data-testid="dashboard-skeleton" />
                ) : s.value}
              </div>
              {s.subtitle && (
                <div className="text-[11px] text-slate-400 mt-0.5" data-testid="dashboard-card-subtitle">{s.subtitle}</div>
              )}
            </Card>
          );
        })}
      </div>

      <div className="mt-4">
        <Card className="p-4 sm:p-5 rounded-xl border-slate-100">
          <div className="flex items-center justify-between flex-wrap gap-2">
            <div className="min-w-0">
              <h3 className="font-display font-semibold text-base" data-testid="dashboard-chart-title">
                {t(`dashboard.performanceTitles.${range}`) || t('dashboard.weeklyTitle')}
              </h3>
              <p className="text-xs text-slate-500">{t('dashboard.weeklySubtitle')}</p>
            </div>
            <div className="flex gap-3 text-[11px] text-slate-500">
              <div className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-blue-500" />{t('dashboard.legendMessages')}</div>
              <div className="flex items-center gap-1"><span className="w-2 h-2 rounded-full bg-pink-500" />{t('dashboard.legendConversions')}</div>
            </div>
          </div>
          <div className="mt-4">
            {loading && !chart.length ? (
              <div className="h-48 animate-pulse rounded-xl bg-slate-100" data-testid="dashboard-chart-skeleton" />
            ) : (
            <div className="flex gap-3">
              <div className="h-48 w-8 flex flex-col justify-between text-[11px] font-medium text-slate-400 text-end">
                {yTicks.map((tick) => (
                  <span key={tick}>{tick}</span>
                ))}
              </div>
              <div className="relative flex-1">
                <div className="absolute inset-0 flex flex-col justify-between pointer-events-none">
                  {yTicks.map((tick) => (
                    <div key={tick} className="border-t border-slate-100 first:border-slate-200" />
                  ))}
                </div>
                <div className="relative flex items-end justify-between gap-2 sm:gap-3 h-48">
                  {chart.map((d, index) => {
                    const key = d.date || `${d.day || 'point'}-${index}`;
                    const messages = Number(d.messages || 0);
                    const conversions = Number(d.conversions || 0);
                    const isActive = hoveredBar === key;
                    return (
                      <div
                        key={key}
                        className="relative flex-1 h-full flex items-end justify-center gap-1.5"
                        onMouseEnter={() => setHoveredBar(key)}
                        onMouseLeave={() => setHoveredBar(null)}
                        onFocus={() => setHoveredBar(key)}
                        onBlur={() => setHoveredBar(null)}
                        tabIndex={0}
                      >
                        {isActive && (
                          <div
                            className="absolute -top-16 left-1/2 -translate-x-1/2 z-10 min-w-[160px] rounded-xl bg-slate-950 px-3 py-2 text-xs text-white shadow-xl"
                            data-testid="dashboard-chart-tooltip"
                          >
                            <div className="font-semibold">
                              {formatChartTooltipTitle(d.date || d.day, range, chartLocale)}
                            </div>
                            <div className="mt-1 flex justify-between gap-4"><span>{ar ? 'الرسائل' : 'Messages'}</span><b>{messages}</b></div>
                            <div className="flex justify-between gap-4"><span>{ar ? 'التحويلات' : 'Conversions'}</span><b>{conversions}</b></div>
                          </div>
                        )}
                        <div
                          aria-label={`${d.day} messages ${messages}`}
                          className={`w-full max-w-[18px] rounded-t-lg bg-gradient-to-t from-blue-500 to-cyan-400 transition-all duration-150 ${isActive ? 'opacity-100 ring-2 ring-blue-200' : 'opacity-80 hover:opacity-100'}`}
                          style={{ height: `${messages > 0 ? Math.max(2, (messages / axisMax) * 100) : 0}%` }}
                        />
                        <div
                          aria-label={`${d.day} conversions ${conversions}`}
                          className={`w-full max-w-[18px] rounded-t-lg bg-gradient-to-t from-pink-500 to-orange-400 transition-all duration-150 ${isActive ? 'opacity-100 ring-2 ring-pink-200' : 'opacity-80 hover:opacity-100'}`}
                          style={{ height: `${conversions > 0 ? Math.max(2, (conversions / axisMax) * 100) : 0}%` }}
                        />
                      </div>
                    );
                  })}
                </div>
              </div>
            </div>
            )}
            <div
              className="relative ms-11 mt-2 h-5"
              data-testid="dashboard-x-axis"
            >
              {xAxisItems.map((tick) => (
                <div
                  key={tick.key}
                  className="absolute top-0 -translate-x-1/2 whitespace-nowrap text-center text-[11px] text-slate-500 font-medium"
                  style={{ left: `${tick.left}%` }}
                >
                  {tick.label}
                </div>
              ))}
            </div>
          </div>
        </Card>
      </div>

      {secondaryKpis.length > 0 && (
        <div className="mt-3">
          <button
            type="button"
            onClick={() => setShowMoreStats((value) => !value)}
            className="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 hover:border-slate-300 focus:outline-none focus:ring-2 focus:ring-slate-200 transition-colors"
            aria-expanded={showMoreStats}
            data-testid="dashboard-more-stats-toggle"
          >
            <span>{showMoreStats ? t('dashboard.lessStats') : t('dashboard.moreStats')}</span>
            {showMoreStats
              ? <ChevronUp className="w-3 h-3" aria-hidden="true" />
              : <ChevronDown className="w-3 h-3" aria-hidden="true" />}
          </button>
          {showMoreStats && (
            <div
              className="mt-2 grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-2"
              data-testid="dashboard-secondary-kpis"
            >
              {secondaryKpis.map((k) => {
                const Icon = k.icon;
                return (
                  <div
                    key={k.label}
                    className="px-3 py-2 rounded-lg bg-white border border-slate-100"
                  >
                    <div className="flex items-center gap-1.5 text-[10px] text-slate-500 uppercase tracking-wide">
                      <Icon className="w-3 h-3" />
                      <span className="truncate">{k.label}</span>
                    </div>
                    <div className="text-base font-semibold font-display tabular-nums mt-0.5">{k.value}</div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      )}

      <div className="mt-4">
        <Card className="p-4 sm:p-5 rounded-xl border-slate-100">
          <div className="flex items-center justify-between">
            <div className="min-w-0">
              <h3 className="font-display font-semibold text-base">{t('dashboard.topAutomations')}</h3>
              <p className="text-[11px] text-slate-500">{t('dashboard.topAutomationsSubtitle')}</p>
            </div>
            <Link to={ROUTES.APP_AUTOMATIONS} className="text-xs font-medium text-slate-500 hover:text-slate-900">{t('dashboard.viewAll')}</Link>
          </div>
          <div className="mt-3 divide-y divide-slate-100">
            {loading && !topAutomations.length && (
              <div className="space-y-2 py-2" data-testid="dashboard-automation-skeleton">
                {[0, 1, 2].map((i) => (
                  <div key={i} className="h-10 animate-pulse rounded-lg bg-slate-100" />
                ))}
              </div>
            )}
            {compactTopAutomations.map(a => {
              const isActive = (a?.status || '').toLowerCase() === 'active';
              return (
                <div
                  key={a.id}
                  className={`flex items-center gap-3 py-2.5 first:pt-0 last:pb-0 ${isActive ? '' : 'opacity-70'}`}
                  data-testid="dashboard-top-automation-row"
                >
                  <div className="w-7 h-7 rounded-lg bg-slate-100 flex items-center justify-center shrink-0">
                    <Zap className="w-3.5 h-3.5 text-slate-600" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="font-medium text-sm truncate">{a.name}</div>
                    <div className="text-[11px] text-slate-500">{a.trigger} · {(a.sent || 0).toLocaleString()} {ar ? 'مُرسلة' : 'sent'}</div>
                  </div>
                  <Badge className={`rounded-full text-[10px] ${a.status === 'active' ? 'bg-emerald-50 text-emerald-700 border-emerald-100' : a.status === 'paused' ? 'bg-amber-50 text-amber-700 border-amber-100' : 'bg-slate-100 text-slate-600 border-slate-200'}`}>
                    {ar
                      ? (a.status === 'active' ? 'نشطة' : a.status === 'paused' ? 'متوقّفة' : a.status === 'draft' ? 'مسودّة' : a.status)
                      : a.status}
                  </Badge>
                </div>
              );
            })}
            {!loading && topAutomations.length === 0 && <div className="text-sm text-slate-500 text-center py-6">{t('dashboard.noAutomations')}</div>}
          </div>
        </Card>
      </div>
    </div>
  );
};

export default Dashboard;

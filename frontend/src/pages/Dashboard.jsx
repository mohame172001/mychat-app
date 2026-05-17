import React, { useEffect, useState } from 'react';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import {
  Users, Zap, Send, TrendingUp, Plus, RefreshCw
} from 'lucide-react';
import { Link } from 'react-router-dom';
import api from '../lib/api';
import { cachedApiGet, cachedApiGetSWR, getCachedApiData } from '../lib/apiCache';
import { useAuth } from '../context/AuthContext';

const DASHBOARD_TTL_MS = 60 * 1000;
// Phase 2.18Y cold-start fix: keep persisted dashboard data eligible
// for stale-while-revalidate render up to 24h. When a user opens the
// site for the first time of the day, the previous 5-min window forced
// a full loading state and made the app feel heavy. With 24h SWR, the
// UI paints from localStorage immediately and a fresh /dashboard/summary
// call refreshes the numbers in the background.
const DASHBOARD_MAX_STALE_MS = 24 * 60 * 60 * 1000;
const REFRESH_FAILED_WITH_CACHE = "Couldn't refresh. Showing the latest available data.";
const INSTAGRAM_CONNECT_REQUIRED = 'Connect or reconnect Instagram to load dashboard data.';

const classifyDashboardError = (err, user) => {
  if (!err) return 'Dashboard data could not be loaded.';
  if (err.code === 'ECONNABORTED' || err.message?.includes?.('timeout')) return 'Dashboard request timed out. Please try again.';
  const status = err?.response?.status;
  if (status === 401 || status === 403) return 'Session expired. Please log in again.';
  if (status && status >= 500) return 'Dashboard server error. Please try again later.';
  const detail = String(err?.response?.data?.detail || err?.message || '').toLowerCase();
  const instagramConnectProblem = detail.includes('no instagram account')
    || detail.includes('instagram account')
    || detail.includes('instagram connection')
    || user?.instagramConnected === false
    || user?.instagramConnectionValid === false;
  if (instagramConnectProblem) return INSTAGRAM_CONNECT_REQUIRED;
  if (!status && err.message?.includes?.('Network')) return 'Network error. Please check your connection.';
  return 'Dashboard data could not be loaded.';
};

const Dashboard = () => {
  const { user } = useAuth();
  const userInstagramConnected = user?.instagramConnected;
  const userInstagramConnectionValid = user?.instagramConnectionValid;
  const cacheKey = [
    'dashboard-summary',
    user?.id || 'anon',
    user?.activeInstagramAccountId || user?.activeInstagramIgUserId || 'active',
  ].join(':');
  const [stats, setStats] = useState(() => getCachedApiData(cacheKey, { maxStaleMs: DASHBOARD_MAX_STALE_MS }) || null);
  const [loading, setLoading] = useState(!stats);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState('');
  const [hoveredBar, setHoveredBar] = useState(null);

  useEffect(() => {
    let alive = true;
    const cached = getCachedApiData(cacheKey, { maxStaleMs: DASHBOARD_MAX_STALE_MS });
    if (cached) {
      setStats(cached);
      setLoading(false);
    } else {
      setStats(null);
      setLoading(true);
    }

    const load = async () => {
      try {
        const result = await cachedApiGetSWR(
          cacheKey,
          () => api.get('/dashboard/summary'),
          {
            ttlMs: DASHBOARD_TTL_MS,
            maxStaleMs: DASHBOARD_MAX_STALE_MS,
            persist: true,
            onUpdate: (data, updateResult) => {
              if (alive) {
                if (data) setStats(data);
                setError(updateResult?.error ? REFRESH_FAILED_WITH_CACHE : '');
              }
            },
          }
        );
        if (!alive) return;
        setStats(result.data);
        if (result.error) {
          setError(REFRESH_FAILED_WITH_CACHE);
        } else {
          setError('');
        }
      } catch (err) {
        console.error('[Dashboard] Failed to load data:', err);
        if (alive) {
          setError(classifyDashboardError(err, {
            instagramConnected: userInstagramConnected,
            instagramConnectionValid: userInstagramConnectionValid,
          }));
        }
      } finally {
        if (alive) setLoading(false);
      }
    };
    load();
    return () => {
      alive = false;
    };
  }, [cacheKey, userInstagramConnected, userInstagramConnectionValid]);

  const refreshDashboard = async () => {
    setRefreshing(true);
    setError('');
    try {
      const result = await cachedApiGet(
        cacheKey,
        () => api.get('/dashboard/summary'),
        { ttlMs: DASHBOARD_TTL_MS, force: true, persist: true }
      );
      setStats(result.data);
    } catch (err) {
      console.error('[Dashboard] Refresh failed:', err);
      setError(stats ? REFRESH_FAILED_WITH_CACHE : classifyDashboardError(err, {
        instagramConnected: userInstagramConnected,
        instagramConnectionValid: userInstagramConnectionValid,
      }));
    } finally {
      setRefreshing(false);
      setLoading(false);
    }
  };

  const chart = stats?.weeklyPerformance || stats?.weekly_chart || [];
  const maxVal = Math.max(
    1,
    ...chart.map(d => Math.max(Number(d.messages || 0), Number(d.conversions || 0)))
  );
  const tickStep = maxVal <= 4 ? 1 : Math.ceil(maxVal / 4);
  const axisMax = Math.max(1, tickStep * 4);
  const yTicks = Array.from({ length: 5 }, (_, i) => axisMax - (tickStep * i));

  const statsCards = [
    { label: 'Total Contacts', value: stats?.totalContacts ?? stats?.total_contacts ?? '-', icon: Users },
    { label: 'Active Automations', value: stats?.activeAutomations ?? stats?.active_automations ?? '-', icon: Zap },
    { label: 'Messages Sent', value: stats ? (stats?.messagesSent ?? stats?.messages_sent ?? 0).toLocaleString() : '-', icon: Send },
    { label: 'Conversion Rate', value: `${stats?.conversionRate ?? stats?.conversion_rate ?? 0}%`, icon: TrendingUp },
  ];
  const topAutomations = stats?.topAutomations || [];

  // Phase 2.18Z: first-run empty state — when a user has signed up
  // but hasn't connected Instagram OR has zero automations, the
  // dashboard otherwise shows '0' cards + an empty chart with no
  // sense of what to do next. The onboarding card below points them
  // to the two concrete actions that unlock everything else.
  const igConnected = Boolean(user?.instagramConnected);
  const totalAutomations = Number(stats?.activeAutomations ?? stats?.active_automations ?? 0);
  const isFirstRun = !loading && (!igConnected || totalAutomations === 0);

  return (
    <div className="p-4 sm:p-6 lg:p-8 max-w-7xl mx-auto" data-testid="dashboard-page">
      <div className="flex items-end justify-between flex-wrap gap-4">
        <div>
          <h1 className="font-display text-3xl font-extrabold tracking-tight">Good morning, {user?.name}</h1>
          <p className="mt-1 text-slate-600">Here is what is happening with your Instagram automations today.</p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            className="rounded-xl"
            onClick={refreshDashboard}
            disabled={refreshing}
            data-testid="dashboard-refresh"
          >
            <RefreshCw className={`w-4 h-4 mr-1.5 ${refreshing ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
          <Link to="/app/automations">
            <Button className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl">
              <Plus className="w-4 h-4 mr-1.5" /> New Automation
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
                {igConnected ? 'You\'re set — create your first automation' : 'Welcome to MyChat 👋'}
              </h3>
              <p className="text-sm text-slate-600 mt-1 max-w-2xl">
                {igConnected
                  ? 'Now build a comment automation: when someone comments on your post, MyChat will reply publicly and send them a DM.'
                  : 'To start automating Instagram comments + DMs, connect your business account first. It takes 30 seconds.'}
              </p>
              <ol className="mt-4 space-y-2 text-sm text-slate-700">
                <li className="flex items-center gap-2">
                  <span className={`flex items-center justify-center w-5 h-5 rounded-full text-xs font-bold ${igConnected ? 'bg-emerald-500 text-white' : 'bg-blue-500 text-white'}`}>
                    {igConnected ? '✓' : '1'}
                  </span>
                  Connect your Instagram business account
                </li>
                <li className="flex items-center gap-2">
                  <span className={`flex items-center justify-center w-5 h-5 rounded-full text-xs font-bold ${totalAutomations > 0 ? 'bg-emerald-500 text-white' : 'bg-slate-300 text-slate-700'}`}>
                    {totalAutomations > 0 ? '✓' : '2'}
                  </span>
                  Create a comment automation rule
                </li>
                <li className="flex items-center gap-2">
                  <span className="flex items-center justify-center w-5 h-5 rounded-full text-xs font-bold bg-slate-300 text-slate-700">3</span>
                  Comments roll in — MyChat replies + DMs them automatically
                </li>
              </ol>
            </div>
            <div className="flex flex-col gap-2 shrink-0">
              {!igConnected ? (
                <Link to="/app/settings">
                  <Button className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl">
                    Connect Instagram
                  </Button>
                </Link>
              ) : (
                <Link to="/app/automations">
                  <Button className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl">
                    <Plus className="w-4 h-4 mr-1.5" /> Create automation
                  </Button>
                </Link>
              )}
            </div>
          </div>
        </Card>
      )}

      <div className="mt-8 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {statsCards.map((s) => {
          const Icon = s.icon;
          return (
            <Card key={s.label} className="p-5 rounded-2xl border-slate-100 hover:shadow-md transition-shadow">
              <div className="w-10 h-10 rounded-xl bg-slate-100 flex items-center justify-center">
                <Icon className="w-5 h-5 text-slate-700" />
              </div>
              <div className="mt-4 text-3xl font-extrabold font-display">
                {loading && !stats ? (
                  <span className="block h-9 w-20 animate-pulse rounded-lg bg-slate-100" data-testid="dashboard-skeleton" />
                ) : s.value}
              </div>
              <div className="text-sm text-slate-500 mt-1">{s.label}</div>
            </Card>
          );
        })}
      </div>

      <div className="mt-6">
        <Card className="p-6 rounded-2xl border-slate-100">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="font-display font-bold text-lg">Weekly Performance</h3>
              <p className="text-sm text-slate-500">Messages sent vs conversions</p>
            </div>
            <div className="flex gap-3 text-xs">
              <div className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full bg-blue-500" />Messages</div>
              <div className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full bg-pink-500" />Conversions</div>
            </div>
          </div>
          <div className="mt-6">
            {loading && !chart.length ? (
              <div className="h-48 animate-pulse rounded-xl bg-slate-100" data-testid="dashboard-chart-skeleton" />
            ) : (
            <div className="flex gap-3">
              <div className="h-48 w-8 flex flex-col justify-between text-[11px] font-medium text-slate-400 text-right">
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
                  {chart.map((d) => {
                    const key = d.date || d.day;
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
                          <div className="absolute -top-16 left-1/2 -translate-x-1/2 z-10 min-w-[150px] rounded-xl bg-slate-950 px-3 py-2 text-xs text-white shadow-xl">
                            <div className="font-semibold">{d.day}{d.date ? `, ${d.date}` : ''}</div>
                            <div className="mt-1 flex justify-between gap-4"><span>Messages</span><b>{messages}</b></div>
                            <div className="flex justify-between gap-4"><span>Conversions</span><b>{conversions}</b></div>
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
            <div className="ml-11 mt-2 grid grid-cols-7 gap-2 sm:gap-3">
              {chart.map((d) => (
                <div key={d.date || d.day} className="text-center text-xs text-slate-500 font-medium">{d.day}</div>
              ))}
            </div>
          </div>
        </Card>
      </div>

      <div className="mt-6">
        <Card className="p-6 rounded-2xl border-slate-100">
          <div className="flex items-center justify-between">
            <h3 className="font-display font-bold text-lg">Top Automations</h3>
            <Link to="/app/automations" className="text-sm font-medium text-slate-600 hover:text-slate-900">View all</Link>
          </div>
          <div className="mt-4 space-y-3">
            {loading && !topAutomations.length && (
              <div className="space-y-3" data-testid="dashboard-automation-skeleton">
                {[0, 1, 2].map((i) => (
                  <div key={i} className="h-16 animate-pulse rounded-xl bg-slate-100" />
                ))}
              </div>
            )}
            {topAutomations.map(a => (
              <div key={a.id} className="flex items-center gap-3 p-3 rounded-xl hover:bg-slate-50 transition-colors">
                <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 to-cyan-400 flex items-center justify-center"><Zap className="w-5 h-5 text-white" /></div>
                <div className="flex-1 min-w-0">
                  <div className="font-semibold text-sm truncate">{a.name}</div>
                  <div className="text-xs text-slate-500">{a.trigger} - {(a.sent || 0).toLocaleString()} sent</div>
                </div>
                <Badge className={`rounded-full ${a.status === 'active' ? 'bg-emerald-50 text-emerald-700 border-emerald-100' : a.status === 'paused' ? 'bg-amber-50 text-amber-700 border-amber-100' : 'bg-slate-100 text-slate-600 border-slate-200'}`}>
                  {a.status}
                </Badge>
              </div>
            ))}
            {!loading && topAutomations.length === 0 && <div className="text-sm text-slate-500 text-center py-6">No automations yet</div>}
          </div>
        </Card>
      </div>
    </div>
  );
};

export default Dashboard;

import React, { useEffect, useState, useCallback } from 'react';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Badge } from '../../components/ui/badge';
import {
  ShieldAlert, Users, Activity, RefreshCw, Search, ArrowLeft,
  CheckCircle2, AlertTriangle, Lock,
} from 'lucide-react';
import api from '../../lib/api';
import { toast } from 'sonner';
import {
  PLAN_KEYS, PLAN_DISPLAY,
  hasAnyExceeded, planDistributionRows, planOptions, formatTimestamp,
} from '../../lib/admin';

/**
 * Phase 2.4 Admin Console v0.
 *
 * Single-page console with tabs (overview | users | user-detail) so we
 * don't need nested routes. Visibility is gated by /api/admin/me — a
 * non-admin caller sees a friendly 'Not available' state.
 *
 * Privacy: every panel renders only sanitized fields returned by the
 * backend. The backend never returns raw text or tokens; this UI
 * therefore never has a surface to leak them.
 */

function StatCard({ label, value, hint }) {
  return (
    <div className="bg-white rounded-2xl border border-slate-100 p-4">
      <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold">{label}</div>
      <div className="mt-1 text-2xl font-bold text-slate-800 font-mono">{value}</div>
      {hint && <div className="text-xs text-slate-400 mt-1">{hint}</div>}
    </div>
  );
}

function OverviewTab({ data, onRefresh, loading }) {
  if (!data) return null;
  const totals = data.current_month_usage_totals || {};
  return (
    <div data-testid="admin-overview">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4 mb-6">
        <StatCard label="Total users" value={data.total_users} hint={`${data.users_created_7d || 0} new in 7d`} />
        <StatCard label="Connected IG accounts" value={data.connected_instagram_accounts} hint={`of ${data.total_instagram_accounts} total`} />
        <StatCard label="Active automations" value={data.active_automations} hint={`of ${data.total_automations} total`} />
        <StatCard label="Plan limited (comments)" value={data.plan_limited_counts} />
        <StatCard label="Comments this month" value={totals.comments_processed || 0} />
        <StatCard label="Public replies this month" value={totals.public_replies_sent || 0} />
        <StatCard label="DMs this month" value={totals.dms_sent || 0} />
        <StatCard label="Link clicks this month" value={totals.links_clicked || 0} />
        <StatCard label="Retryable failures" value={data.retryable_failure_counts || 0} />
        <StatCard label="Permanent failures" value={data.permanent_failure_counts || 0} />
        <StatCard label="Queue pending" value={(data.queue_health || {}).pending || 0} />
        <StatCard label="New users today" value={data.users_created_today || 0} />
      </div>

      <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-6" data-testid="plan-distribution">
        <h3 className="text-sm font-semibold text-slate-700 mb-3">Plan distribution</h3>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {planDistributionRows(data.plan_distribution).map(row => (
            <div key={row.key} className="rounded-xl border border-slate-100 p-3" data-testid={`plan-dist-${row.key}`}>
              <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold">{row.label}</div>
              <div className="mt-1 text-xl font-bold text-slate-800 font-mono">{row.count}</div>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

function UsersTab({ onSelect }) {
  const [page, setPage] = useState(1);
  const [pageSize] = useState(25);
  const [search, setSearch] = useState('');
  const [planKey, setPlanKey] = useState('');
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const params = { page, page_size: pageSize };
      if (search.trim()) params.search = search.trim();
      if (planKey) params.plan_key = planKey;
      const { data } = await api.get('/admin/users', { params });
      setData(data);
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Failed to load users';
      toast.error(typeof msg === 'string' ? msg : 'Failed to load users');
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, search, planKey]);

  useEffect(() => { load(); }, [load]);

  return (
    <div data-testid="admin-users">
      <div className="flex flex-wrap gap-2 mb-4">
        <div className="relative flex-1 min-w-[180px]">
          <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
          <Input
            value={search}
            onChange={(e) => { setSearch(e.target.value); setPage(1); }}
            placeholder="Search email or user_id"
            className="pl-9"
          />
        </div>
        <select
          value={planKey}
          onChange={(e) => { setPlanKey(e.target.value); setPage(1); }}
          className="rounded-md border border-slate-200 px-2 text-sm h-10"
          data-testid="admin-users-plan-filter"
        >
          <option value="">All plans</option>
          {PLAN_KEYS.map(k => <option key={k} value={k}>{PLAN_DISPLAY[k]}</option>)}
        </select>
        <Button variant="outline" size="sm" onClick={load} disabled={loading}>
          <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </div>

      <div className="bg-white rounded-2xl border border-slate-100 overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-slate-50 text-slate-600">
            <tr>
              <th className="text-left px-3 py-2">Email</th>
              <th className="text-left px-3 py-2">Plan</th>
              <th className="text-right px-3 py-2">IG</th>
              <th className="text-right px-3 py-2">Active rules</th>
              <th className="text-right px-3 py-2">Comments</th>
              <th className="text-right px-3 py-2">Replies</th>
              <th className="text-right px-3 py-2">DMs</th>
              <th className="text-left px-3 py-2">Status</th>
              <th className="text-left px-3 py-2">Created</th>
              <th className="text-right px-3 py-2"></th>
            </tr>
          </thead>
          <tbody>
            {(data?.items || []).map(u => {
              const exceeded = hasAnyExceeded(u.exceeded);
              return (
                <tr key={u.user_id} className="border-t border-slate-100">
                  <td className="px-3 py-2 font-mono">{u.email}</td>
                  <td className="px-3 py-2">
                    <Badge className="bg-slate-100 text-slate-700 border-0">
                      {PLAN_DISPLAY[u.plan_key] || u.plan_key}
                    </Badge>
                  </td>
                  <td className="px-3 py-2 text-right font-mono">{u.instagram_accounts_count}</td>
                  <td className="px-3 py-2 text-right font-mono">{u.active_automations_count}</td>
                  <td className="px-3 py-2 text-right font-mono">{u.current_month_usage?.comments_processed ?? 0}</td>
                  <td className="px-3 py-2 text-right font-mono">{u.current_month_usage?.public_replies_sent ?? 0}</td>
                  <td className="px-3 py-2 text-right font-mono">{u.current_month_usage?.dms_sent ?? 0}</td>
                  <td className="px-3 py-2">
                    {exceeded
                      ? <Badge className="bg-rose-100 text-rose-700 border-0"><AlertTriangle className="w-3 h-3 mr-1" />Limit</Badge>
                      : <Badge className="bg-emerald-100 text-emerald-700 border-0"><CheckCircle2 className="w-3 h-3 mr-1" />OK</Badge>}
                  </td>
                  <td className="px-3 py-2 text-xs text-slate-500">{formatTimestamp(u.created_at)}</td>
                  <td className="px-3 py-2 text-right">
                    <Button size="sm" variant="ghost" onClick={() => onSelect(u.user_id)}>
                      View
                    </Button>
                  </td>
                </tr>
              );
            })}
            {(!loading && (!data?.items || data.items.length === 0)) && (
              <tr><td className="px-3 py-6 text-center text-slate-500" colSpan={10}>No users.</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {data?.pagination && data.pagination.total_pages > 1 && (
        <div className="flex items-center justify-end gap-2 mt-3 text-sm">
          <span className="text-slate-500">
            Page {data.pagination.page} / {data.pagination.total_pages}
          </span>
          <Button variant="outline" size="sm" disabled={page <= 1}
                  onClick={() => setPage((p) => Math.max(1, p - 1))}>Prev</Button>
          <Button variant="outline" size="sm"
                  disabled={page >= data.pagination.total_pages}
                  onClick={() => setPage((p) => p + 1)}>Next</Button>
        </div>
      )}
    </div>
  );
}

function UserDetailTab({ userId, onBack }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [planKey, setPlanKey] = useState('');
  const [reason, setReason] = useState('');
  const [assigning, setAssigning] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const { data } = await api.get(`/admin/users/${encodeURIComponent(userId)}/detail`);
      setData(data);
      setPlanKey(data?.plan?.plan_key || 'free');
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Failed to load user';
      toast.error(typeof msg === 'string' ? msg : 'Failed to load user');
    } finally {
      setLoading(false);
    }
  }, [userId]);

  useEffect(() => { load(); }, [load]);

  const onAssign = useCallback(async () => {
    setAssigning(true);
    try {
      await api.post(`/admin/users/${encodeURIComponent(userId)}/plan`, {
        plan_key: planKey,
        reason: reason || 'manual_admin_assignment',
      });
      toast.success(`Plan set to ${PLAN_DISPLAY[planKey] || planKey}`);
      setReason('');
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Plan assignment failed';
      toast.error(typeof msg === 'string' ? msg : 'Plan assignment failed');
    } finally {
      setAssigning(false);
    }
  }, [userId, planKey, reason, load]);

  const onDisableAutomation = useCallback(async (automationId) => {
    if (!window.confirm(`Pause automation ${automationId}?`)) return;
    try {
      await api.post(`/admin/automations/${encodeURIComponent(automationId)}/disable`, {
        reason: 'admin_pause',
      });
      toast.success('Automation paused');
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Failed to pause';
      toast.error(typeof msg === 'string' ? msg : 'Failed to pause');
    }
  }, [load]);

  return (
    <div data-testid="admin-user-detail">
      <Button variant="ghost" size="sm" onClick={onBack} className="mb-4">
        <ArrowLeft className="w-4 h-4 mr-2" /> Back to users
      </Button>

      {loading && !data && (
        <div className="text-center py-12 text-slate-500">Loading…</div>
      )}

      {data && (
        <>
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
            <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold">User</div>
            <div className="mt-1 text-2xl font-bold text-slate-800 font-mono">{data.profile?.email}</div>
            <div className="text-xs text-slate-500 font-mono mt-1">{data.user_id}</div>
            <div className="text-xs text-slate-400 mt-1">
              Created: {formatTimestamp(data.profile?.created_at)}
            </div>
          </section>

          {/* Plan + assignment */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
            <div className="flex flex-wrap items-end justify-between gap-3 mb-3">
              <div>
                <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold">Plan</div>
                <div className="mt-1 text-2xl font-bold text-slate-800">
                  {data.plan?.display_name || PLAN_DISPLAY[data.plan?.plan_key] || data.plan?.plan_key}
                </div>
                <div className="text-xs text-slate-500 mt-1">
                  Billing: <span className="font-semibold">Not enabled yet</span>
                  {data.plan?.assignment_reason && (
                    <span className="ml-2 text-slate-400">· last reason: {data.plan.assignment_reason}</span>
                  )}
                </div>
              </div>
            </div>
            <div className="flex flex-wrap gap-2 items-center">
              <select
                value={planKey}
                onChange={(e) => setPlanKey(e.target.value)}
                className="rounded-md border border-slate-200 px-2 text-sm h-10"
                data-testid="admin-detail-plan-select"
              >
                {planOptions().map(o => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
              <Input
                placeholder="Reason (e.g. 'beta tester')"
                value={reason}
                onChange={(e) => setReason(e.target.value)}
                className="flex-1 min-w-[180px]"
              />
              <Button onClick={onAssign} disabled={assigning} data-testid="admin-detail-assign-btn">
                {assigning ? 'Saving…' : 'Assign plan'}
              </Button>
            </div>
          </section>

          {/* Usage */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
            <h3 className="text-sm font-semibold text-slate-700 mb-3">
              Usage — {data.usage_current_month?.event_month}
            </h3>
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              {Object.entries(data.usage_current_month?.counters || {}).map(([k, v]) => (
                <div key={k} className="rounded-xl border border-slate-100 p-3">
                  <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold">{k}</div>
                  <div className="mt-1 text-xl font-bold text-slate-800 font-mono">{v}</div>
                </div>
              ))}
            </div>
          </section>

          {/* Instagram accounts */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
            <h3 className="text-sm font-semibold text-slate-700 mb-3">Instagram accounts</h3>
            {data.instagram_accounts?.length === 0 && (
              <div className="text-sm text-slate-500">None connected.</div>
            )}
            <ul className="space-y-2 text-sm">
              {(data.instagram_accounts || []).map(a => (
                <li key={a.id} className="flex items-center justify-between border-b border-slate-100 pb-2 last:border-0">
                  <div>
                    <div className="font-mono">{a.username || a.instagram_account_id}</div>
                    <div className="text-xs text-slate-500">
                      {a.connectionValid ? 'Connected' : 'Disconnected'} ·
                      {' '}token: {a.tokenSource || '—'} · expires: {formatTimestamp(a.tokenExpiresAt)}
                    </div>
                  </div>
                  {a.active && <Badge className="bg-blue-100 text-blue-700 border-0">Active</Badge>}
                </li>
              ))}
            </ul>
          </section>

          {/* Automations */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
            <h3 className="text-sm font-semibold text-slate-700 mb-3">Automations</h3>
            {data.automations?.length === 0 && (
              <div className="text-sm text-slate-500">No automations.</div>
            )}
            <ul className="space-y-2 text-sm">
              {(data.automations || []).map(a => (
                <li key={a.automation_id} className="flex items-center justify-between border-b border-slate-100 pb-2 last:border-0">
                  <div>
                    <div className="font-semibold">{a.name || a.automation_id}</div>
                    <div className="text-xs text-slate-500">
                      {a.post_scope || 'unknown scope'}
                      {a.selected_media_id && <> · media <span className="font-mono">{a.selected_media_id}</span></>}
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    {a.active
                      ? <Badge className="bg-emerald-100 text-emerald-700 border-0">Active</Badge>
                      : <Badge className="bg-slate-100 text-slate-600 border-0">{a.status || 'paused'}</Badge>}
                    {a.active && (
                      <Button size="sm" variant="outline" onClick={() => onDisableAutomation(a.automation_id)}>
                        Pause
                      </Button>
                    )}
                  </div>
                </li>
              ))}
            </ul>
          </section>

          {/* Recent failures */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5">
            <h3 className="text-sm font-semibold text-slate-700 mb-3">Recent failures</h3>
            {data.recent_failures?.length === 0 && (
              <div className="text-sm text-slate-500">No recent failures.</div>
            )}
            <ul className="space-y-2 text-sm font-mono">
              {(data.recent_failures || []).map(f => (
                <li key={f.comment_id} className="border-b border-slate-100 pb-2 last:border-0">
                  <div>
                    <span className="text-slate-700">{f.action_status}</span>
                    {' · '}
                    <span className="text-xs text-slate-500">
                      reply={f.reply_status || '—'} · dm={f.dm_status || '—'}
                    </span>
                  </div>
                  <div className="text-xs text-slate-400">
                    ig_comment={f.ig_comment_id} · media={f.media_id} · attempts={f.attempts}
                    {(f.dm_failure_reason || f.reply_failure_reason) && (
                      <> · reason={f.dm_failure_reason || f.reply_failure_reason}</>
                    )}
                  </div>
                </li>
              ))}
            </ul>
          </section>
        </>
      )}
    </div>
  );
}

export default function AdminConsole() {
  const [me, setMe] = useState(null);   // null = loading
  const [overview, setOverview] = useState(null);
  const [overviewLoading, setOverviewLoading] = useState(false);
  const [tab, setTab] = useState('overview');
  const [selectedUserId, setSelectedUserId] = useState(null);

  // Probe admin gate.
  useEffect(() => {
    (async () => {
      try {
        const { data } = await api.get('/admin/me');
        setMe(data);
      } catch (_) {
        setMe({ is_admin: false });
      }
    })();
  }, []);

  const loadOverview = useCallback(async () => {
    setOverviewLoading(true);
    try {
      const { data } = await api.get('/admin/overview');
      setOverview(data);
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Failed to load overview';
      toast.error(typeof msg === 'string' ? msg : 'Failed to load overview');
    } finally {
      setOverviewLoading(false);
    }
  }, []);

  useEffect(() => {
    if (me?.is_admin && tab === 'overview' && !overview) loadOverview();
  }, [me, tab, overview, loadOverview]);

  if (me === null) {
    return <div className="p-6 text-slate-500">Checking admin access…</div>;
  }
  if (!me.is_admin) {
    return (
      <div className="p-6 max-w-3xl mx-auto" data-testid="admin-not-available">
        <div className="bg-white rounded-2xl border border-slate-100 p-6">
          <div className="flex items-center gap-2 text-rose-600 mb-2">
            <ShieldAlert className="w-5 h-5" />
            <h1 className="text-lg font-semibold">Not available</h1>
          </div>
          <p className="text-sm text-slate-600">
            This page is for the product owner. If you reached this by
            mistake, head back to the dashboard.
          </p>
        </div>
      </div>
    );
  }

  const onSelectUser = (uid) => { setSelectedUserId(uid); setTab('user-detail'); };
  const onBackToUsers = () => { setSelectedUserId(null); setTab('users'); };

  return (
    <div className="p-4 sm:p-6 max-w-6xl mx-auto" data-testid="admin-console">
      <div className="mb-6">
        <div className="flex items-center gap-2 text-amber-700 mb-1">
          <Lock className="w-4 h-4" />
          <span className="text-xs font-semibold uppercase tracking-wide">Owner console</span>
          <Badge className="bg-blue-100 text-blue-700 border-0 text-[10px]">admin</Badge>
        </div>
        <h1 className="text-3xl font-bold font-display">Admin</h1>
        <p className="text-slate-500 mt-1 text-sm">
          Monitor users, plans, usage, and failures. No payment controls
          here — billing is enabled later.
        </p>
      </div>

      <div className="flex gap-2 mb-4">
        <Button
          variant={tab === 'overview' ? 'default' : 'outline'}
          size="sm"
          onClick={() => { setTab('overview'); setSelectedUserId(null); }}
          data-testid="admin-tab-overview"
        >
          <Activity className="w-4 h-4 mr-2" /> Overview
        </Button>
        <Button
          variant={tab === 'users' || tab === 'user-detail' ? 'default' : 'outline'}
          size="sm"
          onClick={() => { setTab('users'); setSelectedUserId(null); }}
          data-testid="admin-tab-users"
        >
          <Users className="w-4 h-4 mr-2" /> Users
        </Button>
        <div className="ml-auto" />
        {tab === 'overview' && (
          <Button variant="outline" size="sm" onClick={loadOverview} disabled={overviewLoading}>
            <RefreshCw className={`w-4 h-4 mr-2 ${overviewLoading ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
        )}
      </div>

      {tab === 'overview' && <OverviewTab data={overview} loading={overviewLoading} onRefresh={loadOverview} />}
      {tab === 'users' && <UsersTab onSelect={onSelectUser} />}
      {tab === 'user-detail' && selectedUserId && (
        <UserDetailTab userId={selectedUserId} onBack={onBackToUsers} />
      )}
    </div>
  );
}

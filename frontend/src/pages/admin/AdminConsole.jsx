import React, { useEffect, useState, useCallback } from 'react';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Badge } from '../../components/ui/badge';
import {
  ShieldAlert, Users, Activity, RefreshCw, Search, ArrowLeft,
  CheckCircle2, AlertTriangle, Lock, UserCog, BarChart3,
} from 'lucide-react';
import api from '../../lib/api';
import { cachedApiGet, invalidateApiCache } from '../../lib/apiCache';
import { toast } from 'sonner';
import analytics from '../../lib/analytics';
import {
  PLAN_KEYS, PLAN_DISPLAY,
  hasAnyExceeded, planDistributionRows, planOptions, formatTimestamp,
} from '../../lib/admin';
import {
  ROLE_DISPLAY, hasPermission, canManageRole, roleOptionsAssignableBy,
  PERM_OVERVIEW_VIEW, PERM_USERS_VIEW, PERM_USERS_MANAGE, PERM_PLANS_ASSIGN,
  PERM_AUTOMATIONS_DISABLE, PERM_MEMBERS_VIEW, PERM_MEMBERS_MANAGE,
} from '../../lib/adminPermissions';

const ADMIN_CACHE_TTL_MS = 30000;

class AdminSectionErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false };
  }

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  componentDidCatch(error) {
    // Safe diagnostic only: no payload, tokens, request bodies, or user content.
    console.error('[AdminConsole] section render failed', {
      section: this.props.name || 'unknown',
      error: error?.name || 'RenderError',
    });
  }

  componentDidUpdate(prevProps) {
    if (prevProps.resetKey !== this.props.resetKey && this.state.hasError) {
      this.setState({ hasError: false });
    }
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="rounded-2xl border border-rose-100 bg-rose-50 p-4 text-sm text-rose-800"
             data-testid="admin-section-error">
          <div className="font-semibold">Could not render this user section</div>
          <div className="text-xs mt-1">section={this.props.name || 'unknown'}</div>
        </div>
      );
    }
    return this.props.children;
  }
}

function AdminErrorCard({ title = 'Could not load this section', onRetry, loading }) {
  return (
    <div className="rounded-2xl border border-amber-100 bg-amber-50 p-4 text-sm text-amber-900">
      <div className="font-semibold">{title}</div>
      {onRetry && (
        <Button variant="outline" size="sm" className="mt-3" onClick={onRetry} disabled={loading}>
          <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
          Retry
        </Button>
      )}
    </div>
  );
}

function AdminSkeleton({ rows = 3 }) {
  return (
    <div className="space-y-2" data-testid="admin-section-skeleton">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="h-12 rounded-xl bg-slate-100 animate-pulse" />
      ))}
    </div>
  );
}

function safeList(value) {
  return Array.isArray(value) ? value : [];
}

function invalidateAdminUserCaches(userId) {
  invalidateApiCache(`admin:user-detail:${userId}`);
  invalidateApiCache('admin:users');
  invalidateApiCache('admin:overview');
}

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
      const cacheKey = `admin:users:${page}:${pageSize}:${search.trim()}:${planKey || 'all'}`;
      const result = await cachedApiGet(
        cacheKey,
        () => api.get('/admin/users', { params }),
        { ttlMs: ADMIN_CACHE_TTL_MS },
      );
      setData(result.data);
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
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            invalidateApiCache('admin:users');
            load();
          }}
          disabled={loading}
        >
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

function UserDetailTab({ userId, onBack, me }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [planKey, setPlanKey] = useState('');
  const [reason, setReason] = useState('');
  const [assigning, setAssigning] = useState(false);
  const [loadError, setLoadError] = useState('');
  const canAssignPlan = hasPermission(me, PERM_PLANS_ASSIGN);
  const canDisableAutomation = hasPermission(me, PERM_AUTOMATIONS_DISABLE);
  const canManageUsers = hasPermission(me, PERM_USERS_MANAGE);
  const canDeleteUsers = hasPermission(me, 'admin.owner.manage');
  // Allowance form state
  const [allowanceType, setAllowanceType] = useState('additive_allowance');
  const [allowanceCommentsExtra, setAllowanceCommentsExtra] = useState('');
  const [allowanceDmsExtra, setAllowanceDmsExtra] = useState('');
  const [allowanceRepliesExtra, setAllowanceRepliesExtra] = useState('');
  const [allowanceDays, setAllowanceDays] = useState('30');
  const [allowanceReason, setAllowanceReason] = useState('');
  const [grantBusy, setGrantBusy] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    setLoadError('');
    try {
      const cacheKey = `admin:user-detail:${userId}`;
      const result = await cachedApiGet(
        cacheKey,
        () => api.get(`/admin/users/${encodeURIComponent(userId)}/detail`),
        { ttlMs: ADMIN_CACHE_TTL_MS },
      );
      setData(result.data);
      setPlanKey(result.data?.plan?.plan_key || 'free');
      if (result.stale) setLoadError('Showing cached user detail. Refresh failed.');
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Failed to load user';
      setLoadError(typeof msg === 'string' ? msg : 'Failed to load user');
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
      invalidateAdminUserCaches(userId);
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Plan assignment failed';
      toast.error(typeof msg === 'string' ? msg : 'Plan assignment failed');
    } finally {
      setAssigning(false);
    }
  }, [userId, planKey, reason, load]);

  const onCreateAllowance = useCallback(async () => {
    const metrics = {};
    if (parseInt(allowanceCommentsExtra, 10) > 0)
      metrics.comments_processed_extra = parseInt(allowanceCommentsExtra, 10);
    if (parseInt(allowanceDmsExtra, 10) > 0)
      metrics.dms_sent_extra = parseInt(allowanceDmsExtra, 10);
    if (parseInt(allowanceRepliesExtra, 10) > 0)
      metrics.public_replies_sent_extra = parseInt(allowanceRepliesExtra, 10);
    if (Object.keys(metrics).length === 0) {
      toast.error('Enter at least one numeric allowance');
      return;
    }
    setGrantBusy(true);
    try {
      const days = parseInt(allowanceDays, 10);
      const ends_at = days > 0 ? new Date(Date.now() + days * 24 * 3600 * 1000).toISOString() : null;
      await api.post(`/admin/users/${encodeURIComponent(userId)}/limit-overrides`, {
        type: allowanceType, metrics,
        starts_at: new Date().toISOString(), ends_at,
        reason: allowanceReason || 'manual_admin_grant',
      });
      toast.success('Allowance granted');
      setAllowanceCommentsExtra('');
      setAllowanceDmsExtra('');
      setAllowanceRepliesExtra('');
      setAllowanceReason('');
      invalidateAdminUserCaches(userId);
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Failed to grant';
      toast.error(typeof msg === 'string' ? msg : 'Failed to grant');
    } finally {
      setGrantBusy(false);
    }
  }, [userId, allowanceType, allowanceCommentsExtra, allowanceDmsExtra,
      allowanceRepliesExtra, allowanceDays, allowanceReason, load]);

  const onRevokeAllowance = useCallback(async (overrideId) => {
    if (!window.confirm('Revoke this allowance now?')) return;
    try {
      await api.patch(
        `/admin/users/${encodeURIComponent(userId)}/limit-overrides/${encodeURIComponent(overrideId)}/revoke`,
        { reason: 'admin_revoke' },
      );
      toast.success('Allowance revoked');
      invalidateAdminUserCaches(userId);
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Failed to revoke';
      toast.error(typeof msg === 'string' ? msg : 'Failed to revoke');
    }
  }, [userId, load]);

  const onSuspend = useCallback(async () => {
    const reasonText = window.prompt('Suspend reason (optional):', '') || '';
    try {
      await api.post(`/admin/users/${encodeURIComponent(userId)}/suspend`, { reason: reasonText });
      toast.success('User suspended');
      invalidateAdminUserCaches(userId);
      await load();
    } catch (err) {
      toast.error(err?.response?.data?.detail || 'Failed to suspend');
    }
  }, [userId, load]);

  const onUnsuspend = useCallback(async () => {
    try {
      await api.post(`/admin/users/${encodeURIComponent(userId)}/unsuspend`, {});
      toast.success('User reactivated');
      invalidateAdminUserCaches(userId);
      await load();
    } catch (err) {
      toast.error(err?.response?.data?.detail || 'Failed to unsuspend');
    }
  }, [userId, load]);

  const onSoftDelete = useCallback(async () => {
    const reasonText = window.prompt(
      'SOFT DELETE this user? This pauses all automations, disconnects IG accounts, and blocks login. Type a reason:',
      '',
    );
    if (reasonText === null) return;
    if (!window.confirm(`Confirm soft-delete of ${userId}? This cannot be undone via UI.`)) return;
    try {
      await api.post(`/admin/users/${encodeURIComponent(userId)}/delete`, { reason: reasonText });
      toast.success('User soft-deleted');
      invalidateAdminUserCaches(userId);
      await load();
    } catch (err) {
      toast.error(err?.response?.data?.detail || 'Failed to delete');
    }
  }, [userId, load]);

  const onDisableAutomation = useCallback(async (automationId) => {
    if (!window.confirm(`Pause automation ${automationId}?`)) return;
    try {
      await api.post(`/admin/automations/${encodeURIComponent(automationId)}/disable`, {
        reason: 'admin_pause',
      });
      toast.success('Automation paused');
      invalidateAdminUserCaches(userId);
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Failed to pause';
      toast.error(typeof msg === 'string' ? msg : 'Failed to pause');
    }
  }, [userId, load]);

  const profile = data?.profile || {};
  const plan = data?.plan || {};
  const usage = data?.usage_current_month || {};
  const usageCounters = usage.counters || {};
  const activeOverrides = safeList(data?.active_overrides);
  const instagramAccounts = safeList(data?.instagram_accounts);
  const automations = safeList(data?.automations);
  const recentFailures = safeList(data?.recent_failures);

  return (
    <div data-testid="admin-user-detail">
      <Button variant="ghost" size="sm" onClick={onBack} className="mb-4">
        <ArrowLeft className="w-4 h-4 mr-2" /> Back to users
      </Button>

      {loading && !data && (
        <div className="text-center py-12 text-slate-500">Loading…</div>
      )}

      {loadError && (
        <div className="mb-4">
          <AdminErrorCard
            title={loadError}
            onRetry={() => {
              invalidateApiCache(`admin:user-detail:${userId}`);
              load();
            }}
            loading={loading}
          />
        </div>
      )}

      {data && (
        <>
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
            <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold">User</div>
            <div className="mt-1 text-2xl font-bold text-slate-800 font-mono">{profile.email || 'Unknown user'}</div>
            <div className="text-xs text-slate-500 font-mono mt-1">{data.user_id || userId}</div>
            <div className="text-xs text-slate-400 mt-1">
              Created: {formatTimestamp(profile.created_at)}
            </div>
          </section>

          {/* Plan + assignment */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
            <div className="flex flex-wrap items-end justify-between gap-3 mb-3">
              <div>
                <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold">Plan</div>
                <div className="mt-1 text-2xl font-bold text-slate-800">
                  {plan.display_name || PLAN_DISPLAY[plan.plan_key] || plan.plan_key || 'Free'}
                </div>
                <div className="text-xs text-slate-500 mt-1">
                  Billing: <span className="font-semibold">Not enabled yet</span>
                  {plan.assignment_reason && (
                    <span className="ml-2 text-slate-400">· last reason: {plan.assignment_reason}</span>
                  )}
                </div>
              </div>
            </div>
            {canAssignPlan ? (
              <div className="flex flex-wrap gap-2 items-center" data-testid="admin-detail-plan-controls">
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
            ) : (
              <div className="text-xs text-slate-500" data-testid="admin-detail-plan-readonly">
                You don't have permission to change this user's plan.
              </div>
            )}
          </section>

          {/* Status + suspend/delete */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4" data-testid="admin-user-status">
            <div className="flex flex-wrap items-center justify-between gap-2 mb-2">
              <div>
                <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold">Status</div>
                <div className="mt-1 text-base font-semibold">
                  {profile.status === 'suspended'
                    ? <Badge className="bg-amber-100 text-amber-800 border-0">Suspended</Badge>
                    : profile.status === 'deleted'
                      ? <Badge className="bg-rose-100 text-rose-700 border-0">Deleted</Badge>
                      : <Badge className="bg-emerald-100 text-emerald-700 border-0">Active</Badge>}
                  {profile.google_linked && (
                    <Badge className="ml-2 bg-blue-100 text-blue-700 border-0">Google linked</Badge>
                  )}
                </div>
              </div>
              <div className="flex gap-2">
                {canManageUsers && (profile.status || 'active') === 'active' && (
                  <Button size="sm" variant="outline" onClick={onSuspend} data-testid="admin-suspend-btn">
                    Suspend
                  </Button>
                )}
                {canManageUsers && profile.status === 'suspended' && (
                  <Button size="sm" variant="outline" onClick={onUnsuspend} data-testid="admin-unsuspend-btn">
                    Unsuspend
                  </Button>
                )}
                {canDeleteUsers && profile.status !== 'deleted' && (
                  <Button size="sm" variant="outline"
                          className="text-rose-700 border-rose-200 hover:bg-rose-50"
                          onClick={onSoftDelete} data-testid="admin-soft-delete-btn">
                    Soft delete
                  </Button>
                )}
              </div>
            </div>
            {profile.suspended_at && (
              <div className="text-xs text-slate-500">
                Suspended at {formatTimestamp(profile.suspended_at)}
                {profile.suspended_by && <> by <span className="font-mono">{profile.suspended_by}</span></>}
              </div>
            )}
            {profile.deleted_at && (
              <div className="text-xs text-slate-500">
                Deleted at {formatTimestamp(profile.deleted_at)}
                {profile.deleted_by && <> by <span className="font-mono">{profile.deleted_by}</span></>}
              </div>
            )}
          </section>

          {/* Active allowances + grant form */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4" data-testid="admin-allowances">
            <h3 className="text-sm font-semibold text-slate-700 mb-3">Custom allowances</h3>
            {activeOverrides.length === 0 && (
              <div className="text-sm text-slate-500 mb-3">No active grants.</div>
            )}
            {activeOverrides.length > 0 && (
              <ul className="space-y-2 text-sm mb-3">
                {activeOverrides.map((ov) => (
                  <li key={ov.id} className="flex items-start justify-between border-b border-slate-100 pb-2 last:border-0">
                    <div>
                      <div className="font-semibold">
                        {ov.grant_name || ov.type}
                        <Badge className="ml-2 bg-slate-100 text-slate-700 border-0 text-[10px]">{ov.type}</Badge>
                      </div>
                      <div className="text-xs text-slate-500 font-mono mt-1">
                        {Object.entries(ov.metrics || {}).map(([k, v]) => (
                          <span key={k} className="mr-3">{k}: +{v}</span>
                        ))}
                      </div>
                      <div className="text-xs text-slate-400 mt-1">
                        starts {formatTimestamp(ov.starts_at)}
                        {ov.ends_at && <> · ends {formatTimestamp(ov.ends_at)}</>}
                        {ov.created_by_email && <> · by {ov.created_by_email}</>}
                      </div>
                    </div>
                    {canAssignPlan && (
                      <Button size="sm" variant="ghost" onClick={() => onRevokeAllowance(ov.id)}>
                        Revoke
                      </Button>
                    )}
                  </li>
                ))}
              </ul>
            )}
            {canAssignPlan && (
              <div className="border-t border-slate-100 pt-3">
                <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold mb-2">
                  Grant new allowance
                </div>
                <div className="grid gap-2 sm:grid-cols-2">
                  <select value={allowanceType} onChange={(e) => setAllowanceType(e.target.value)}
                          className="rounded-md border border-slate-200 px-2 text-sm h-10"
                          data-testid="admin-allowance-type-select">
                    <option value="additive_allowance">Additive (extras on top of plan)</option>
                    <option value="trial_grant">Trial grant (extras with end date)</option>
                  </select>
                  <Input type="number" min="0" placeholder="Days valid (0 = no end)"
                         value={allowanceDays} onChange={(e) => setAllowanceDays(e.target.value)} />
                  <Input type="number" min="0" placeholder="Extra comments processed"
                         value={allowanceCommentsExtra}
                         onChange={(e) => setAllowanceCommentsExtra(e.target.value)} />
                  <Input type="number" min="0" placeholder="Extra DMs sent"
                         value={allowanceDmsExtra}
                         onChange={(e) => setAllowanceDmsExtra(e.target.value)} />
                  <Input type="number" min="0" placeholder="Extra public replies"
                         value={allowanceRepliesExtra}
                         onChange={(e) => setAllowanceRepliesExtra(e.target.value)} />
                  <Input placeholder="Reason / grant name (optional)"
                         value={allowanceReason}
                         onChange={(e) => setAllowanceReason(e.target.value)} />
                </div>
                <div className="mt-2">
                  <Button onClick={onCreateAllowance} disabled={grantBusy}
                          data-testid="admin-grant-allowance-btn">
                    {grantBusy ? 'Granting…' : 'Grant allowance'}
                  </Button>
                </div>
              </div>
            )}
          </section>

          {/* Usage */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
            <h3 className="text-sm font-semibold text-slate-700 mb-3">
              Usage — {usage.event_month || 'current month'}
            </h3>
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
              {Object.entries(usageCounters).map(([k, v]) => (
                <div key={k} className="rounded-xl border border-slate-100 p-3">
                  <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold">{k}</div>
                  <div className="mt-1 text-xl font-bold text-slate-800 font-mono">{v}</div>
                </div>
              ))}
              {Object.keys(usageCounters).length === 0 && (
                <div className="text-sm text-slate-500">No usage recorded this month.</div>
              )}
            </div>
          </section>

          {/* Instagram accounts */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
            <h3 className="text-sm font-semibold text-slate-700 mb-3">Instagram accounts</h3>
            {instagramAccounts.length === 0 && (
              <div className="text-sm text-slate-500">None connected.</div>
            )}
            <ul className="space-y-2 text-sm">
              {instagramAccounts.map(a => (
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
            {automations.length === 0 && (
              <div className="text-sm text-slate-500">No automations.</div>
            )}
            <ul className="space-y-2 text-sm">
              {automations.map(a => (
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
                    {a.active && canDisableAutomation && (
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
            {recentFailures.length === 0 && (
              <div className="text-sm text-slate-500">No recent failures.</div>
            )}
            <ul className="space-y-2 text-sm font-mono">
              {recentFailures.map(f => (
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

function AdminsTab({ me }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [addEmail, setAddEmail] = useState('');
  const [addRole, setAddRole] = useState('viewer');
  const [addReason, setAddReason] = useState('');
  const [submitting, setSubmitting] = useState(false);

  const canManage = hasPermission(me, PERM_MEMBERS_MANAGE);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const result = await cachedApiGet(
        'admin:members',
        () => api.get('/admin/members'),
        { ttlMs: ADMIN_CACHE_TTL_MS },
      );
      setData(result.data);
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Failed to load members';
      toast.error(typeof msg === 'string' ? msg : 'Failed to load members');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const onAdd = useCallback(async (e) => {
    e?.preventDefault?.();
    if (!addEmail.trim()) return;
    setSubmitting(true);
    try {
      await api.post('/admin/members', {
        email: addEmail.trim().toLowerCase(),
        role: addRole,
        reason: addReason || 'manual_admin_assignment',
      });
      toast.success(`Added ${addEmail} as ${ROLE_DISPLAY[addRole]}`);
      setAddEmail(''); setAddReason(''); setAddRole('viewer');
      invalidateApiCache('admin:members');
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Failed to add member';
      toast.error(typeof msg === 'string' ? msg : 'Failed to add member');
    } finally {
      setSubmitting(false);
    }
  }, [addEmail, addRole, addReason, load]);

  const onChangeRole = useCallback(async (member, newRole) => {
    try {
      await api.patch(`/admin/members/${encodeURIComponent(member.user_id)}`, {
        role: newRole, reason: 'admin_role_change',
      });
      toast.success(`Role changed to ${ROLE_DISPLAY[newRole]}`);
      invalidateApiCache('admin:members');
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Failed to change role';
      toast.error(typeof msg === 'string' ? msg : 'Failed to change role');
    }
  }, [load]);

  const onRemove = useCallback(async (member) => {
    if (!window.confirm(`Remove ${member.email}?`)) return;
    try {
      await api.delete(`/admin/members/${encodeURIComponent(member.user_id)}`);
      toast.success('Member removed');
      invalidateApiCache('admin:members');
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Failed to remove member';
      toast.error(typeof msg === 'string' ? msg : 'Failed to remove member');
    }
  }, [load]);

  const myRole = me?.role;
  const assignableOptions = roleOptionsAssignableBy(myRole);

  return (
    <div data-testid="admin-admins">
      <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
        <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold mb-1">
          You are
        </div>
        <div className="text-base font-semibold text-slate-800">
          {ROLE_DISPLAY[myRole] || myRole}
          {me?.bootstrap_owner && (
            <span className="ml-2 text-xs text-blue-700">(bootstrap owner via ADMIN_EMAILS)</span>
          )}
        </div>
        <div className="text-xs text-slate-400 mt-1">{(me?.permissions || []).length} permissions</div>
      </section>

      {canManage && (
        <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4" data-testid="admin-add-member">
          <h3 className="text-sm font-semibold text-slate-700 mb-3">Add admin member</h3>
          <form className="flex flex-wrap gap-2 items-center" onSubmit={onAdd}>
            <Input
              placeholder="email@example.com"
              value={addEmail}
              onChange={(e) => setAddEmail(e.target.value)}
              className="flex-1 min-w-[200px]"
              type="email"
            />
            <select
              value={addRole}
              onChange={(e) => setAddRole(e.target.value)}
              className="rounded-md border border-slate-200 px-2 text-sm h-10"
              data-testid="admin-add-role-select"
            >
              {assignableOptions.map(o => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>
            <Input
              placeholder="Reason (optional)"
              value={addReason}
              onChange={(e) => setAddReason(e.target.value)}
              className="flex-1 min-w-[160px]"
            />
            <Button type="submit" disabled={submitting || !addEmail.trim()} data-testid="admin-add-member-btn">
              {submitting ? 'Adding…' : 'Add member'}
            </Button>
          </form>
          <p className="text-xs text-slate-400 mt-2">
            User must already have an account. No invitation email is sent.
          </p>
        </section>
      )}

      <div className="bg-white rounded-2xl border border-slate-100 overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-slate-50 text-slate-600">
            <tr>
              <th className="text-left px-3 py-2">Email</th>
              <th className="text-left px-3 py-2">Role</th>
              <th className="text-left px-3 py-2">Status</th>
              <th className="text-left px-3 py-2">Added by</th>
              <th className="text-left px-3 py-2">Created</th>
              <th className="text-right px-3 py-2"></th>
            </tr>
          </thead>
          <tbody>
            {(data?.items || []).map(m => {
              const canMutate = canManage && canManageRole(myRole, m.role);
              return (
                <tr key={m.user_id || m.email} className="border-t border-slate-100">
                  <td className="px-3 py-2 font-mono">
                    {m.email}
                    {m.bootstrap_owner && <span className="ml-2 text-[10px] text-blue-700">bootstrap</span>}
                  </td>
                  <td className="px-3 py-2">
                    <Badge className="bg-slate-100 text-slate-700 border-0">
                      {ROLE_DISPLAY[m.role] || m.role}
                    </Badge>
                  </td>
                  <td className="px-3 py-2">
                    {m.disabled_at
                      ? <Badge className="bg-rose-100 text-rose-700 border-0">Disabled</Badge>
                      : <Badge className="bg-emerald-100 text-emerald-700 border-0">Active</Badge>}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs">{m.added_by_email || '—'}</td>
                  <td className="px-3 py-2 text-xs text-slate-500">{formatTimestamp(m.created_at)}</td>
                  <td className="px-3 py-2 text-right">
                    {canMutate && !m.disabled_at && (
                      <span className="inline-flex gap-1">
                        <select
                          value={m.role}
                          onChange={(e) => onChangeRole(m, e.target.value)}
                          className="rounded-md border border-slate-200 px-2 text-xs h-8"
                          data-testid={`admin-member-role-${m.user_id}`}
                        >
                          {roleOptionsAssignableBy(myRole).map(o => (
                            <option key={o.value} value={o.value}>{o.label}</option>
                          ))}
                          {/* Always show current role as an option even if not assignable. */}
                          {!roleOptionsAssignableBy(myRole).some(o => o.value === m.role) && (
                            <option value={m.role}>{ROLE_DISPLAY[m.role] || m.role}</option>
                          )}
                        </select>
                        <Button size="sm" variant="ghost" onClick={() => onRemove(m)}>
                          Remove
                        </Button>
                      </span>
                    )}
                  </td>
                </tr>
              );
            })}
            {(!loading && (!data?.items || data.items.length === 0)) && (
              <tr><td className="px-3 py-6 text-center text-slate-500" colSpan={6}>No members.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}


function ReconciliationTab() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const result = await cachedApiGet(
        'admin:metrics:reconciliation',
        () => api.get('/admin/metrics/reconciliation'),
        { ttlMs: ADMIN_CACHE_TTL_MS },
      );
      setData(result.data);
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Failed to load reconciliation';
      toast.error(typeof msg === 'string' ? msg : 'Failed to load reconciliation');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  return (
    <div data-testid="admin-reconciliation">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-semibold text-slate-700">
          Metrics reconciliation
          {data?.event_month && <span className="text-slate-400 font-mono ml-2">{data.event_month}</span>}
        </h3>
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            invalidateApiCache('admin:metrics');
            load();
          }}
          disabled={loading}
        >
          <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </div>
      {data && data.mismatch_count > 0 && (
        <div className="mb-3 rounded-2xl border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900"
             data-testid="reconciliation-mismatch-banner">
          <AlertTriangle className="inline w-4 h-4 mr-1" />
          {data.mismatch_count} mismatch(es) detected. Review the table below.
        </div>
      )}
      <div className="bg-white rounded-2xl border border-slate-100 overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-slate-50 text-slate-600">
            <tr>
              <th className="text-left px-3 py-2">Metric</th>
              <th className="text-right px-3 py-2">Dashboard</th>
              <th className="text-right px-3 py-2">Recomputed</th>
              <th className="text-right px-3 py-2">Δ</th>
              <th className="text-left px-3 py-2">Status</th>
              <th className="text-left px-3 py-2">Source</th>
            </tr>
          </thead>
          <tbody>
            {(data?.items || []).map((row) => (
              <tr key={row.metric_name} className="border-t border-slate-100">
                <td className="px-3 py-2 font-mono text-xs">{row.metric_name}</td>
                <td className="px-3 py-2 text-right font-mono">{row.dashboard_value}</td>
                <td className="px-3 py-2 text-right font-mono">{row.recomputed_value}</td>
                <td className="px-3 py-2 text-right font-mono">{row.difference ?? '—'}</td>
                <td className="px-3 py-2">
                  {row.status === 'ok'
                    ? <Badge className="bg-emerald-100 text-emerald-700 border-0">OK</Badge>
                    : <Badge className="bg-amber-100 text-amber-800 border-0">Mismatch</Badge>}
                </td>
                <td className="px-3 py-2 text-xs text-slate-500">{row.source}</td>
              </tr>
            ))}
            {(!loading && (!data?.items || data.items.length === 0)) && (
              <tr><td className="px-3 py-6 text-center text-slate-500" colSpan={6}>No data.</td></tr>
            )}
          </tbody>
        </table>
      </div>
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
        if (data?.is_admin) analytics.capture('admin_console_viewed', {});
      } catch (_) {
        setMe({ is_admin: false });
      }
    })();
  }, []);

  const loadOverview = useCallback(async () => {
    setOverviewLoading(true);
    try {
      const result = await cachedApiGet(
        'admin:overview',
        () => api.get('/admin/overview'),
        { ttlMs: ADMIN_CACHE_TTL_MS },
      );
      setOverview(result.data);
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

  const canViewOverview = hasPermission(me, PERM_OVERVIEW_VIEW);
  const canViewUsers = hasPermission(me, PERM_USERS_VIEW);
  const canViewMembers = hasPermission(me, PERM_MEMBERS_VIEW);
  const canViewMetrics = hasPermission(me, 'admin.audit.view');

  return (
    <div className="p-4 sm:p-6 max-w-6xl mx-auto" data-testid="admin-console">
      <div className="mb-6">
        <div className="flex items-center gap-2 text-amber-700 mb-1">
          <Lock className="w-4 h-4" />
          <span className="text-xs font-semibold uppercase tracking-wide">Owner console</span>
          <Badge className="bg-blue-100 text-blue-700 border-0 text-[10px]" data-testid="admin-current-role">
            {ROLE_DISPLAY[me?.role] || me?.role}
          </Badge>
        </div>
        <h1 className="text-3xl font-bold font-display">Admin</h1>
        <p className="text-slate-500 mt-1 text-sm">
          Monitor users, plans, usage, and failures. No payment controls
          here — billing is enabled later.
        </p>
      </div>

      <div className="flex gap-2 mb-4 flex-wrap">
        {canViewOverview && (
          <Button
            variant={tab === 'overview' ? 'default' : 'outline'}
            size="sm"
            onClick={() => { setTab('overview'); setSelectedUserId(null); }}
            data-testid="admin-tab-overview"
          >
            <Activity className="w-4 h-4 mr-2" /> Overview
          </Button>
        )}
        {canViewUsers && (
          <Button
            variant={tab === 'users' || tab === 'user-detail' ? 'default' : 'outline'}
            size="sm"
            onClick={() => { setTab('users'); setSelectedUserId(null); }}
            data-testid="admin-tab-users"
          >
            <Users className="w-4 h-4 mr-2" /> Users
          </Button>
        )}
        {canViewMembers && (
          <Button
            variant={tab === 'admins' ? 'default' : 'outline'}
            size="sm"
            onClick={() => { setTab('admins'); setSelectedUserId(null); }}
            data-testid="admin-tab-admins"
          >
            <UserCog className="w-4 h-4 mr-2" /> Admins
          </Button>
        )}
        {canViewMetrics && (
          <Button
            variant={tab === 'metrics' ? 'default' : 'outline'}
            size="sm"
            onClick={() => { setTab('metrics'); setSelectedUserId(null); }}
            data-testid="admin-tab-metrics"
          >
            <BarChart3 className="w-4 h-4 mr-2" /> Metrics
          </Button>
        )}
        <div className="ml-auto" />
        {tab === 'overview' && canViewOverview && (
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              invalidateApiCache('admin:overview');
              loadOverview();
            }}
            disabled={overviewLoading}
          >
            <RefreshCw className={`w-4 h-4 mr-2 ${overviewLoading ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
        )}
      </div>

      {tab === 'overview' && canViewOverview && (
        <OverviewTab data={overview} loading={overviewLoading} onRefresh={loadOverview} />
      )}
      {tab === 'users' && canViewUsers && <UsersTab onSelect={onSelectUser} />}
      {tab === 'user-detail' && selectedUserId && canViewUsers && (
        <AdminSectionErrorBoundary name="user-detail" resetKey={selectedUserId}>
          <UserDetailTab userId={selectedUserId} onBack={onBackToUsers} me={me} />
        </AdminSectionErrorBoundary>
      )}
      {tab === 'admins' && canViewMembers && <AdminsTab me={me} />}
      {tab === 'metrics' && canViewMetrics && <ReconciliationTab />}
    </div>
  );
}

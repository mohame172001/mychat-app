import React, { useEffect, useState, useCallback } from 'react';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Badge } from '../../components/ui/badge';
import {
  ShieldAlert, Users, Activity, RefreshCw, Search, ArrowLeft,
  CheckCircle2, AlertTriangle, Lock, UserCog, BarChart3,
  ScrollText, Inbox, RotateCcw,
} from 'lucide-react';
import api from '../../lib/api';
import { cachedApiGetSWR, invalidateApiCache } from '../../lib/apiCache';
import { toast } from 'sonner';
import analytics from '../../lib/analytics';
import { fetchAdminMe } from '../../lib/useIsAdmin';
import { describeDmFailureReason, dmFailureToneClasses } from '../../lib/dmFailureReasons';
import { useAuth } from '../../context/AuthContext';
import {
  PLAN_KEYS, PLAN_DISPLAY,
  hasAnyExceeded, planDistributionRows, planOptions, formatTimestamp,
} from '../../lib/admin';
import {
  ROLE_DISPLAY, hasPermission, canManageRole, roleOptionsAssignableBy,
  PERM_OVERVIEW_VIEW, PERM_USERS_VIEW, PERM_USERS_MANAGE, PERM_PLANS_ASSIGN,
  PERM_AUTOMATIONS_DISABLE, PERM_MEMBERS_VIEW, PERM_MEMBERS_MANAGE,
  PERM_FAILURES_VIEW,
} from '../../lib/adminPermissions';
import { useTranslation } from '../../lib/i18n';

function isAr() {
  try {
    if (typeof document !== 'undefined' && document.documentElement?.lang === 'ar') return true;
    if (typeof localStorage !== 'undefined' && localStorage.getItem('mychat_lang') === 'ar') return true;
  } catch (_) { /* ignore */ }
  return false;
}

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
          <div className="font-semibold">{isAr() ? "تعذّر عرض هذا القسم" : "Could not render this user section"}</div>
          <div className="text-xs mt-1">section={this.props.name || 'unknown'}</div>
        </div>
      );
    }
    return this.props.children;
  }
}

function AdminErrorCard({ title = (isAr() ? 'تعذّر تحميل هذا القسم' : 'Could not load this section'), onRetry, loading }) {
  return (
    <div className="rounded-2xl border border-amber-100 bg-amber-50 p-4 text-sm text-amber-900">
      <div className="font-semibold">{title}</div>
      {onRetry && (
        <Button variant="outline" size="sm" className="mt-3" onClick={onRetry} disabled={loading}>
          <RefreshCw className={`w-4 h-4 me-2 ${loading ? 'animate-spin' : ''}`} />
          {isAr() ? "إعادة المحاولة" : "Retry"}
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
  invalidateApiCache('admin:user-detail');
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
  const [reclassifying, setReclassifying] = useState(false);

  const runReclassifier = async () => {
    setReclassifying(true);
    try {
      const { data: res } = await api.post('/admin/comments/reclassify-collab-failures?limit=300');
      const s = res.summary || {};
      const moved = (s.reclassified_as_collab_skip || 0)
        + (s.reclassified_as_unauthorized_skip || 0)
        + (s.reclassified_as_gone_skip || 0);
      toast.success(
        moved > 0
          ? (isAr()
              ? `أُعيد تصنيف ${moved} سجلّ من سلّة الأعطال`
              : `Reclassifier moved ${moved} row(s) out of the failure bucket`)
          : (isAr()
              ? `تم فحص ${s.scanned || 0} سجلّ — لا شيء لإعادة تصنيفه`
              : `Reclassifier scanned ${s.scanned || 0} row(s) — nothing to move`),
        { duration: 6000 },
      );
      if (onRefresh) onRefresh();
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'فشل إعادة التصنيف' : 'Reclassifier failed');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'فشل إعادة التصنيف' : 'Reclassifier failed'));
    } finally {
      setReclassifying(false);
    }
  };

  if (!data) return null;
  const totals = data.current_month_usage_totals || {};
  return (
    <div data-testid="admin-overview">
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4 mb-6">
        <StatCard label={isAr() ? "إجمالي المستخدمين" : "Total users"} value={data.total_users} hint={isAr() ? `${data.users_created_7d || 0} جديد خلال 7 أيام` : `${data.users_created_7d || 0} new in 7d`} />
        <StatCard label={isAr() ? "حسابات Instagram المربوطة" : "Connected IG accounts"} value={data.connected_instagram_accounts} hint={isAr() ? `من إجمالي ${data.total_instagram_accounts}` : `of ${data.total_instagram_accounts} total`} />
        <StatCard label={isAr() ? "الأتمتات النشطة" : "Active automations"} value={data.active_automations} hint={isAr() ? `من إجمالي ${data.total_automations}` : `of ${data.total_automations} total`} />
        <StatCard label={isAr() ? "متجاوزون لحدّ التعليقات" : "Plan limited (comments)"} value={data.plan_limited_counts} />
        <StatCard label={isAr() ? "تعليقات هذا الشهر" : "Comments this month"} value={totals.comments_processed || 0} />
        <StatCard label={isAr() ? "ردود علنية هذا الشهر" : "Public replies this month"} value={totals.public_replies_sent || 0} />
        <StatCard label={isAr() ? "رسائل هذا الشهر" : "DMs this month"} value={totals.dms_sent || 0} />
        <StatCard label={isAr() ? "نقرات الروابط هذا الشهر" : "Link clicks this month"} value={totals.links_clicked || 0} />
        <StatCard label={isAr() ? "أعطال قابلة لإعادة المحاولة" : "Retryable failures"} value={data.retryable_failure_counts || 0} />
        <StatCard label={isAr() ? "أعطال دائمة" : "Permanent failures"} value={data.permanent_failure_counts || 0} />
        <StatCard label={isAr() ? "في الطابور" : "Queue pending"} value={(data.queue_health || {}).pending || 0} />
        <StatCard label={isAr() ? "مستخدمون جدد اليوم" : "New users today"} value={data.users_created_today || 0} />
      </div>

      {/* Operator tools — sweep collab/unauthorized comments out of the
          failure bucket on demand. The background loop also does this
          every 15 min, but the button is here for instant feedback. */}
      <div className="mb-6 flex items-center justify-between bg-white rounded-2xl border border-slate-100 px-5 py-4 gap-4 flex-wrap">
        <div className="text-sm">
          <div className="font-semibold text-slate-700">{isAr() ? "إعادة تصنيف أعطال الـCollab" : "Reclassify collab failures"}</div>
          <p className="text-xs text-slate-500 mt-0.5">
            {isAr() ? "يفحص الأعطال الدائمة/الجزئية الحديثة للتعليقات على المنشورات" : "Scans recent permanent/partial failures for comments on posts"}
            you don't own and moves them out of the failure count.
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={runReclassifier} disabled={reclassifying}>
          <RefreshCw className={`w-4 h-4 me-2 ${reclassifying ? 'animate-spin' : ''}`} />
          {isAr() ? "تشغيل الآن" : "Run now"}
        </Button>
      </div>

      <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-6" data-testid="plan-distribution">
        <h3 className="text-sm font-semibold text-slate-700 mb-3">{isAr() ? "توزيع الخطط" : "Plan distribution"}</h3>
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
  const { user } = useAuth();
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
      const cacheKey = `admin:users:${user?.id || 'anon'}:${page}:${pageSize}:${search.trim()}:${planKey || 'all'}`;
      const result = await cachedApiGetSWR(
        cacheKey,
        () => api.get('/admin/users', { params }),
        { ttlMs: ADMIN_CACHE_TTL_MS, maxStaleMs: 2 * 60 * 1000, persist: true, onUpdate: setData },
      );
      setData(result.data);
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'تعذّر تحميل المستخدمين' : 'Failed to load users');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر تحميل المستخدمين' : 'Failed to load users'));
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, search, planKey, user?.id]);

  useEffect(() => { load(); }, [load]);

  return (
    <div data-testid="admin-users">
      <div className="flex flex-wrap gap-2 mb-4">
        <div className="relative flex-1 min-w-[180px]">
          <Search className="w-4 h-4 absolute start- top-1/2 -translate-y-1/2 text-slate-400" />
          <Input
            value={search}
            onChange={(e) => { setSearch(e.target.value); setPage(1); }}
            placeholder={isAr() ? "ابحث بالبريد أو user_id" : "Search email or user_id"}
            className="ps-9"
          />
        </div>
        <select
          value={planKey}
          onChange={(e) => { setPlanKey(e.target.value); setPage(1); }}
          className="rounded-md border border-slate-200 px-2 text-sm h-10"
          data-testid="admin-users-plan-filter"
        >
          <option value="">{isAr() ? "جميع الخطط" : "All plans"}</option>
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
          <RefreshCw className={`w-4 h-4 me-2 ${loading ? 'animate-spin' : ''}`} />
          {isAr() ? "تحديث" : "Refresh"}
        </Button>
        <Button
          variant="outline"
          size="sm"
          onClick={async () => {
            try {
              const res = await api.get('/admin/users.csv', { responseType: 'blob' });
              const url = URL.createObjectURL(res.data);
              const a = document.createElement('a');
              a.href = url;
              a.download = `mychat_users_${new Date().toISOString().slice(0,10)}.csv`;
              document.body.appendChild(a);
              a.click();
              document.body.removeChild(a);
              URL.revokeObjectURL(url);
              toast.success((isAr() ? 'تم تنزيل ملف CSV' : 'CSV downloaded'));
            } catch (err) {
              toast.error((isAr() ? 'فشل تصدير CSV' : 'CSV export failed'));
            }
          }}
        >
          {isAr() ? "تصدير CSV" : "Export CSV"}
        </Button>
      </div>

      <div className="bg-white rounded-2xl border border-slate-100 overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-slate-50 text-slate-600">
            <tr>
              <th className="text-start px-3 py-2">{isAr() ? "البريد" : "Email"}</th>
              <th className="text-start px-3 py-2">{isAr() ? "الخطة" : "Plan"}</th>
              <th className="text-end px-3 py-2">{isAr() ? "Instagram" : "IG"}</th>
              <th className="text-end px-3 py-2">{isAr() ? "القواعد النشطة" : "Active rules"}</th>
              {/* Phase 2.18S: column titles now spell out the
                  current-month scoping so a fresh month showing 0
                  across the board does not look like a bug. */}
              <th className="text-end px-3 py-2" title={isAr() ? "التعليقات المعالَجة هذا الشهر" : "Comments processed this calendar month"}>{isAr() ? "تعليقات (شهري)" : "Comments (mo)"}</th>
              <th className="text-end px-3 py-2" title={isAr() ? "الردود العلنية المُرسلة هذا الشهر" : "Public replies sent this calendar month"}>{isAr() ? "ردود (شهري)" : "Replies (mo)"}</th>
              <th className="text-end px-3 py-2" title="DMs sent this calendar month">DMs (mo)</th>
              <th className="text-start px-3 py-2">{isAr() ? "الحالة" : "Status"}</th>
              <th className="text-start px-3 py-2">{isAr() ? "تاريخ الإنشاء" : "Created"}</th>
              <th className="text-end px-3 py-2"></th>
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
                  <td className="px-3 py-2 text-end font-mono">{u.instagram_accounts_count}</td>
                  <td className="px-3 py-2 text-end font-mono">{u.active_automations_count}</td>
                  <td className="px-3 py-2 text-end font-mono">{u.current_month_usage?.comments_processed ?? 0}</td>
                  <td className="px-3 py-2 text-end font-mono">{u.current_month_usage?.public_replies_sent ?? 0}</td>
                  <td className="px-3 py-2 text-end font-mono">{u.current_month_usage?.dms_sent ?? 0}</td>
                  <td className="px-3 py-2">
                    {exceeded
                      ? <Badge className="bg-rose-100 text-rose-700 border-0"><AlertTriangle className="w-3 h-3 me-1" />{isAr() ? "تجاوز الحدّ" : "Limit"}</Badge>
                      : <Badge className="bg-emerald-100 text-emerald-700 border-0"><CheckCircle2 className="w-3 h-3 me-1" />{isAr() ? "سليم" : "OK"}</Badge>}
                  </td>
                  <td className="px-3 py-2 text-xs text-slate-500">{formatTimestamp(u.created_at)}</td>
                  <td className="px-3 py-2 text-end">
                    <Button size="sm" variant="ghost" onClick={() => onSelect(u.user_id)}>
                      {isAr() ? "عرض" : "View"}
                    </Button>
                  </td>
                </tr>
              );
            })}
            {loading && (!data?.items || data.items.length === 0) && (
              // Phase 2.18P: /admin/users is a heavy join (~5s on cold
              // backend). Without a skeleton the table looks empty +
              // broken until the request resolves.
              [0, 1, 2, 3, 4].map((i) => (
                <tr key={`skeleton-${i}`} data-testid="admin-users-skeleton">
                  <td className="px-3 py-3" colSpan={10}>
                    <div className="h-4 w-full animate-pulse rounded bg-slate-100" />
                  </td>
                </tr>
              ))
            )}
            {(!loading && (!data?.items || data.items.length === 0)) && (
              <tr><td className="px-3 py-6 text-center text-slate-500" colSpan={10}>No users.</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {data?.pagination && data.pagination.total_pages > 1 && (
        <div className="flex items-center justify-end gap-2 mt-3 text-sm">
          <span className="text-slate-500">
            {isAr() ? 'الصفحة ' : 'Page '}{data.pagination.page} / {data.pagination.total_pages}
          </span>
          <Button variant="outline" size="sm" disabled={page <= 1}
                  onClick={() => setPage((p) => Math.max(1, p - 1))}>{isAr() ? "السابق" : "Prev"}</Button>
          <Button variant="outline" size="sm"
                  disabled={page >= data.pagination.total_pages}
                  onClick={() => setPage((p) => p + 1)}>{isAr() ? "التالي" : "Next"}</Button>
        </div>
      )}
    </div>
  );
}

function UserDetailTab({ userId, onBack, me }) {
  const { user, refreshUser } = useAuth();
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
      const cacheKey = `admin:user-detail:${user?.id || 'anon'}:${userId}`;
      const result = await cachedApiGetSWR(
        cacheKey,
        () => api.get(`/admin/users/${encodeURIComponent(userId)}/detail`),
        { ttlMs: ADMIN_CACHE_TTL_MS, maxStaleMs: 2 * 60 * 1000, persist: true, onUpdate: setData },
      );
      setData(result.data);
      setPlanKey(result.data?.plan?.plan_key || 'free');
      if (result.stale) setLoadError('Showing cached user detail. Refresh failed.');
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'تعذّر تحميل المستخدم' : 'Failed to load user');
      setLoadError(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر تحميل المستخدم' : 'Failed to load user'));
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر تحميل المستخدم' : 'Failed to load user'));
    } finally {
      setLoading(false);
    }
  }, [userId, user?.id]);

  useEffect(() => { load(); }, [load]);

  const onAssign = useCallback(async (opts = {}) => {
    setAssigning(true);
    try {
      const body = {
        plan_key: planKey,
        reason: reason || 'manual_admin_assignment',
      };
      if (opts.period_days != null) body.period_days = opts.period_days;
      if (opts.extend) body.extend = true;
      await api.post(`/admin/users/${encodeURIComponent(userId)}/plan`, body);
      const msg = opts.period_days != null
        ? (opts.extend
            ? (isAr()
                ? `تم تجديد الخطة ${PLAN_DISPLAY[planKey] || planKey} لمدة ${opts.period_days} يوم`
                : `Renewed ${PLAN_DISPLAY[planKey] || planKey} for ${opts.period_days} more days`)
            : (isAr()
                ? `تم تفعيل الخطة ${PLAN_DISPLAY[planKey] || planKey} لمدة ${opts.period_days} يوم`
                : `Activated ${PLAN_DISPLAY[planKey] || planKey} for ${opts.period_days} days`))
        : (isAr()
            ? `تم تعيين الخطة إلى ${PLAN_DISPLAY[planKey] || planKey}`
            : `Plan set to ${PLAN_DISPLAY[planKey] || planKey}`);
      toast.success(msg);
      setReason('');
      invalidateAdminUserCaches(userId);
      // Phase 2.18J: if the admin assigned the plan to themselves,
      // their own Dashboard / Automations / Billing / usage caches
      // now show the OLD plan limits. Wipe those snapshots and have
      // refreshUser run /auth/bootstrap so the next visit to /app
      // renders with the new plan limits immediately.
      if (user?.id && userId === user.id) {
        invalidateApiCache('dashboard-summary');
        invalidateApiCache('automations-summary');
        invalidateApiCache('instagram-accounts');
        // Billing page reads /plan/current via billing:plan-current:*
        // and /plans via billing:plans:* — both must drop so the next
        // visit to /app/billing reflects the new plan card.
        invalidateApiCache('billing:plan-current');
        if (typeof refreshUser === 'function') {
          // Fire-and-forget — onAssign returns without blocking on
          // the bootstrap round-trip.
          refreshUser();
        }
      }
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'فشل تعيين الخطة' : 'Plan assignment failed');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'فشل تعيين الخطة' : 'Plan assignment failed'));
    } finally {
      setAssigning(false);
    }
  }, [userId, planKey, reason, load, user?.id, refreshUser]);

  const [recomputeBusy, setRecomputeBusy] = useState(false);
  const onRecomputeUsage = useCallback(async () => {
    setRecomputeBusy(true);
    try {
      const r = await api.post(`/admin/users/${encodeURIComponent(userId)}/plan/recompute-usage`);
      const scanned = r.data?.events_scanned ?? 0;
      toast.success(isAr()
        ? `أُعيد احتساب الاستخدام من ${scanned} حدث`
        : `Recomputed usage from ${scanned} events`);
      invalidateAdminUserCaches(userId);
      invalidateApiCache('billing:plan-current');
      invalidateApiCache('dashboard-summary');
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'تعذّر إعادة الاحتساب' : 'Failed to recompute usage');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر إعادة الاحتساب' : 'Failed to recompute usage'));
    } finally {
      setRecomputeBusy(false);
    }
  }, [userId, load]);

  const onCreateAllowance = useCallback(async () => {
    const metrics = {};
    if (parseInt(allowanceCommentsExtra, 10) > 0)
      metrics.comments_processed_extra = parseInt(allowanceCommentsExtra, 10);
    if (parseInt(allowanceDmsExtra, 10) > 0)
      metrics.dms_sent_extra = parseInt(allowanceDmsExtra, 10);
    if (parseInt(allowanceRepliesExtra, 10) > 0)
      metrics.public_replies_sent_extra = parseInt(allowanceRepliesExtra, 10);
    if (Object.keys(metrics).length === 0) {
      toast.error((isAr() ? 'أدخل قيمة رقمية واحدة على الأقل' : 'Enter at least one numeric allowance'));
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
      toast.success((isAr() ? 'تم منح الحصّة' : 'Allowance granted'));
      setAllowanceCommentsExtra('');
      setAllowanceDmsExtra('');
      setAllowanceRepliesExtra('');
      setAllowanceReason('');
      invalidateAdminUserCaches(userId);
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'تعذّر المنح' : 'Failed to grant');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر المنح' : 'Failed to grant'));
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
      toast.success((isAr() ? 'تم سحب الحصّة' : 'Allowance revoked'));
      invalidateAdminUserCaches(userId);
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'تعذّر السحب' : 'Failed to revoke');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر السحب' : 'Failed to revoke'));
    }
  }, [userId, load]);

  const onSuspend = useCallback(async () => {
    const reasonText = window.prompt('Suspend reason (optional):', '') || '';
    try {
      await api.post(`/admin/users/${encodeURIComponent(userId)}/suspend`, { reason: reasonText });
      toast.success((isAr() ? 'تم إيقاف المستخدم' : 'User suspended'));
      invalidateAdminUserCaches(userId);
      await load();
    } catch (err) {
      toast.error(err?.response?.data?.detail || (isAr() ? 'تعذّر إيقاف المستخدم' : 'Failed to suspend'));
    }
  }, [userId, load]);

  const onUnsuspend = useCallback(async () => {
    try {
      await api.post(`/admin/users/${encodeURIComponent(userId)}/unsuspend`, {});
      toast.success((isAr() ? 'تم إعادة تفعيل المستخدم' : 'User reactivated'));
      invalidateAdminUserCaches(userId);
      await load();
    } catch (err) {
      toast.error(err?.response?.data?.detail || (isAr() ? 'تعذّر إعادة التفعيل' : 'Failed to unsuspend'));
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
      toast.success((isAr() ? 'تم حذف المستخدم مؤقّتاً' : 'User soft-deleted'));
      invalidateAdminUserCaches(userId);
      await load();
    } catch (err) {
      toast.error(err?.response?.data?.detail || (isAr() ? 'تعذّر الحذف' : 'Failed to delete'));
    }
  }, [userId, load]);

  const onDisableAutomation = useCallback(async (automationId) => {
    if (!window.confirm(`Pause automation ${automationId}?`)) return;
    try {
      await api.post(`/admin/automations/${encodeURIComponent(automationId)}/disable`, {
        reason: 'admin_pause',
      });
      toast.success((isAr() ? 'تم إيقاف الأتمتة' : 'Automation paused'));
      invalidateAdminUserCaches(userId);
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'تعذّر الإيقاف' : 'Failed to pause');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر الإيقاف' : 'Failed to pause'));
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
        <ArrowLeft className="w-4 h-4 me-2" /> Back to users
      </Button>

      {loading && !data && (
        <div className="text-center py-12 text-slate-500">{isAr() ? 'جارٍ التحميل…' : 'Loading…'}</div>
      )}

      {loadError && (
        <div className="mb-4">
          <AdminErrorCard
            title={loadError}
            onRetry={() => {
              invalidateApiCache('admin:user-detail');
              load();
            }}
            loading={loading}
          />
        </div>
      )}

      {data && (
        <>
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
            <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold">{isAr() ? "المستخدم" : "User"}</div>
            <div className="mt-1 text-2xl font-bold text-slate-800 font-mono">{profile.email || (isAr() ? 'مستخدم غير معروف' : 'Unknown user')}</div>
            <div className="text-xs text-slate-500 font-mono mt-1">{data.user_id || userId}</div>
            <div className="text-xs text-slate-400 mt-1">
              {isAr() ? 'تاريخ الإنشاء: ' : 'Created: '}{formatTimestamp(profile.created_at)}
            </div>
          </section>

          {/* Plan + assignment */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
            <div className="flex flex-wrap items-end justify-between gap-3 mb-3">
              <div>
                <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold">{isAr() ? "الخطة" : "Plan"}</div>
                <div className="mt-1 text-2xl font-bold text-slate-800">
                  {plan.display_name || PLAN_DISPLAY[plan.plan_key] || plan.plan_key || (isAr() ? 'المجانية' : 'Free')}
                </div>
                <div className="text-xs text-slate-500 mt-1">
                  {isAr() ? 'الفوترة: ' : 'Billing: '}<span className="font-semibold">{isAr() ? 'غير مُفعّلة بعد' : 'Not enabled yet'}</span>
                  {plan.assignment_reason && (
                    <span className="ms-2 text-slate-400">· {isAr() ? 'آخر سبب: ' : 'last reason: '}{plan.assignment_reason}</span>
                  )}
                </div>
              </div>
            </div>
            {/* Current plan-period summary (manual grant while billing is off) */}
            {(plan.current_period_end || plan.plan_expired) && (
              <div className="mb-3 text-xs text-slate-600 bg-slate-50 border border-slate-100 rounded-md p-2"
                   data-testid="admin-detail-plan-period">
                {plan.plan_expired ? (
                  <span className="text-rose-700 font-semibold">
                    {isAr() ? 'انتهت صلاحية الخطة — رجع للحساب المجاني' : 'Plan expired — fell back to Free'}
                  </span>
                ) : (
                  <span>
                    {isAr() ? 'تنتهي في: ' : 'Renews / expires: '}
                    <span className="font-mono">{formatTimestamp(plan.current_period_end)}</span>
                  </span>
                )}
              </div>
            )}
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
                  placeholder={isAr() ? "السبب (مثل: مختبر تجريبي)" : "Reason (e.g. 'beta tester')"}
                  value={reason}
                  onChange={(e) => setReason(e.target.value)}
                  className="flex-1 min-w-[180px]"
                />
                <Button onClick={() => onAssign()} disabled={assigning} data-testid="admin-detail-assign-btn">
                  {assigning ? (isAr() ? 'جارٍ الحفظ…' : 'Saving…') : (isAr() ? 'تعيين الخطة' : 'Assign plan')}
                </Button>
                <Button
                  variant="outline"
                  onClick={() => onAssign({ period_days: 30 })}
                  disabled={assigning}
                  data-testid="admin-detail-activate-30d-btn"
                  title={isAr() ? 'فعّل الخطة لمدة 30 يوم اعتباراً من الآن' : 'Activate plan for 30 days starting now'}
                >
                  {isAr() ? 'تفعيل 30 يوم' : 'Activate 30 days'}
                </Button>
                <Button
                  variant="outline"
                  onClick={() => onAssign({ period_days: 30, extend: true })}
                  disabled={assigning}
                  data-testid="admin-detail-renew-30d-btn"
                  title={isAr() ? 'أضف 30 يوم فوق الفترة الحالية' : 'Add 30 days on top of the current period'}
                >
                  {isAr() ? 'تمديد +30 يوم' : 'Extend +30 days'}
                </Button>
                <Button
                  variant="outline"
                  onClick={onRecomputeUsage}
                  disabled={recomputeBusy}
                  data-testid="admin-detail-recompute-usage-btn"
                  title={isAr() ? 'أعد احتساب عدّادات الاستهلاك من السجل الخام' : 'Rebuild monthly usage counters from raw events'}
                >
                  {recomputeBusy
                    ? (isAr() ? 'جارٍ الاحتساب…' : 'Recomputing…')
                    : (isAr() ? 'إعادة احتساب الاستهلاك' : 'Recompute usage')}
                </Button>
              </div>
            ) : (
              <div className="text-xs text-slate-500" data-testid="admin-detail-plan-readonly">
                {isAr() ? 'ليس لديك صلاحية لتغيير خطة هذا المستخدم.' : "You don't have permission to change this user's plan."}
              </div>
            )}
          </section>

          {/* Status + suspend/delete */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4" data-testid="admin-user-status">
            <div className="flex flex-wrap items-center justify-between gap-2 mb-2">
              <div>
                <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold">{isAr() ? "الحالة" : "Status"}</div>
                <div className="mt-1 text-base font-semibold">
                  {profile.status === 'suspended'
                    ? <Badge className="bg-amber-100 text-amber-800 border-0">{isAr() ? "موقوف" : "Suspended"}</Badge>
                    : profile.status === 'deleted'
                      ? <Badge className="bg-rose-100 text-rose-700 border-0">{isAr() ? "محذوف" : "Deleted"}</Badge>
                      : <Badge className="bg-emerald-100 text-emerald-700 border-0">{isAr() ? "نشط" : "Active"}</Badge>}
                  {profile.google_linked && (
                    <Badge className="ms-2 bg-blue-100 text-blue-700 border-0">{isAr() ? "مرتبط بـGoogle" : "Google linked"}</Badge>
                  )}
                </div>
              </div>
              <div className="flex gap-2">
                {canManageUsers && (profile.status || 'active') === 'active' && (
                  <Button size="sm" variant="outline" onClick={onSuspend} data-testid="admin-suspend-btn">
                    {isAr() ? "إيقاف" : "Suspend"}
                  </Button>
                )}
                {canManageUsers && profile.status === 'suspended' && (
                  <Button size="sm" variant="outline" onClick={onUnsuspend} data-testid="admin-unsuspend-btn">
                    {isAr() ? "إعادة تفعيل" : "Unsuspend"}
                  </Button>
                )}
                {canDeleteUsers && profile.status !== 'deleted' && (
                  <Button size="sm" variant="outline"
                          className="text-rose-700 border-rose-200 hover:bg-rose-50"
                          onClick={onSoftDelete} data-testid="admin-soft-delete-btn">
                    {isAr() ? "حذف مؤقّت" : "Soft delete"}
                  </Button>
                )}
              </div>
            </div>
            {profile.suspended_at && (
              <div className="text-xs text-slate-500">
                {isAr() ? 'تاريخ الإيقاف: ' : 'Suspended at '}{formatTimestamp(profile.suspended_at)}
                {profile.suspended_by && <> {isAr() ? 'بواسطة ' : 'by '}<span className="font-mono">{profile.suspended_by}</span></>}
              </div>
            )}
            {profile.deleted_at && (
              <div className="text-xs text-slate-500">
                {isAr() ? 'تاريخ الحذف: ' : 'Deleted at '}{formatTimestamp(profile.deleted_at)}
                {profile.deleted_by && <> {isAr() ? 'بواسطة ' : 'by '}<span className="font-mono">{profile.deleted_by}</span></>}
              </div>
            )}
          </section>

          {/* Active allowances + grant form */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4" data-testid="admin-allowances">
            <h3 className="text-sm font-semibold text-slate-700 mb-3">{isAr() ? "حصص مخصّصة" : "Custom allowances"}</h3>
            {activeOverrides.length === 0 && (
              <div className="text-sm text-slate-500 mb-3">{isAr() ? "لا توجد منح نشطة." : "No active grants."}</div>
            )}
            {activeOverrides.length > 0 && (
              <ul className="space-y-2 text-sm mb-3">
                {activeOverrides.map((ov) => (
                  <li key={ov.id} className="flex items-start justify-between border-b border-slate-100 pb-2 last:border-0">
                    <div>
                      <div className="font-semibold">
                        {ov.grant_name || ov.type}
                        <Badge className="ms-2 bg-slate-100 text-slate-700 border-0 text-[10px]">{ov.type}</Badge>
                      </div>
                      <div className="text-xs text-slate-500 font-mono mt-1">
                        {Object.entries(ov.metrics || {}).map(([k, v]) => (
                          <span key={k} className="me-3">{k}: +{v}</span>
                        ))}
                      </div>
                      <div className="text-xs text-slate-400 mt-1">
                        {isAr() ? 'تبدأ ' : 'starts '}{formatTimestamp(ov.starts_at)}
                        {ov.ends_at && <> · {isAr() ? 'تنتهي ' : 'ends '}{formatTimestamp(ov.ends_at)}</>}
                        {ov.created_by_email && <> · {isAr() ? 'بواسطة ' : 'by '}{ov.created_by_email}</>}
                      </div>
                    </div>
                    {canAssignPlan && (
                      <Button size="sm" variant="ghost" onClick={() => onRevokeAllowance(ov.id)}>
                        {isAr() ? "إلغاء" : "Revoke"}
                      </Button>
                    )}
                  </li>
                ))}
              </ul>
            )}
            {canAssignPlan && (
              <div className="border-t border-slate-100 pt-3">
                <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold mb-2">
                  {isAr() ? "منح حصّة جديدة" : "Grant new allowance"}
                </div>
                <div className="grid gap-2 sm:grid-cols-2">
                  <select value={allowanceType} onChange={(e) => setAllowanceType(e.target.value)}
                          className="rounded-md border border-slate-200 px-2 text-sm h-10"
                          data-testid="admin-allowance-type-select">
                    <option value="additive_allowance">{isAr() ? "إضافي (فوق الخطة)" : "Additive (extras on top of plan)"}</option>
                    <option value="trial_grant">{isAr() ? "منحة تجريبية (بتاريخ انتهاء)" : "Trial grant (extras with end date)"}</option>
                  </select>
                  <Input type="number" min="0" placeholder={isAr() ? "عدد الأيام (0 = بلا نهاية)" : "Days valid (0 = no end)"}
                         value={allowanceDays} onChange={(e) => setAllowanceDays(e.target.value)} />
                  <Input type="number" min="0" placeholder={isAr() ? "تعليقات إضافية" : "Extra comments processed"}
                         value={allowanceCommentsExtra}
                         onChange={(e) => setAllowanceCommentsExtra(e.target.value)} />
                  <Input type="number" min="0" placeholder={isAr() ? "رسائل إضافية" : "Extra DMs sent"}
                         value={allowanceDmsExtra}
                         onChange={(e) => setAllowanceDmsExtra(e.target.value)} />
                  <Input type="number" min="0" placeholder={isAr() ? "ردود علنية إضافية" : "Extra public replies"}
                         value={allowanceRepliesExtra}
                         onChange={(e) => setAllowanceRepliesExtra(e.target.value)} />
                  <Input placeholder={isAr() ? "السبب / اسم المنحة (اختياري)" : "Reason / grant name (optional)"}
                         value={allowanceReason}
                         onChange={(e) => setAllowanceReason(e.target.value)} />
                </div>
                <div className="mt-2">
                  <Button onClick={onCreateAllowance} disabled={grantBusy}
                          data-testid="admin-grant-allowance-btn">
                    {grantBusy ? (isAr() ? 'جارٍ المنح…' : 'Granting…') : (isAr() ? 'منح الحصّة' : 'Grant allowance')}
                  </Button>
                </div>
              </div>
            )}
          </section>

          {/* Usage */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
            <h3 className="text-sm font-semibold text-slate-700 mb-3">
              {isAr() ? 'الاستهلاك — ' : 'Usage — '}{usage.event_month || (isAr() ? 'الشهر الحالي' : 'current month')}
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
            <h3 className="text-sm font-semibold text-slate-700 mb-3">{isAr() ? "حسابات Instagram" : "Instagram accounts"}</h3>
            {instagramAccounts.length === 0 && (
              <div className="text-sm text-slate-500">None connected.</div>
            )}
            <ul className="space-y-2 text-sm">
              {instagramAccounts.map(a => (
                <li key={a.id} className="flex items-center justify-between border-b border-slate-100 pb-2 last:border-0">
                  <div>
                    <div className="font-mono">{a.username || a.instagram_account_id}</div>
                    <div className="text-xs text-slate-500">
                      {a.connectionValid ? (isAr() ? 'مربوط' : 'Connected') : (isAr() ? 'غير مربوط' : 'Disconnected')} ·
                      {' '}token: {a.tokenSource || '—'} · expires: {formatTimestamp(a.tokenExpiresAt)}
                    </div>
                  </div>
                  {a.active && <Badge className="bg-blue-100 text-blue-700 border-0">{isAr() ? "نشط" : "Active"}</Badge>}
                </li>
              ))}
            </ul>
          </section>

          {/* Automations */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
            <h3 className="text-sm font-semibold text-slate-700 mb-3">{isAr() ? "الأتمتات" : "Automations"}</h3>
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
                      ? <Badge className="bg-emerald-100 text-emerald-700 border-0">{isAr() ? "نشط" : "Active"}</Badge>
                      : <Badge className="bg-slate-100 text-slate-600 border-0">{a.status || 'paused'}</Badge>}
                    {a.active && canDisableAutomation && (
                      <Button size="sm" variant="outline" onClick={() => onDisableAutomation(a.automation_id)}>
                        {isAr() ? "إيقاف مؤقت" : "Pause"}
                      </Button>
                    )}
                  </div>
                </li>
              ))}
            </ul>
          </section>

          {/* Recent failures */}
          <section className="bg-white rounded-2xl border border-slate-100 p-5">
            <h3 className="text-sm font-semibold text-slate-700 mb-3">{isAr() ? "الأعطال الحديثة" : "Recent failures"}</h3>
            {recentFailures.length === 0 && (
              <div className="text-sm text-slate-500">No recent failures.</div>
            )}
            <ul className="space-y-3 text-sm">
              {recentFailures.map(f => {
                const reason = f.dm_failure_reason || f.reply_failure_reason;
                const described = reason ? describeDmFailureReason(reason) : null;
                return (
                  <li key={f.comment_id} className="border-b border-slate-100 pb-3 last:border-0">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-slate-700 font-mono text-xs">{f.action_status}</span>
                      {described && (
                        <span className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-medium ${dmFailureToneClasses(described.tone)}`}>
                          {described.label}
                        </span>
                      )}
                      <span className="text-xs text-slate-500 font-mono">
                        reply={f.reply_status || '—'} · dm={f.dm_status || '—'}
                      </span>
                    </div>
                    {described && (
                      <p className="mt-1 text-xs text-slate-500 leading-relaxed">{described.detail}</p>
                    )}
                    <div className="mt-1 text-[11px] text-slate-400 font-mono">
                      ig_comment={f.ig_comment_id} · media={f.media_id} · attempts={f.attempts}
                      {reason && <> · raw_reason={reason}</>}
                    </div>
                  </li>
                );
              })}
            </ul>
          </section>
        </>
      )}
    </div>
  );
}

function AdminsTab({ me }) {
  const { user } = useAuth();
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
      const result = await cachedApiGetSWR(
        `admin:members:${user?.id || 'anon'}`,
        () => api.get('/admin/members'),
        { ttlMs: ADMIN_CACHE_TTL_MS, maxStaleMs: 2 * 60 * 1000, persist: true, onUpdate: setData },
      );
      setData(result.data);
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'تعذّر تحميل الأعضاء' : 'Failed to load members');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر تحميل الأعضاء' : 'Failed to load members'));
    } finally {
      setLoading(false);
    }
  }, [user?.id]);

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
      toast.success(isAr()
        ? `أُضيف ${addEmail} بدور ${ROLE_DISPLAY[addRole]}`
        : `Added ${addEmail} as ${ROLE_DISPLAY[addRole]}`);
      setAddEmail(''); setAddReason(''); setAddRole('viewer');
      invalidateApiCache('admin:members');
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'تعذّر إضافة العضو' : 'Failed to add member');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر إضافة العضو' : 'Failed to add member'));
    } finally {
      setSubmitting(false);
    }
  }, [addEmail, addRole, addReason, load]);

  const onChangeRole = useCallback(async (member, newRole) => {
    try {
      await api.patch(`/admin/members/${encodeURIComponent(member.user_id)}`, {
        role: newRole, reason: 'admin_role_change',
      });
      toast.success(isAr()
        ? `تم تغيير الدور إلى ${ROLE_DISPLAY[newRole]}`
        : `Role changed to ${ROLE_DISPLAY[newRole]}`);
      invalidateApiCache('admin:members');
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'تعذّر تغيير الدور' : 'Failed to change role');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر تغيير الدور' : 'Failed to change role'));
    }
  }, [load]);

  const onRemove = useCallback(async (member) => {
    if (!window.confirm(`Remove ${member.email}?`)) return;
    try {
      await api.delete(`/admin/members/${encodeURIComponent(member.user_id)}`);
      toast.success((isAr() ? 'تمت إزالة العضو' : 'Member removed'));
      invalidateApiCache('admin:members');
      await load();
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'تعذّر إزالة العضو' : 'Failed to remove member');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر إزالة العضو' : 'Failed to remove member'));
    }
  }, [load]);

  const myRole = me?.role;
  const assignableOptions = roleOptionsAssignableBy(myRole);

  return (
    <div data-testid="admin-admins">
      <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
        <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold mb-1">
          {isAr() ? 'أنت' : 'You are'}
        </div>
        <div className="text-base font-semibold text-slate-800">
          {ROLE_DISPLAY[myRole] || myRole}
          {me?.bootstrap_owner && (
            <span className="ms-2 text-xs text-blue-700">(bootstrap owner via ADMIN_EMAILS)</span>
          )}
        </div>
        <div className="text-xs text-slate-400 mt-1">{(me?.permissions || []).length} permissions</div>
      </section>

      {canManage && (
        <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4" data-testid="admin-add-member">
          <h3 className="text-sm font-semibold text-slate-700 mb-3">{isAr() ? "إضافة عضو إدارة" : "Add admin member"}</h3>
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
              placeholder={isAr() ? "السبب (اختياري)" : "Reason (optional)"}
              value={addReason}
              onChange={(e) => setAddReason(e.target.value)}
              className="flex-1 min-w-[160px]"
            />
            <Button type="submit" disabled={submitting || !addEmail.trim()} data-testid="admin-add-member-btn">
              {submitting ? (isAr() ? 'جارٍ الإضافة…' : 'Adding…') : (isAr() ? 'إضافة عضو' : 'Add member')}
            </Button>
          </form>
          <p className="text-xs text-slate-400 mt-2">
            {isAr() ? 'يجب أن يكون لدى المستخدم حساب بالفعل. لا يُرسل بريد دعوة.' : 'User must already have an account. No invitation email is sent.'}
          </p>
        </section>
      )}

      <div className="bg-white rounded-2xl border border-slate-100 overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-slate-50 text-slate-600">
            <tr>
              <th className="text-start px-3 py-2">{isAr() ? "البريد" : "Email"}</th>
              <th className="text-start px-3 py-2">{isAr() ? "الدور" : "Role"}</th>
              <th className="text-start px-3 py-2">{isAr() ? "الحالة" : "Status"}</th>
              <th className="text-start px-3 py-2">{isAr() ? "أضافه" : "Added by"}</th>
              <th className="text-start px-3 py-2">{isAr() ? "تاريخ الإنشاء" : "Created"}</th>
              <th className="text-end px-3 py-2"></th>
            </tr>
          </thead>
          <tbody>
            {(data?.items || []).map(m => {
              const canMutate = canManage && canManageRole(myRole, m.role);
              return (
                <tr key={m.user_id || m.email} className="border-t border-slate-100">
                  <td className="px-3 py-2 font-mono">
                    {m.email}
                    {m.bootstrap_owner && <span className="ms-2 text-[10px] text-blue-700">bootstrap</span>}
                  </td>
                  <td className="px-3 py-2">
                    <Badge className="bg-slate-100 text-slate-700 border-0">
                      {ROLE_DISPLAY[m.role] || m.role}
                    </Badge>
                  </td>
                  <td className="px-3 py-2">
                    {m.disabled_at
                      ? <Badge className="bg-rose-100 text-rose-700 border-0">{isAr() ? "مُعطّل" : "Disabled"}</Badge>
                      : <Badge className="bg-emerald-100 text-emerald-700 border-0">{isAr() ? "نشط" : "Active"}</Badge>}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs">{m.added_by_email || '—'}</td>
                  <td className="px-3 py-2 text-xs text-slate-500">{formatTimestamp(m.created_at)}</td>
                  <td className="px-3 py-2 text-end">
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
                          {isAr() ? "إزالة" : "Remove"}
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
  const { user } = useAuth();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const result = await cachedApiGetSWR(
        `admin:metrics:${user?.id || 'anon'}:reconciliation`,
        () => api.get('/admin/metrics/reconciliation'),
        { ttlMs: ADMIN_CACHE_TTL_MS, maxStaleMs: 2 * 60 * 1000, persist: true, onUpdate: setData },
      );
      setData(result.data);
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'تعذّر تحميل التسوية' : 'Failed to load reconciliation');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر تحميل التسوية' : 'Failed to load reconciliation'));
    } finally {
      setLoading(false);
    }
  }, [user?.id]);

  useEffect(() => { load(); }, [load]);

  return (
    <div data-testid="admin-reconciliation">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-semibold text-slate-700">
          {isAr() ? 'تسوية المقاييس' : 'Metrics reconciliation'}
          {data?.event_month && <span className="text-slate-400 font-mono ms-2">{data.event_month}</span>}
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
          <RefreshCw className={`w-4 h-4 me-2 ${loading ? 'animate-spin' : ''}`} />
          {isAr() ? "تحديث" : "Refresh"}
        </Button>
      </div>
      {data && data.mismatch_count > 0 && (
        <div className="mb-3 rounded-2xl border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900"
             data-testid="reconciliation-mismatch-banner">
          <AlertTriangle className="inline w-4 h-4 me-1" />
          {data.mismatch_count} mismatch(es) detected. Review the table below.
        </div>
      )}
      <div className="bg-white rounded-2xl border border-slate-100 overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-slate-50 text-slate-600">
            <tr>
              <th className="text-start px-3 py-2">{isAr() ? "المقياس" : "Metric"}</th>
              <th className="text-end px-3 py-2">{isAr() ? "لوحة التحكّم" : "Dashboard"}</th>
              <th className="text-end px-3 py-2">{isAr() ? "المُعاد حسابه" : "Recomputed"}</th>
              <th className="text-end px-3 py-2">Δ</th>
              <th className="text-start px-3 py-2">{isAr() ? "الحالة" : "Status"}</th>
              <th className="text-start px-3 py-2">{isAr() ? "المصدر" : "Source"}</th>
            </tr>
          </thead>
          <tbody>
            {(data?.items || []).map((row) => (
              <tr key={row.metric_name} className="border-t border-slate-100">
                <td className="px-3 py-2 font-mono text-xs">{row.metric_name}</td>
                <td className="px-3 py-2 text-end font-mono">{row.dashboard_value}</td>
                <td className="px-3 py-2 text-end font-mono">{row.recomputed_value}</td>
                <td className="px-3 py-2 text-end font-mono">{row.difference ?? '—'}</td>
                <td className="px-3 py-2">
                  {row.status === 'ok'
                    ? <Badge className="bg-emerald-100 text-emerald-700 border-0">{isAr() ? "سليم" : "OK"}</Badge>
                    : <Badge className="bg-amber-100 text-amber-800 border-0">{isAr() ? "غير متطابق" : "Mismatch"}</Badge>}
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


// Phase 2.18W: admin audit-log viewer. Backend endpoint
// /api/admin/audit-log already exists and returns sanitized entries
// (admin email, action, target user id, metadata, timestamp). This
// component just renders them in reverse-chronological order so the
// owner can see every privileged mutation at a glance.
function AuditLogTab() {
  const { user } = useAuth();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const result = await cachedApiGetSWR(
        `admin:audit-log:${user?.id || 'anon'}`,
        () => api.get('/admin/audit-log', { params: { limit: 100 } }),
        { ttlMs: ADMIN_CACHE_TTL_MS, maxStaleMs: 2 * 60 * 1000, persist: true, onUpdate: setData },
      );
      setData(result.data);
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'تعذّر تحميل سجلّ التدقيق' : 'Failed to load audit log');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر تحميل سجلّ التدقيق' : 'Failed to load audit log'));
    } finally {
      setLoading(false);
    }
  }, [user?.id]);

  useEffect(() => { load(); }, [load]);

  const items = safeList(data?.items);

  return (
    <div data-testid="admin-audit-log">
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-sm font-semibold text-slate-700">
          {isAr() ? 'سجلّ تدقيق الإدارة' : 'Admin audit log'}
          {data?.count !== undefined && (
            <span className="ms-2 text-slate-400 font-mono">({data.count})</span>
          )}
        </h3>
        <Button
          variant="outline"
          size="sm"
          onClick={() => {
            invalidateApiCache('admin:audit-log');
            load();
          }}
          disabled={loading}
        >
          <RefreshCw className={`w-4 h-4 me-2 ${loading ? 'animate-spin' : ''}`} />
          {isAr() ? "تحديث" : "Refresh"}
        </Button>
      </div>
      {loading && items.length === 0 ? (
        <AdminSkeleton rows={6} />
      ) : items.length === 0 ? (
        <div className="rounded-2xl border border-slate-100 bg-white p-6 text-center text-sm text-slate-500">
          {isAr() ? "لم تُسجَّل أي إجراءات إدارية بعد." : "No admin actions recorded yet."}
        </div>
      ) : (
        <div className="bg-white rounded-2xl border border-slate-100 overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-slate-50 text-slate-500 text-xs uppercase tracking-wide">
              <tr>
                <th className="text-start px-3 py-2">{isAr() ? "الوقت" : "When"}</th>
                <th className="text-start px-3 py-2">{isAr() ? "المسؤول" : "Admin"}</th>
                <th className="text-start px-3 py-2">{isAr() ? "الإجراء" : "Action"}</th>
                <th className="text-start px-3 py-2">{isAr() ? "المستخدم المستهدف" : "Target user"}</th>
                <th className="text-start px-3 py-2">{isAr() ? "التفاصيل" : "Details"}</th>
              </tr>
            </thead>
            <tbody>
              {items.map((entry) => {
                const meta = entry.metadata || {};
                const metaKeys = Object.keys(meta);
                return (
                  <tr key={entry.id} className="border-t border-slate-100">
                    <td className="px-3 py-2 text-xs text-slate-500 font-mono whitespace-nowrap">
                      {formatTimestamp(entry.created_at)}
                    </td>
                    <td className="px-3 py-2 font-mono text-xs">
                      {entry.admin_email || '—'}
                    </td>
                    <td className="px-3 py-2">
                      <Badge className="bg-slate-100 text-slate-700 border-0">
                        {entry.action || 'unknown'}
                      </Badge>
                    </td>
                    <td className="px-3 py-2 font-mono text-xs text-slate-600">
                      {entry.target_user_id || '—'}
                    </td>
                    <td className="px-3 py-2 text-xs text-slate-600">
                      {metaKeys.length === 0
                        ? '—'
                        : (
                          <span className="font-mono break-all">
                            {metaKeys.slice(0, 3).map(k => `${k}=${JSON.stringify(meta[k])}`).join(' · ')}
                            {metaKeys.length > 3 ? ` · +${metaKeys.length - 3} more` : ''}
                          </span>
                        )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}


function WebhookDlqTab() {
  const [items, setItems] = useState([]);
  const [counts, setCounts] = useState({ pending_retry: 0, permanently_failed: 0, replayed: 0 });
  const [loading, setLoading] = useState(false);
  const [filter, setFilter] = useState('');
  const [retrying, setRetrying] = useState({});

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const url = '/admin/webhook-dlq' + (filter ? `?status=${encodeURIComponent(filter)}` : '');
      const { data } = await api.get(url);
      setItems(data.items || []);
      setCounts(data.counts || {});
    } catch (err) {
      const msg = err?.response?.data?.detail || (isAr() ? 'تعذّر تحميل طابور الأخطاء' : 'Failed to load webhook DLQ');
      toast.error(typeof msg === 'string' ? msg : (isAr() ? 'تعذّر تحميل طابور الأخطاء' : 'Failed to load webhook DLQ'));
    } finally {
      setLoading(false);
    }
  }, [filter]);

  useEffect(() => { load(); }, [load]);

  const replayOne = async (id) => {
    setRetrying((prev) => ({ ...prev, [id]: true }));
    try {
      await api.post(`/admin/webhook-dlq/${encodeURIComponent(id)}/retry`);
      toast.success(isAr() ? 'تمّت إعادة التشغيل بنجاح' : 'Replayed successfully');
      await load();
    } catch (err) {
      const detail = err?.response?.data?.detail || (isAr() ? 'فشلت إعادة التشغيل' : 'Replay failed');
      toast.error(typeof detail === 'string' ? detail : (isAr() ? 'فشلت إعادة التشغيل' : 'Replay failed'));
    } finally {
      setRetrying((prev) => ({ ...prev, [id]: false }));
    }
  };

  const statusStyle = (s) => {
    if (s === 'replayed') return 'bg-emerald-50 text-emerald-700 border-emerald-200';
    if (s === 'permanently_failed') return 'bg-rose-50 text-rose-700 border-rose-200';
    return 'bg-amber-50 text-amber-700 border-amber-200';
  };

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        {[
          { key: 'pending_retry', label: (isAr() ? 'بانتظار إعادة المحاولة' : 'Pending retry'), value: counts.pending_retry, tone: 'amber' },
          { key: 'permanently_failed', label: (isAr() ? 'فشل دائم' : 'Permanently failed'), value: counts.permanently_failed, tone: 'rose' },
          { key: 'replayed', label: (isAr() ? 'أُعيد تشغيلها' : 'Replayed'), value: counts.replayed, tone: 'emerald' },
        ].map((tile) => (
          <button
            key={tile.key}
            type="button"
            onClick={() => setFilter(filter === tile.key ? '' : tile.key)}
            className={`rounded-2xl border p-4 text-start transition ${
              filter === tile.key ? 'ring-2 ring-slate-900' : 'hover:shadow-sm'
            } ${
              tile.tone === 'amber' ? 'border-amber-200 bg-amber-50' :
              tile.tone === 'rose' ? 'border-rose-200 bg-rose-50' :
              'border-emerald-200 bg-emerald-50'
            }`}
          >
            <div className={`text-2xl font-extrabold font-display ${
              tile.tone === 'amber' ? 'text-amber-700' :
              tile.tone === 'rose' ? 'text-rose-700' :
              'text-emerald-700'
            }`}>{tile.value ?? 0}</div>
            <div className="text-xs font-medium mt-1 text-slate-700">{tile.label}</div>
          </button>
        ))}
      </div>

      <div className="bg-white rounded-2xl border border-slate-100 p-5">
        <div className="flex items-center justify-between mb-3 flex-wrap gap-2">
          <h3 className="text-sm font-semibold text-slate-700">
            {isAr() ? "طابور أخطاء الـWebhook" : "Webhook DLQ"} {filter && <span className="text-slate-500 font-normal">— {isAr() ? "مفلتر: " : "filtered: "}{filter}</span>}
          </h3>
          <div className="flex items-center gap-2">
            {filter && (
              <Button variant="outline" size="sm" onClick={() => setFilter('')}>{isAr() ? "مسح الفلتر" : "Clear filter"}</Button>
            )}
            <Button variant="outline" size="sm" onClick={load} disabled={loading}>
              <RefreshCw className={`w-4 h-4 me-2 ${loading ? 'animate-spin' : ''}`} /> Refresh
            </Button>
          </div>
        </div>
        {!loading && items.length === 0 && (
          <div className="text-sm text-slate-500 py-6 text-center">
            No DLQ entries{filter ? ` with status ${filter}` : ''}. Webhook processing is healthy. 🎉
          </div>
        )}
        <ul className="space-y-3">
          {items.map((entry) => (
            <li key={entry.id} className="border border-slate-100 rounded-xl p-4">
              <div className="flex items-start justify-between flex-wrap gap-2">
                <div className="flex items-center gap-2 flex-wrap">
                  <Badge className={`rounded-full border ${statusStyle(entry.status)}`}>{entry.status}</Badge>
                  <span className="text-xs text-slate-600 font-mono">attempts={entry.attempts}</span>
                  {entry.payload_object && (
                    <span className="text-xs text-slate-500 font-mono">object={entry.payload_object}</span>
                  )}
                  {entry.payload_entry_count > 0 && (
                    <span className="text-xs text-slate-500 font-mono">entries={entry.payload_entry_count}</span>
                  )}
                </div>
                {entry.status !== 'replayed' && (
                  <Button
                    variant="outline" size="sm"
                    onClick={() => replayOne(entry.id)}
                    disabled={retrying[entry.id]}
                  >
                    <RotateCcw className={`w-4 h-4 me-2 ${retrying[entry.id] ? 'animate-spin' : ''}`} />
                    {isAr() ? "إعادة التشغيل الآن" : "Replay now"}
                  </Button>
                )}
              </div>
              {entry.exception_type && (
                <div className="mt-2 text-xs text-rose-700">
                  <span className="font-semibold">{entry.exception_type}</span>
                  {entry.exception_message && <>: {entry.exception_message}</>}
                </div>
              )}
              <div className="mt-2 text-[11px] text-slate-400 font-mono">
                first_failed={entry.first_failed_at?.slice(0, 19)}
                {entry.last_failed_at && entry.last_failed_at !== entry.first_failed_at && (
                  <> · last_failed={entry.last_failed_at.slice(0, 19)}</>
                )}
                {entry.next_attempt_at && entry.status === 'pending_retry' && (
                  <> · next_attempt={entry.next_attempt_at.slice(0, 19)}</>
                )}
                {entry.replayed_at && <> · replayed={entry.replayed_at.slice(0, 19)}</>}
              </div>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}


/**
 * Operator-facing summary of where the latest comment-to-DM automation
 * stopped, per linked Instagram account. Backed by the protected
 * /api/admin/instagram/automation-stop-point endpoint (RBAC: admin.users.view).
 *
 * Lives ONLY inside this admin console — there is no public diagnostics
 * page. The endpoint is not advertised in the sidebar; an operator who
 * is not signed in as admin cannot reach this tab.
 *
 * Privacy: every external id we render is partial-redacted by the
 * backend. We never render tokens, full webhook payloads, or full
 * comment/DM text. The "Last send error" line includes only a short
 * sanitized Meta error code + message that the backend already capped.
 */
function AutomationStopPointTab() {
  const [state, setState] = useState('idle');
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  const load = useCallback(async () => {
    setState('loading');
    setError(null);
    try {
      const r = await api.get('/admin/instagram/automation-stop-point');
      setData(r.data);
      setState('success');
    } catch (err) {
      const status = err?.response?.status;
      const detail = err?.response?.data?.detail || err?.message || 'Request failed';
      setError(String(detail).slice(0, 240));
      setState(status === 401 || status === 403 ? 'forbidden' : 'error');
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const summaries = data?.summaries || (data?.summary ? [data.summary] : []);
  const ar = isAr();

  return (
    <section data-testid="admin-automation-stop-point">
      <header className="flex flex-wrap items-center gap-3 mb-3">
        <Activity className="w-4 h-4 text-slate-500" />
        <div className="flex-1 min-w-[200px]">
          <div className="font-semibold text-slate-800">
            {ar ? 'نقطة توقّف الأتمتة' : 'Automation Stop Point'}
          </div>
          <div className="text-xs text-slate-500">
            {ar
              ? 'ملخّص آخر تعليق ولماذا لم يكتمل التدفّق — لكل حساب انستجرام مربوط. الأرقام والمعرّفات مُغطّاة جزئياً.'
              : 'Latest comment + why the flow did not complete, per linked Instagram account. External ids are partial-redacted.'}
          </div>
        </div>
        {state === 'success' && (
          <Badge className="bg-emerald-100 text-emerald-700 border-0">
            <CheckCircle2 className="w-3 h-3 me-1" /> {summaries.length} {ar ? 'حساب' : 'accounts'}
          </Badge>
        )}
        {state === 'forbidden' && (
          <Badge className="bg-amber-100 text-amber-800 border-0">
            <ShieldAlert className="w-3 h-3 me-1" /> {ar ? 'غير مصرّح' : 'Forbidden'}
          </Badge>
        )}
        {state === 'error' && (
          <Badge className="bg-rose-100 text-rose-700 border-0">
            <AlertTriangle className="w-3 h-3 me-1" /> {ar ? 'فشل' : 'Failed'}
          </Badge>
        )}
        <Button
          size="sm"
          variant="outline"
          onClick={load}
          disabled={state === 'loading'}
          data-testid="automation-stop-point-reload"
        >
          <RefreshCw className={`w-4 h-4 me-2 ${state === 'loading' ? 'animate-spin' : ''}`} />
          {ar ? 'تحديث' : 'Reload'}
        </Button>
      </header>

      {state === 'loading' && <AdminSkeleton rows={3} />}
      {error && (
        <div className="text-xs text-rose-700 bg-rose-50 border border-rose-200 rounded-md p-2 mb-3">
          {error}
        </div>
      )}
      {state === 'forbidden' && (
        <div className="text-xs text-amber-800 bg-amber-50 border border-amber-200 rounded-md p-2 mb-3">
          {ar
            ? 'حسابك لا يملك صلاحية admin.users.view المطلوبة.'
            : 'Your account does not have the required admin.users.view permission.'}
        </div>
      )}

      {state === 'success' && summaries.length === 0 && (
        <div className="text-xs text-slate-500 py-6 text-center">
          {ar
            ? 'لا توجد حسابات Instagram مربوطة على workspace الخاص بك.'
            : 'No linked Instagram accounts on this workspace.'}
        </div>
      )}

      {summaries.length > 0 && (
        <div className="space-y-3">
          {summaries.map((s) => (
            <AutomationStopPointCard key={s.username || s.instagram_account_id_partial} summary={s} />
          ))}
        </div>
      )}
    </section>
  );
}

function AutomationStopPointCard({ summary }) {
  const ar = isAr();
  const reached = !!summary.last_comment_reached_backend;
  const matched = !!summary.rule_matched;
  const reason = summary.exact_stop_reason || '';
  const successCase = reason === 'automation_success';
  const toneBg = successCase
    ? 'bg-emerald-50 border-emerald-200'
    : reached
      ? 'bg-amber-50 border-amber-200'
      : 'bg-rose-50 border-rose-200';

  return (
    <div
      className={`rounded-2xl border p-4 ${toneBg}`}
      data-testid={`stop-point-card-${summary.username || summary.instagram_account_id_partial || 'unknown'}`}
    >
      <div className="flex flex-wrap items-center gap-2 mb-2">
        <div className="font-semibold text-slate-800">
          @{summary.username || summary.instagram_account_id_partial || '-'}
        </div>
        {summary.instagram_account_id_partial && (
          <span className="text-xs text-slate-500 font-mono">{summary.instagram_account_id_partial}</span>
        )}
        <Badge className={
          successCase
            ? 'bg-emerald-100 text-emerald-700 border-0'
            : reached
              ? 'bg-amber-100 text-amber-800 border-0'
              : 'bg-rose-100 text-rose-700 border-0'
        }>
          {successCase
            ? (ar ? 'نجح' : 'Success')
            : reached
              ? (ar ? 'وصل التعليق' : 'Comment reached')
              : (ar ? 'لم يصل التعليق' : 'No comment')}
        </Badge>
      </div>

      <div className="grid sm:grid-cols-2 gap-x-4 gap-y-1 text-xs text-slate-700 mb-3">
        <div>
          {ar ? 'وصل التعليق:' : 'Comment reached:'}{' '}
          <span className="font-mono">{String(reached)}</span>
          {' · '}
          <span className="font-mono">source={summary.source || '-'}</span>
        </div>
        <div>
          {ar ? 'القواعد المحمَّلة:' : 'Rules loaded:'}{' '}
          <span className="font-mono">{summary.rules_loaded_count ?? '-'}</span>
        </div>
        <div>
          {ar ? 'مطابقة:' : 'Rule matched:'} <span className="font-mono">{String(matched)}</span>
          {summary.selected_media_matched != null && (
            <>
              {' · '}
              {ar ? 'المنشور المختار:' : 'Selected media:'}{' '}
              <span className="font-mono">{String(summary.selected_media_matched)}</span>
            </>
          )}
        </div>
        <div>
          {ar ? 'محاولات:' : 'Attempts:'}{' '}
          <span className="font-mono">reply={String(!!summary.reply_attempted)}</span>
          {' · '}
          <span className="font-mono">dm={String(!!summary.dm_attempted)}</span>
        </div>
        <div className="sm:col-span-2">
          {ar ? 'بعد رسالة الافتتاح:' : 'Post opening DM:'}{' '}
          <span className="font-mono">opening_dm_sent={String(!!summary.opening_dm_sent)}</span>
          {' · '}
          <span className={`font-mono ${summary.click_received ? 'text-emerald-700' : 'text-rose-700'}`}>
            click_received={String(!!summary.click_received)}
          </span>
          {' · '}
          <span className={`font-mono ${summary.continuation_send_success ? 'text-emerald-700' : ''}`}>
            continuation_send_success={String(!!summary.continuation_send_success)}
          </span>
        </div>
        <div className="sm:col-span-2">
          {ar ? 'السبب الدقيق:' : 'Exact stop reason:'}{' '}
          <span className="font-mono">{reason || '-'}</span>
        </div>
        {summary.last_send_error && (
          <div className="sm:col-span-2 text-rose-700">
            {ar ? 'آخر خطأ Meta:' : 'Last send error:'}{' '}
            <span className="font-mono">{summary.last_send_error.stage}</span>
            {' · '}
            <span className="font-mono">{summary.last_send_error.reason || '-'}</span>
            {summary.last_send_error.error_code && (
              <>
                {' · '}
                <span className="font-mono">code={summary.last_send_error.error_code}</span>
              </>
            )}
            {summary.last_send_error.error_message && (
              <>
                {' · '}
                <span className="font-mono">{summary.last_send_error.error_message}</span>
              </>
            )}
          </div>
        )}
      </div>

      <div className="text-sm text-slate-800 bg-white border border-slate-200 rounded-md p-3">
        <div className="text-xs uppercase tracking-wide text-slate-500 font-semibold mb-1">
          {ar ? 'الإجراء المُوصى به' : 'Next recommended action'}
        </div>
        <div>{summary.next_recommended_action || '-'}</div>
      </div>
    </div>
  );
}


/**
 * Sanitized rule-coverage inspector, gated by the same admin permission
 * as the other Instagram support tabs. Calls the protected backend
 * endpoint /api/admin/instagram/rule-coverage-inspector and renders the
 * already-sanitized JSON. Used to answer "my Stop Point says
 * rule_not_matched — what does the backend actually see in my rule
 * document?" without reintroducing the deleted /app/admin/
 * instagram-diagnostics page. All identifiers are partial-redacted by
 * the backend; we render exactly what the API returned and never fetch
 * raw rule bodies or tokens.
 */
function RuleCoverageTab() {
  const [state, setState] = useState('idle');
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [copyStatus, setCopyStatus] = useState('idle');

  const load = useCallback(async () => {
    setState('loading');
    setError(null);
    setCopyStatus('idle');
    try {
      const r = await api.get('/admin/instagram/rule-coverage-inspector');
      setData(r.data);
      setState('success');
    } catch (err) {
      const status = err?.response?.status;
      const detail = err?.response?.data?.detail || err?.message || 'Request failed';
      setError(String(detail).slice(0, 240));
      setState(status === 401 || status === 403 ? 'forbidden' : 'error');
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const onCopyJson = useCallback(async () => {
    if (!data) return;
    try {
      const text = JSON.stringify(data, null, 2);
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.setAttribute('readonly', '');
        ta.style.position = 'absolute';
        ta.style.left = '-9999px';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
      }
      setCopyStatus('copied');
      setTimeout(() => setCopyStatus('idle'), 1500);
    } catch (_) {
      setCopyStatus('error');
      setTimeout(() => setCopyStatus('idle'), 1500);
    }
  }, [data]);

  const accounts = data?.accounts || [];
  const ar = isAr();

  return (
    <section data-testid="admin-rule-coverage">
      <header className="flex flex-wrap items-center gap-3 mb-3">
        <Activity className="w-4 h-4 text-slate-500" />
        <div className="flex-1 min-w-[200px]">
          <div className="font-semibold text-slate-800">
            {ar ? 'تغطية القواعد' : 'Rule Coverage'}
          </div>
          <div className="text-xs text-slate-500">
            {ar
              ? 'الشكل الفعلي لكل قاعدة أتمتة نشطة كما يقرأها الخادم — لتحديد سبب «rule_not_matched». المعرّفات مغطّاة جزئياً.'
              : 'How the backend sees each active automation rule — to explain "rule_not_matched". External ids are partial-redacted.'}
          </div>
        </div>
        {state === 'success' && (
          <Badge className="bg-emerald-100 text-emerald-700 border-0">
            <CheckCircle2 className="w-3 h-3 me-1" /> {accounts.length} {ar ? 'حساب' : 'accounts'}
          </Badge>
        )}
        {state === 'forbidden' && (
          <Badge className="bg-amber-100 text-amber-800 border-0">
            <ShieldAlert className="w-3 h-3 me-1" /> {ar ? 'غير مصرّح' : 'Forbidden'}
          </Badge>
        )}
        {state === 'error' && (
          <Badge className="bg-rose-100 text-rose-700 border-0">
            <AlertTriangle className="w-3 h-3 me-1" /> {ar ? 'فشل' : 'Failed'}
          </Badge>
        )}
        <Button
          size="sm"
          variant="outline"
          onClick={load}
          disabled={state === 'loading'}
          data-testid="rule-coverage-reload"
        >
          <RefreshCw className={`w-4 h-4 me-2 ${state === 'loading' ? 'animate-spin' : ''}`} />
          {ar ? 'تحديث' : 'Reload'}
        </Button>
        <Button
          size="sm"
          variant="outline"
          onClick={onCopyJson}
          disabled={!data || state === 'loading'}
          data-testid="rule-coverage-copy"
        >
          {copyStatus === 'copied'
            ? (ar ? 'تم النسخ' : 'Copied')
            : copyStatus === 'error'
              ? (ar ? 'فشل النسخ' : 'Copy failed')
              : (ar ? 'نسخ JSON' : 'Copy JSON')}
        </Button>
      </header>

      {state === 'loading' && <AdminSkeleton rows={3} />}
      {error && (
        <div className="text-xs text-rose-700 bg-rose-50 border border-rose-200 rounded-md p-2 mb-3">
          {error}
        </div>
      )}
      {state === 'forbidden' && (
        <div className="text-xs text-amber-800 bg-amber-50 border border-amber-200 rounded-md p-2 mb-3">
          {ar
            ? 'حسابك لا يملك صلاحية admin.users.view المطلوبة.'
            : 'Your account does not have the required admin.users.view permission.'}
        </div>
      )}

      {state === 'success' && accounts.length === 0 && (
        <div className="text-xs text-slate-500 py-6 text-center">
          {ar
            ? 'لا توجد حسابات Instagram مربوطة على workspace الخاص بك.'
            : 'No linked Instagram accounts on this workspace.'}
        </div>
      )}

      {accounts.length > 0 && (
        <div className="space-y-3">
          {accounts.map((acc) => (
            <RuleCoverageAccountCard
              key={acc.instagram_account_id_partial || acc.username || Math.random()}
              account={acc}
            />
          ))}
        </div>
      )}
    </section>
  );
}

function RuleCoverageAccountCard({ account }) {
  const ar = isAr();
  const counts = account.classification_counts || {};
  return (
    <div className="bg-white border border-slate-200 rounded-lg p-3 text-sm" data-testid="rule-coverage-account-card">
      <div className="flex flex-wrap items-center gap-2 mb-2">
        <span className="font-semibold text-slate-800">@{account.username || '-'}</span>
        <span className="text-xs text-slate-500 font-mono">
          {account.instagram_account_id_partial || '-'}
        </span>
        <span className="text-xs text-slate-400">·</span>
        <span className="text-xs text-slate-500">
          {ar ? 'آخر تعليق على media:' : 'latest comment media:'}{' '}
          <span className="font-mono">{account.latest_comment_media_id_partial || '-'}</span>
        </span>
        <div className="ms-auto flex flex-wrap gap-1">
          <Badge className="bg-slate-100 text-slate-700 border-0 text-[10px]">
            {ar ? 'قواعد نشطة' : 'active'}: {account.active_rule_count ?? 0}
          </Badge>
          <Badge className="bg-emerald-100 text-emerald-700 border-0 text-[10px]">
            {ar ? 'عامة' : 'general'}: {counts.general_any_post ?? 0}
          </Badge>
          <Badge className="bg-sky-100 text-sky-700 border-0 text-[10px]">
            {ar ? 'محدّدة' : 'post-specific'}: {counts.post_specific ?? 0}
          </Badge>
          {(counts.invalid_or_non_comment ?? 0) > 0 && (
            <Badge className="bg-rose-100 text-rose-700 border-0 text-[10px]">
              {ar ? 'غير صالحة' : 'invalid'}: {counts.invalid_or_non_comment}
            </Badge>
          )}
        </div>
      </div>

      {Array.isArray(account.rules) && account.rules.length > 0 ? (
        <div className="space-y-2">
          {account.rules.map((rule, idx) => (
            <RuleCoverageRuleRow
              key={(rule.rule_id_partial || `rule-${idx}`) + idx}
              rule={rule}
            />
          ))}
        </div>
      ) : (
        <div className="text-xs text-slate-500 py-3 text-center">
          {ar ? 'لا توجد قواعد نشطة لهذا الحساب.' : 'No active rules for this account.'}
        </div>
      )}
    </div>
  );
}

function RuleCoverageRuleRow({ rule }) {
  const ar = isAr();
  const cls = rule.classification?.class || 'invalid_or_non_comment';
  const should = rule.classification?.should_evaluate_as_comment_rule;
  const match = rule.latest_comment_match?.should_match;
  const matchReason = rule.latest_comment_match?.match_failure_reason;
  const toneBg = cls === 'general'
    ? (match === true ? 'bg-emerald-50 border-emerald-200' : 'bg-amber-50 border-amber-200')
    : cls === 'post_specific'
      ? 'bg-sky-50 border-sky-200'
      : 'bg-rose-50 border-rose-200';
  const nodes = Array.isArray(rule.trigger_nodes) ? rule.trigger_nodes : [];
  const selectedAliases = rule.selected_media_alias_partials || {};
  const scopeFields = rule.scope_field_partials || {};
  const aliasEntries = Object.entries(selectedAliases).filter(([, v]) => v);
  const scopeEntries = Object.entries(scopeFields).filter(([, v]) => v);

  return (
    <div className={`rounded-md border p-2 ${toneBg}`} data-testid="rule-coverage-rule-row">
      <div className="flex flex-wrap items-center gap-2 text-xs mb-1">
        <span className="font-mono text-slate-700">{rule.rule_id_partial || '-'}</span>
        <Badge className="bg-white text-slate-700 border border-slate-300 text-[10px]">
          {rule.status || 'unknown'}
        </Badge>
        <Badge className={`border-0 text-[10px] ${
          cls === 'general' ? 'bg-emerald-100 text-emerald-800' :
          cls === 'post_specific' ? 'bg-sky-100 text-sky-800' :
          'bg-rose-100 text-rose-800'
        }`}>
          {cls}
        </Badge>
        {should === true ? (
          <Badge className="bg-emerald-100 text-emerald-700 border-0 text-[10px]">
            {ar ? 'سيُقيَّم كـ comment' : 'evaluates as comment'}
          </Badge>
        ) : should === false ? (
          <Badge className="bg-rose-100 text-rose-700 border-0 text-[10px]">
            {ar ? 'لن يُقيَّم' : 'not evaluated'}
          </Badge>
        ) : null}
        {match === true && (
          <Badge className="bg-emerald-100 text-emerald-700 border-0 text-[10px]">
            {ar ? 'مطابقة' : 'should match'}
          </Badge>
        )}
        {match === false && (
          <Badge className="bg-amber-100 text-amber-800 border-0 text-[10px]">
            {ar ? 'لن تطابق' : 'will NOT match'}{matchReason ? `: ${matchReason}` : ''}
          </Badge>
        )}
        {rule.name && (
          <span className="text-slate-600 truncate">{rule.name}</span>
        )}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-x-3 gap-y-1 text-[11px] text-slate-700">
        <div>
          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold">
            {ar ? 'القيم الخام' : 'raw top-level'}
          </div>
          <div>
            trigger:{' '}
            <span className="font-mono">{rule.raw_top_level?.trigger ?? '-'}</span>
          </div>
          <div>
            post_scope:{' '}
            <span className="font-mono">{rule.raw_top_level?.post_scope ?? '-'}</span>
          </div>
          <div>
            postScope:{' '}
            <span className="font-mono">{rule.raw_top_level?.postScope ?? '-'}</span>
          </div>
          <div>
            scope:{' '}
            <span className="font-mono">{rule.raw_top_level?.scope ?? '-'}</span>
          </div>
          <div>
            process_existing:{' '}
            <span className="font-mono">{String(!!rule.raw_top_level?.process_existing_unreplied_comments)}</span>
          </div>
        </div>

        <div>
          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold">
            {ar ? 'القيم المعيارية' : 'canonical normalized'}
          </div>
          <div>
            trigger:{' '}
            <span className="font-mono">{rule.normalized?.canonical_trigger ?? '-'}</span>
          </div>
          <div>
            post_scope:{' '}
            <span className="font-mono">{rule.normalized?.canonical_post_scope ?? '-'}</span>
          </div>
          <div>
            selected_media:{' '}
            <span className="font-mono">{rule.normalized?.selected_specific_media_id_partial ?? '-'}</span>
          </div>
          <div>
            is_comment_automation_rule:{' '}
            <span className="font-mono">{String(!!rule.classification?.is_comment_automation_rule)}</span>
          </div>
        </div>

        <div className="md:col-span-2">
          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold">
            {ar ? 'العقد (trigger nodes)' : 'trigger nodes'}
          </div>
          {nodes.length === 0 ? (
            <div className="text-slate-500">{ar ? 'لا يوجد عقد trigger.' : '(no trigger node)'}</div>
          ) : (
            <ul className="list-disc ps-5">
              {nodes.map((n, i) => (
                <li key={i}>
                  <span className="font-mono">
                    data.trigger=
                    {n.data_trigger || '-'}
                    {n.data_trigger_type ? ` · trigger_type=${n.data_trigger_type}` : ''}
                    {' · '}
                    data.post_scope={n.data_post_scope || '-'}
                    {n.data_media_id_partial ? ` · media=${n.data_media_id_partial}` : ''}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </div>

        <div>
          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold">
            {ar ? 'اختيار الـ media' : 'selected media aliases'}
          </div>
          {aliasEntries.length === 0 ? (
            <div className="text-slate-500">{ar ? 'لا يوجد.' : '(none)'}</div>
          ) : (
            <ul className="list-disc ps-5">
              {aliasEntries.map(([k, v]) => (
                <li key={k}>
                  <span className="font-mono">{k}={v}</span>
                </li>
              ))}
            </ul>
          )}
        </div>

        <div>
          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold">
            {ar ? 'نطاق الحساب' : 'account scoping fields'}
          </div>
          {scopeEntries.length === 0 ? (
            <div className="text-slate-500">{ar ? 'لا يوجد.' : '(none)'}</div>
          ) : (
            <ul className="list-disc ps-5">
              {scopeEntries.map(([k, v]) => (
                <li key={k}>
                  <span className="font-mono">{k}={v}</span>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </div>
  );
}


/**
 * Sanitized webhook-health panel. Calls the protected backend
 * /api/admin/instagram/multi-account-health endpoint and renders the
 * already-sanitized JSON, so an operator can classify "automation
 * works but source=polling" into one of these five buckets without
 * extracting an admin JWT:
 *   A. Meta does not send comment webhooks for this account.
 *   B. Webhook arrives but no webhook_comment_detected fires.
 *   C. Webhook arrives but account resolution fails.
 *   D. Webhook resolves but processing is skipped.
 *   E. Webhook is fast, polling overwrites the displayed source.
 *
 * Visibility only — no automation logic, no public diagnostics page.
 * Backend output is partial-redacted; this component renders exactly
 * what the API returned.
 */
function WebhookHealthTab() {
  const [state, setState] = useState('idle');
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [copyStatus, setCopyStatus] = useState('idle');

  const load = useCallback(async () => {
    setState('loading');
    setError(null);
    setCopyStatus('idle');
    try {
      const r = await api.get('/admin/instagram/multi-account-health');
      setData(r.data);
      setState('success');
    } catch (err) {
      const status = err?.response?.status;
      const detail = err?.response?.data?.detail || err?.message || 'Request failed';
      setError(String(detail).slice(0, 240));
      setState(status === 401 || status === 403 ? 'forbidden' : 'error');
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const onCopyJson = useCallback(async () => {
    if (!data) return;
    try {
      const text = JSON.stringify(data, null, 2);
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
      } else {
        const ta = document.createElement('textarea');
        ta.value = text;
        ta.setAttribute('readonly', '');
        ta.style.position = 'absolute';
        ta.style.left = '-9999px';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
      }
      setCopyStatus('copied');
      setTimeout(() => setCopyStatus('idle'), 1500);
    } catch (_) {
      setCopyStatus('error');
      setTimeout(() => setCopyStatus('idle'), 1500);
    }
  }, [data]);

  const accounts = data?.accounts || [];
  const ar = isAr();

  return (
    <section data-testid="admin-webhook-health">
      <header className="flex flex-wrap items-center gap-3 mb-3">
        <Activity className="w-4 h-4 text-slate-500" />
        <div className="flex-1 min-w-[200px]">
          <div className="font-semibold text-slate-800">
            {ar ? 'صحّة الـ Webhook' : 'Webhook Health'}
          </div>
          <div className="text-xs text-slate-500">
            {ar
              ? 'حالة اشتراك Meta Webhooks وأوقات آخر استلام/معالجة لكل حساب — لتشخيص سبب تأخّر التعليقات عبر polling.'
              : 'Meta webhook subscription state and last received/processed times per linked account — to classify why comments arrive via polling.'}
          </div>
        </div>
        {state === 'success' && (
          <Badge className="bg-emerald-100 text-emerald-700 border-0">
            <CheckCircle2 className="w-3 h-3 me-1" /> {accounts.length} {ar ? 'حساب' : 'accounts'}
          </Badge>
        )}
        {state === 'forbidden' && (
          <Badge className="bg-amber-100 text-amber-800 border-0">
            <ShieldAlert className="w-3 h-3 me-1" /> {ar ? 'غير مصرّح' : 'Forbidden'}
          </Badge>
        )}
        {state === 'error' && (
          <Badge className="bg-rose-100 text-rose-700 border-0">
            <AlertTriangle className="w-3 h-3 me-1" /> {ar ? 'فشل' : 'Failed'}
          </Badge>
        )}
        <Button
          size="sm"
          variant="outline"
          onClick={load}
          disabled={state === 'loading'}
          data-testid="webhook-health-reload"
        >
          <RefreshCw className={`w-4 h-4 me-2 ${state === 'loading' ? 'animate-spin' : ''}`} />
          {ar ? 'تحديث' : 'Reload'}
        </Button>
        <Button
          size="sm"
          variant="outline"
          onClick={onCopyJson}
          disabled={!data || state === 'loading'}
          data-testid="webhook-health-copy"
        >
          {copyStatus === 'copied'
            ? (ar ? 'تم النسخ' : 'Copied')
            : copyStatus === 'error'
              ? (ar ? 'فشل النسخ' : 'Copy failed')
              : (ar ? 'نسخ JSON' : 'Copy JSON')}
        </Button>
      </header>

      {state === 'loading' && <AdminSkeleton rows={3} />}
      {error && (
        <div className="text-xs text-rose-700 bg-rose-50 border border-rose-200 rounded-md p-2 mb-3">
          {error}
        </div>
      )}
      {state === 'forbidden' && (
        <div className="text-xs text-amber-800 bg-amber-50 border border-amber-200 rounded-md p-2 mb-3">
          {ar
            ? 'حسابك لا يملك صلاحية admin.users.view المطلوبة.'
            : 'Your account does not have the required admin.users.view permission.'}
        </div>
      )}

      {state === 'success' && data && (
        <div className="bg-slate-50 border border-slate-200 rounded-lg p-3 text-xs mb-3" data-testid="webhook-health-overview">
          <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
            <div>
              <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold">
                {ar ? 'آخر webhook استُلم' : 'webhook.last_received_at'}
              </div>
              <div className="font-mono break-all">{data.webhook?.last_received_at || '-'}</div>
            </div>
            <div>
              <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold">
                {ar ? 'آخر webhook عولج' : 'webhook.last_processed_at'}
              </div>
              <div className="font-mono break-all">{data.webhook?.last_processed_at || '-'}</div>
            </div>
            <div>
              <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold">
                {ar ? 'polling مفعّل' : 'polling.enabled'}
              </div>
              <div className="font-mono">{String(!!data.polling?.enabled)}</div>
            </div>
            <div>
              <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold">
                {ar ? 'فاصل polling (ث)' : 'polling.interval_seconds'}
              </div>
              <div className="font-mono">{data.polling?.interval_seconds ?? '-'}</div>
            </div>
          </div>
          {data.webhook?.recent_field_summary && (
            <div className="mt-3 pt-3 border-t border-slate-200" data-testid="webhook-health-field-summary">
              <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold mb-1">
                {ar ? 'آخر 50 webhook — حقول مرصودة' : 'recent webhook field-shape (last 50)'}
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-2">
                <div>
                  <div className="text-slate-500 text-[10px]">{ar ? 'عيّنات' : 'samples'}</div>
                  <div className="font-mono">{data.webhook.recent_field_summary.samples ?? 0}</div>
                </div>
                <div>
                  <div className="text-slate-500 text-[10px]">
                    {ar ? 'حقول رُصدت' : 'fields_seen'}
                  </div>
                  <div className="font-mono break-words">
                    {(data.webhook.recent_field_summary.fields_seen || []).join(', ') || '-'}
                  </div>
                </div>
                <div>
                  <div className="text-slate-500 text-[10px]">
                    {ar ? 'عيّنات تحوي comments' : 'comment_field_samples'}
                  </div>
                  <div className="font-mono">{data.webhook.recent_field_summary.comment_field_samples ?? 0}</div>
                </div>
                <div>
                  <div className="text-slate-500 text-[10px]">
                    {ar ? 'عيّنات messaging فقط' : 'messaging_only_samples'}
                  </div>
                  <div className="font-mono">{data.webhook.recent_field_summary.messaging_only_samples ?? 0}</div>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {state === 'success' && accounts.length === 0 && (
        <div className="text-xs text-slate-500 py-6 text-center">
          {ar
            ? 'لا توجد حسابات Instagram مربوطة على workspace الخاص بك.'
            : 'No linked Instagram accounts on this workspace.'}
        </div>
      )}

      {accounts.length > 0 && (
        <div className="space-y-3">
          {accounts.map((acc) => (
            <WebhookHealthAccountCard
              key={acc.instagram_account_id_partial || acc.username || Math.random()}
              account={acc}
            />
          ))}
        </div>
      )}
    </section>
  );
}

function WebhookHealthAccountCard({ account }) {
  const ar = isAr();
  const subscribed = Array.isArray(account.webhook_subscription_fields)
    ? account.webhook_subscription_fields
    : [];
  const missing = Array.isArray(account.webhook_subscription_missing)
    ? account.webhook_subscription_missing
    : [];
  const issues = Array.isArray(account.issues) ? account.issues : [];
  const stop = account.stop_point_summary || {};
  const stopReason = stop.exact_stop_reason || '-';
  const stopSource = stop.source || '-';
  const lastWebhook = account.last_webhook_event_time;
  const lastPolling = account.last_polling_scan_time || account.polling_last_scan_at;
  const lastWebhookMissing = !lastWebhook;
  const cardTone = !account.connection_valid
    ? 'bg-rose-50 border-rose-200'
    : missing.length > 0 || lastWebhookMissing
      ? 'bg-amber-50 border-amber-200'
      : 'bg-emerald-50 border-emerald-200';

  return (
    <div className={`border rounded-lg p-3 text-sm ${cardTone}`} data-testid="webhook-health-account-card">
      <div className="flex flex-wrap items-center gap-2 mb-2">
        <span className="font-semibold text-slate-800">@{account.username || '-'}</span>
        <span className="text-xs text-slate-500 font-mono">
          {account.instagram_account_id_partial || '-'}
        </span>
        <div className="ms-auto flex flex-wrap gap-1">
          <Badge className={`border-0 text-[10px] ${account.connection_valid ? 'bg-emerald-100 text-emerald-700' : 'bg-rose-100 text-rose-700'}`}>
            connectionValid: {String(!!account.connection_valid)}
          </Badge>
          <Badge className={`border-0 text-[10px] ${account.instant_webhook_eligible ? 'bg-emerald-100 text-emerald-700' : 'bg-amber-100 text-amber-800'}`}>
            instant_webhook_eligible: {String(!!account.instant_webhook_eligible)}
          </Badge>
          {missing.length > 0 && (
            <Badge className="bg-rose-100 text-rose-700 border-0 text-[10px]">
              {ar ? 'حقول مفقودة' : 'missing'}: {missing.length}
            </Badge>
          )}
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-x-3 gap-y-1 text-[11px] text-slate-700">
        <div>
          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold">
            {ar ? 'حقول مشترك بها' : 'subscribed_fields'}
          </div>
          {subscribed.length === 0 ? (
            <div className="text-rose-700">{ar ? 'لا يوجد' : '(none)'}</div>
          ) : (
            <div className="font-mono break-words">{subscribed.join(', ')}</div>
          )}
          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold mt-2">
            {ar ? 'حقول مفقودة' : 'webhook_subscription_missing'}
          </div>
          {missing.length === 0 ? (
            <div className="text-emerald-700">{ar ? 'لا يوجد' : '(none)'}</div>
          ) : (
            <div className="font-mono break-words text-rose-700">{missing.join(', ')}</div>
          )}
          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold mt-2">
            {ar ? 'آخر فحص اشتراك' : 'subscription_last_checked_at'}
          </div>
          <div className="font-mono break-all">{account.webhook_subscription_last_checked_at || '-'}</div>
        </div>

        <div>
          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold">
            {ar ? 'آخر webhook (حدث)' : 'last_webhook_event_time'}
          </div>
          <div className={`font-mono break-all ${lastWebhookMissing ? 'text-rose-700' : ''}`}>
            {lastWebhook || (ar ? '— لم يصل بعد' : '— never observed')}
          </div>

          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold mt-2">
            {ar ? 'آخر مسح polling' : 'last_polling_scan_time'}
          </div>
          <div className="font-mono break-all">{lastPolling || '-'}</div>

          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold mt-2">
            {ar ? 'آخر تعليق رُصد' : 'last_comment_seen_time'}
          </div>
          <div className="font-mono break-all">{account.last_comment_seen_time || '-'}</div>

          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold mt-2">
            {ar ? 'آخر مطابقة قاعدة' : 'last_rule_match_time'}
          </div>
          <div className="font-mono break-all">{account.last_rule_match_time || '-'}</div>

          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold mt-2">
            {ar ? 'آخر نجاح أتمتة' : 'last_automation_success_time'}
          </div>
          <div className="font-mono break-all">{account.last_automation_success_time || '-'}</div>
        </div>

        <div className="md:col-span-2">
          <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold">
            {ar ? 'نقطة التوقّف' : 'stop_point_summary'}
          </div>
          <div className="font-mono break-words">
            source=<span>{stopSource}</span>{' · '}
            exact_stop_reason=<span>{stopReason}</span>
          </div>
        </div>

        {issues.length > 0 && (
          <div className="md:col-span-2">
            <div className="uppercase tracking-wide text-slate-500 text-[10px] font-semibold">
              {ar ? 'تنبيهات' : 'issues'}
            </div>
            <ul className="list-disc ps-5">
              {issues.map((iss, i) => (
                <li key={i} className="font-mono">{iss}</li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </div>
  );
}


export default function AdminConsole() {
  const { user } = useAuth();
  const { lang } = useTranslation();
  const ar = lang === 'ar';
  const [me, setMe] = useState(null);   // null = loading
  const [overview, setOverview] = useState(null);
  const [overviewLoading, setOverviewLoading] = useState(false);
  const [tab, setTab] = useState('overview');
  const [selectedUserId, setSelectedUserId] = useState(null);

  // Probe admin gate. Uses the shared fetchAdminMe cache so Sidebar +
  // DashboardLayout + this page all see one /admin/me call instead of
  // three concurrent ones.
  useEffect(() => {
    let alive = true;
    fetchAdminMe().then((data) => {
      if (!alive) return;
      setMe(data || { is_admin: false });
      if (data?.is_admin) analytics.capture('admin_console_viewed', {});
    });
    return () => { alive = false; };
  }, []);

  const loadOverview = useCallback(async () => {
    setOverviewLoading(true);
    try {
      const result = await cachedApiGetSWR(
        `admin:overview:${user?.id || 'anon'}`,
        () => api.get('/admin/overview'),
        { ttlMs: ADMIN_CACHE_TTL_MS, maxStaleMs: 2 * 60 * 1000, persist: true, onUpdate: setOverview },
      );
      setOverview(result.data);
    } catch (err) {
      const msg = err?.response?.data?.detail || (ar ? 'تعذّر تحميل النظرة العامة' : 'Failed to load overview');
      toast.error(typeof msg === 'string' ? msg : (ar ? 'تعذّر تحميل النظرة العامة' : 'Failed to load overview'));
    } finally {
      setOverviewLoading(false);
    }
  }, [ar, user?.id]);

  useEffect(() => {
    if (me?.is_admin && tab === 'overview' && !overview) loadOverview();
  }, [me, tab, overview, loadOverview]);

  if (me === null) {
    return <div className="p-6 text-slate-500">{ar ? 'جارٍ التحقّق من صلاحية الإدارة…' : 'Checking admin access…'}</div>;
  }
  if (!me.is_admin) {
    return (
      <div className="p-6 max-w-3xl mx-auto" data-testid="admin-not-available">
        <div className="bg-white rounded-2xl border border-slate-100 p-6">
          <div className="flex items-center gap-2 text-rose-600 mb-2">
            <ShieldAlert className="w-5 h-5" />
            <h1 className="text-lg font-semibold">{ar ? 'غير متاح' : 'Not available'}</h1>
          </div>
          <p className="text-sm text-slate-600">
            {ar
              ? 'هذه الصفحة مخصّصة لمالك المنتج. إذا وصلت إليها بالخطأ، عُد إلى لوحة التحكّم.'
              : 'This page is for the product owner. If you reached this by mistake, head back to the dashboard.'}
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
  const canViewAudit = hasPermission(me, 'admin.audit.view');
  const canViewDlq = hasPermission(me, PERM_FAILURES_VIEW);

  return (
    <div className="p-4 sm:p-6 max-w-6xl mx-auto" data-testid="admin-console">
      <div className="mb-6">
        <div className="flex items-center gap-2 text-amber-700 mb-1">
          <Lock className="w-4 h-4" />
          <span className="text-xs font-semibold uppercase tracking-wide">{ar ? 'وحدة تحكّم المالك' : 'Owner console'}</span>
          <Badge className="bg-blue-100 text-blue-700 border-0 text-[10px]" data-testid="admin-current-role">
            {ROLE_DISPLAY[me?.role] || me?.role}
          </Badge>
        </div>
        <h1 className="text-3xl font-bold font-display">{ar ? 'الإدارة' : 'Admin'}</h1>
        <p className="text-slate-500 mt-1 text-sm">
          {ar
            ? 'متابعة المستخدمين والخطط والاستهلاك والأعطال. لا توجد عناصر تحكّم في المدفوعات هنا — الفوترة تُفعَّل لاحقاً.'
            : 'Monitor users, plans, usage, and failures. No payment controls here — billing is enabled later.'}
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
            <Activity className="w-4 h-4 me-2" /> {ar ? 'نظرة عامة' : 'Overview'}
          </Button>
        )}
        {canViewUsers && (
          <Button
            variant={tab === 'users' || tab === 'user-detail' ? 'default' : 'outline'}
            size="sm"
            onClick={() => { setTab('users'); setSelectedUserId(null); }}
            data-testid="admin-tab-users"
          >
            <Users className="w-4 h-4 me-2" /> {ar ? 'المستخدمون' : 'Users'}
          </Button>
        )}
        {canViewMembers && (
          <Button
            variant={tab === 'admins' ? 'default' : 'outline'}
            size="sm"
            onClick={() => { setTab('admins'); setSelectedUserId(null); }}
            data-testid="admin-tab-admins"
          >
            <UserCog className="w-4 h-4 me-2" /> {ar ? 'المسؤولون' : 'Admins'}
          </Button>
        )}
        {canViewMetrics && (
          <Button
            variant={tab === 'metrics' ? 'default' : 'outline'}
            size="sm"
            onClick={() => { setTab('metrics'); setSelectedUserId(null); }}
            data-testid="admin-tab-metrics"
          >
            <BarChart3 className="w-4 h-4 me-2" /> {ar ? 'المقاييس' : 'Metrics'}
          </Button>
        )}
        {canViewAudit && (
          <Button
            variant={tab === 'audit' ? 'default' : 'outline'}
            size="sm"
            onClick={() => { setTab('audit'); setSelectedUserId(null); }}
            data-testid="admin-tab-audit"
          >
            <ScrollText className="w-4 h-4 me-2" /> {ar ? 'سجلّ التدقيق' : 'Audit log'}
          </Button>
        )}
        {canViewDlq && (
          <Button
            variant={tab === 'webhook-dlq' ? 'default' : 'outline'}
            size="sm"
            onClick={() => { setTab('webhook-dlq'); setSelectedUserId(null); }}
            data-testid="admin-tab-webhook-dlq"
          >
            <Inbox className="w-4 h-4 me-2" /> {ar ? 'طابور الأخطاء' : 'Webhook DLQ'}
          </Button>
        )}
        {canViewUsers && (
          <Button
            variant={tab === 'automation-stop-point' ? 'default' : 'outline'}
            size="sm"
            onClick={() => { setTab('automation-stop-point'); setSelectedUserId(null); }}
            data-testid="admin-tab-automation-stop-point"
          >
            <Activity className="w-4 h-4 me-2" /> {ar ? 'نقطة توقّف الأتمتة' : 'Automation Stop Point'}
          </Button>
        )}
        {canViewUsers && (
          <Button
            variant={tab === 'rule-coverage' ? 'default' : 'outline'}
            size="sm"
            onClick={() => { setTab('rule-coverage'); setSelectedUserId(null); }}
            data-testid="admin-tab-rule-coverage"
          >
            <Activity className="w-4 h-4 me-2" /> {ar ? 'تغطية القواعد' : 'Rule Coverage'}
          </Button>
        )}
        {canViewUsers && (
          <Button
            variant={tab === 'webhook-health' ? 'default' : 'outline'}
            size="sm"
            onClick={() => { setTab('webhook-health'); setSelectedUserId(null); }}
            data-testid="admin-tab-webhook-health"
          >
            <Activity className="w-4 h-4 me-2" /> {ar ? 'صحّة الـ Webhook' : 'Webhook Health'}
          </Button>
        )}
        <div className="ms-auto" />
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
            <RefreshCw className={`w-4 h-4 me-2 ${overviewLoading ? 'animate-spin' : ''}`} />
            {ar ? 'تحديث' : 'Refresh'}
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
      {tab === 'audit' && canViewAudit && <AuditLogTab />}
      {tab === 'webhook-dlq' && canViewDlq && (
        <AdminSectionErrorBoundary name="webhook-dlq" resetKey="webhook-dlq">
          <WebhookDlqTab />
        </AdminSectionErrorBoundary>
      )}
      {tab === 'automation-stop-point' && canViewUsers && (
        <AdminSectionErrorBoundary name="automation-stop-point" resetKey="automation-stop-point">
          <AutomationStopPointTab />
        </AdminSectionErrorBoundary>
      )}
      {tab === 'rule-coverage' && canViewUsers && (
        <AdminSectionErrorBoundary name="rule-coverage" resetKey="rule-coverage">
          <RuleCoverageTab />
        </AdminSectionErrorBoundary>
      )}
      {tab === 'webhook-health' && canViewUsers && (
        <AdminSectionErrorBoundary name="webhook-health" resetKey="webhook-health">
          <WebhookHealthTab />
        </AdminSectionErrorBoundary>
      )}
    </div>
  );
}

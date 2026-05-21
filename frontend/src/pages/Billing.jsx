import React, { useEffect, useState, useCallback } from 'react';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import {
  CreditCard, RefreshCw, CheckCircle2, AlertTriangle, XCircle, Lock, Info,
} from 'lucide-react';
import api from '../lib/api';
import { cachedApiGetSWR, getCachedApiData } from '../lib/apiCache';
import { toast } from 'sonner';
import analytics from '../lib/analytics';
import { useAuth } from '../context/AuthContext';
import { useTranslation } from '../lib/i18n';
import {
  computeAllUsageRows,
  computeAccountRow,
  computeAutomationRow,
  isCurrentPlan,
  statusToTone,
} from '../lib/usage';

/**
 * Phase 2.3: Usage / Billing placeholder page.
 *
 * Reads /api/plan/current (plan + usage + limits) and /api/plans (plan
 * catalogue). Shows the user their current plan, this month's counters,
 * remaining quota, and plan tiers. Billing is not enabled.
 *
 * Privacy: only renders sanitized counters and labels — no raw text.
 */

const TONE_BG = {
  emerald: 'bg-emerald-100 text-emerald-700',
  amber:   'bg-amber-100 text-amber-800',
  rose:    'bg-rose-100 text-rose-700',
  slate:   'bg-slate-100 text-slate-600',
};

const TONE_BAR = {
  emerald: 'bg-emerald-500',
  amber:   'bg-amber-500',
  rose:    'bg-rose-500',
  slate:   'bg-slate-300',
};

function StatusPill({ status, ar }) {
  const tone = statusToTone(status);
  const cls = TONE_BG[tone] || TONE_BG.slate;
  const Icon = status === 'exceeded' ? XCircle
    : status === 'near_limit' ? AlertTriangle
    : status === 'unlimited' ? Info
    : CheckCircle2;
  const label = ar
    ? (status === 'exceeded' ? 'تجاوز الحدّ' : status === 'near_limit' ? 'قارب الحدّ' : status === 'unlimited' ? 'بلا حدود' : 'سليم')
    : (status === 'exceeded' ? 'Exceeded' : status === 'near_limit' ? 'Near limit' : status === 'unlimited' ? 'Unlimited' : 'OK');
  return (
    <Badge className={`${cls} border-0`}>
      <Icon className="w-3 h-3 me-1" /> {label}
    </Badge>
  );
}

function UsageBar({ row, ar }) {
  const tone = statusToTone(row.status);
  const barCls = TONE_BAR[tone] || TONE_BAR.slate;
  const percent = row.percent === null ? 0 : row.percent;
  return (
    <div className="bg-white rounded-2xl border border-slate-100 p-4" data-testid={`usage-row-${row.key || row.label}`}>
      <div className="flex items-center justify-between gap-2 mb-2">
        <div className="text-sm font-medium text-slate-700">{row.label}</div>
        <StatusPill status={row.status} ar={ar} />
      </div>
      <div className="flex items-baseline gap-2 text-xs text-slate-500 mb-2">
        <span className="text-base font-semibold text-slate-800 font-mono">{row.used}</span>
        {row.limit === null
          ? <span>{ar ? '/ بلا حدود' : '/ unlimited'}</span>
          : <>
              <span>/ {row.limit}</span>
              <span className="text-slate-400">·</span>
              <span>{row.remaining} {ar ? 'متبقّية' : 'remaining'}</span>
            </>
        }
      </div>
      {row.limit !== null && (
        <div className="h-1.5 bg-slate-100 rounded-full overflow-hidden">
          <div
            className={`h-full ${barCls} transition-all`}
            style={{ width: `${percent}%` }}
            data-testid={`usage-bar-${row.key || row.label}`}
          />
        </div>
      )}
    </div>
  );
}

function PlanCard({ plan, current, ar }) {
  const highlighted = isCurrentPlan(plan, current);
  const formatLimit = (n) => n === null || n === undefined ? (ar ? 'بلا حدود' : 'Unlimited') : n.toLocaleString();
  return (
    <section
      className={
        'rounded-2xl border p-5 flex flex-col gap-3 ' +
        (highlighted
          ? 'border-blue-500 bg-blue-50'
          : 'border-slate-100 bg-white')
      }
      data-testid={`plan-card-${plan.plan_key}`}
    >
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold text-slate-800">{plan.display_name}</h3>
          <div className="text-xs text-slate-500">
            ${plan.monthly_price_placeholder ?? 0}<span className="ms-1">{ar ? '/ شهر' : '/ month'}</span>
          </div>
        </div>
        {highlighted && (
          <Badge className="bg-blue-600 text-white border-0" data-testid="current-plan-badge">
            {ar ? 'الخطة الحالية' : 'Current plan'}
          </Badge>
        )}
      </div>
      <ul className="text-sm text-slate-600 space-y-1.5">
        <li>{formatLimit(plan.max_instagram_accounts)} {ar ? 'حسابات Instagram' : 'Instagram accounts'}</li>
        <li>{formatLimit(plan.max_active_automations)} {ar ? 'أتمتات نشطة' : 'active automations'}</li>
        <li>{formatLimit(plan.monthly_comments_processed_limit)} {ar ? 'تعليق معالَج / شهر' : 'comments processed / month'}</li>
        <li>{formatLimit(plan.monthly_public_replies_sent_limit)} {ar ? 'ردّ علني / شهر' : 'public replies / month'}</li>
        <li>{formatLimit(plan.monthly_dms_sent_limit)} {ar ? 'رسالة خاصة / شهر' : 'DMs / month'}</li>
      </ul>
      {plan.features && plan.features.length > 0 && (
        <ul className="text-xs text-slate-500 list-disc list-inside space-y-0.5">
          {plan.features.map((feature) => (
            <li key={feature}>{feature}</li>
          ))}
        </ul>
      )}
      <Button
        variant="outline"
        disabled
        title={ar ? 'ستتاح ترقية الخطة بعد تفعيل الفوترة' : 'Plan upgrades will be available after billing is enabled'}
        data-testid={`upgrade-btn-${plan.plan_key}`}
        className="mt-auto"
      >
        <Lock className="w-3 h-3 me-2" />
        {highlighted ? (ar ? 'الحالية' : 'Current') : (ar ? 'الترقية قريباً' : 'Upgrade coming soon')}
      </Button>
    </section>
  );
}

export default function Billing() {
  const { user } = useAuth();
  const { t, lang } = useTranslation();
  const billingCacheKey = `billing:plan-current:${user?.id || 'anon'}`;
  const plansCacheKey = `billing:plans:${user?.id || 'anon'}`;
  const [current, setCurrent] = useState(() => getCachedApiData(billingCacheKey) || null);
  const [plans, setPlans] = useState(() => getCachedApiData(plansCacheKey)?.plans || null);
  const [loading, setLoading] = useState(!(current && plans));
  const [error, setError] = useState(null);

  const load = useCallback(async ({ force = false } = {}) => {
    if (!(getCachedApiData(billingCacheKey) && getCachedApiData(plansCacheKey))) {
      setLoading(true);
    }
    setError(null);
    try {
      const [planResp, plansResp] = await Promise.all([
        cachedApiGetSWR(billingCacheKey, () => api.get('/plan/current', { timeout: 8000 }), {
          ttlMs: 60000,
          maxStaleMs: 5 * 60 * 1000,
          force,
          onUpdate: setCurrent,
        }),
        cachedApiGetSWR(plansCacheKey, () => api.get('/plans', { timeout: 8000 }), {
          ttlMs: 300000,
          maxStaleMs: 30 * 60 * 1000,
          force,
          onUpdate: (data) => setPlans(data?.plans || []),
        }),
      ]);
      setCurrent(planResp.data);
      setPlans(plansResp.data?.plans || []);
    } catch (err) {
      console.error('[Billing] load failed', err);
      const fallback = lang === 'ar' ? 'تعذّر تحميل بيانات الفوترة' : 'Failed to load billing info';
      const msg = err?.response?.data?.detail || fallback;
      setError(typeof msg === 'string' ? msg : fallback);
      toast.error(typeof msg === 'string' ? msg : fallback);
    } finally {
      setLoading(false);
    }
  }, [billingCacheKey, lang, plansCacheKey]);

  useEffect(() => {
    load();
    analytics.capture('billing_page_viewed', {});
  }, [load]);

  const usageRows = current ? computeAllUsageRows(current) : [];
  const accountRow = current ? computeAccountRow(current) : null;
  const automationRow = current ? computeAutomationRow(current) : null;

  return (
    <div className="p-4 sm:p-6 max-w-5xl mx-auto" data-testid="billing-page">
      <div className="flex flex-wrap items-end justify-between gap-3 mb-6">
        <div>
          <div className="flex items-center gap-2 text-slate-500 text-xs uppercase tracking-wide font-semibold mb-1">
            <CreditCard className="w-4 h-4" />
            {lang === 'ar' ? 'الفوترة والاستهلاك' : 'Billing & usage'}
          </div>
          <h1 className="text-3xl font-bold font-display">{t('billing.title')}</h1>
          <p className="text-slate-500 mt-1 text-sm">
            {t('billing.subtitle')}
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={() => load({ force: true })} disabled={loading}>
          <RefreshCw className={`w-4 h-4 me-2 ${loading ? 'animate-spin' : ''}`} />
          {t('common.refresh')}
        </Button>
      </div>

      {/* Billing-not-enabled banner */}
      <div
        className="mb-6 rounded-2xl border border-amber-200 bg-amber-50 p-4 flex items-start gap-3"
        data-testid="billing-disabled-banner"
      >
        <Lock className="w-4 h-4 text-amber-700 mt-0.5 shrink-0" />
        <div className="text-sm text-amber-900">
          <div className="font-semibold mb-1">{t('billing.billingNotEnabled')}</div>
          <div className="text-amber-800">
            {t('billing.contactSupport')}
          </div>
        </div>
      </div>

      {error && (
        <div className="mb-4 rounded-xl border border-rose-200 bg-rose-50 p-3 text-sm text-rose-700">
          <AlertTriangle className="inline w-4 h-4 me-1" />
          {error}
        </div>
      )}

      {loading && !current && (
        <div className="text-center py-16 text-slate-500">{t('common.loading')}</div>
      )}

      {current && (
        <>
          {/* Current plan summary */}
          <section className="mb-6 rounded-2xl border border-slate-100 bg-white p-5">
            <div className="flex items-center justify-between mb-2">
              <div>
                <div className="text-xs text-slate-500 uppercase tracking-wide font-semibold">
                  {t('billing.currentPlan')}
                </div>
                <div className="text-2xl font-bold text-slate-800" data-testid="current-plan-name">
                  {current.display_name || current.plan_key || 'Free'}
                </div>
                <div className="text-xs text-slate-500 mt-1">
                  {lang === 'ar' ? 'الشهر:' : 'Month:'} <span className="font-mono">{current.event_month}</span>
                  <span className="ms-3">
                    {lang === 'ar' ? 'الفوترة:' : 'Billing:'}{' '}
                    <span className="font-semibold text-slate-700">
                      {current.billing_enabled
                        ? (lang === 'ar' ? 'مُفعّلة' : 'Enabled')
                        : (lang === 'ar' ? 'غير مُفعّلة بعد' : 'Not enabled yet')}
                    </span>
                  </span>
                  {current.current_period_end && (
                    <span className="ms-3" data-testid="plan-period-end">
                      {current.plan_expired
                        ? (lang === 'ar' ? 'انتهت الخطة في: ' : 'Expired on: ')
                        : (lang === 'ar' ? 'تنتهي في: ' : 'Renews / expires: ')}
                      <span className="font-mono">{new Date(current.current_period_end).toLocaleString()}</span>
                    </span>
                  )}
                </div>
                {current.plan_expired && (
                  <div className="mt-2 inline-flex items-center gap-2 text-xs text-rose-700 bg-rose-50 border border-rose-200 rounded-md px-2 py-1"
                       data-testid="plan-expired-banner">
                    <AlertTriangle className="w-3 h-3" />
                    {lang === 'ar'
                      ? `انتهت صلاحية خطتك (${current.expired_plan_key}). تواصل مع الدعم للتجديد.`
                      : `Your ${current.expired_plan_key} plan expired. Contact support to renew.`}
                  </div>
                )}
              </div>
            </div>
          </section>

          {/* Counter cards */}
          <section className="mb-6">
            <h2 className="text-sm font-semibold text-slate-700 mb-3">{t('billing.thisMonth')}</h2>
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
              {accountRow && <UsageBar row={accountRow} ar={lang === "ar"} />}
              {automationRow && <UsageBar row={automationRow} ar={lang === "ar"} />}
              {usageRows.map((row) => (
                <UsageBar key={row.key} row={row} ar={lang === "ar"} />
              ))}
            </div>
          </section>

          {/* Per-Instagram-account usage breakdown */}
          {Array.isArray(current.per_account_counters) && current.per_account_counters.length > 0 && (
            <section className="mb-6" data-testid="per-account-usage">
              <h2 className="text-sm font-semibold text-slate-700 mb-3">
                {lang === 'ar' ? 'الاستهلاك لكل حساب Instagram' : 'Usage per Instagram account'}
              </h2>
              <div className="overflow-x-auto rounded-2xl border border-slate-100 bg-white">
                <table className="w-full text-sm">
                  <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
                    <tr>
                      <th className="text-start px-3 py-2">{lang === 'ar' ? 'الحساب' : 'Account'}</th>
                      <th className="text-end px-3 py-2">{lang === 'ar' ? 'تعليقات' : 'Comments'}</th>
                      <th className="text-end px-3 py-2">{lang === 'ar' ? 'ردود علنية' : 'Public replies'}</th>
                      <th className="text-end px-3 py-2">{lang === 'ar' ? 'رسائل خاصة' : 'DMs'}</th>
                      <th className="text-end px-3 py-2">{lang === 'ar' ? 'نقرات الرابط' : 'Link clicks'}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {current.per_account_counters.map((row) => {
                      const c = row.counters || {};
                      return (
                        <tr key={row.instagramAccountId}
                            className="border-t border-slate-100"
                            data-testid={`per-account-row-${row.instagramAccountId}`}>
                          <td className="px-3 py-2">
                            <div className="font-semibold text-slate-700">@{row.username || row.instagramAccountId}</div>
                            <div className="text-xs text-slate-400 font-mono">{row.instagramAccountId}</div>
                          </td>
                          <td className="text-end px-3 py-2 font-mono">{c.comments_processed || 0}</td>
                          <td className="text-end px-3 py-2 font-mono">{c.public_replies_sent || 0}</td>
                          <td className="text-end px-3 py-2 font-mono">{c.dms_sent || 0}</td>
                          <td className="text-end px-3 py-2 font-mono">{c.links_clicked || 0}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
              <p className="text-xs text-slate-400 mt-2">
                {lang === 'ar'
                  ? 'الأرقام هنا منفصلة لكل حساب Instagram لتسهيل تتبّع الاستهلاك حين تربط أكثر من حساب.'
                  : 'These counters are split per Instagram account so you can attribute usage when multiple accounts are linked.'}
              </p>
            </section>
          )}

          {/* Plan catalogue */}
          {plans && plans.length > 0 && (
            <section className="mb-6">
              <h2 className="text-sm font-semibold text-slate-700 mb-3">{t('billing.plans')}</h2>
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                {plans.map((plan) => (
                  <PlanCard key={plan.plan_key} plan={plan} current={current} ar={lang === 'ar'} />
                ))}
              </div>
              <p className="text-xs text-slate-400 mt-3">
                {lang === 'ar'
                  ? 'ستتاح ترقية الخطة بعد تفعيل الفوترة. خلال الفترة التجريبية، تواصل مع الدعم لتغيير الخطة.'
                  : 'Plan upgrades will be available after billing is enabled. During beta, contact support to change your plan.'}
              </p>
            </section>
          )}
        </>
      )}
    </div>
  );
}

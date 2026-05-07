import React, { useEffect, useState, useCallback } from 'react';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import {
  CreditCard, RefreshCw, CheckCircle2, AlertTriangle, XCircle, Lock, Info,
} from 'lucide-react';
import api from '../lib/api';
import { toast } from 'sonner';
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

function StatusPill({ status }) {
  const tone = statusToTone(status);
  const cls = TONE_BG[tone] || TONE_BG.slate;
  const Icon = status === 'exceeded' ? XCircle
    : status === 'near_limit' ? AlertTriangle
    : status === 'unlimited' ? Info
    : CheckCircle2;
  const label =
    status === 'exceeded' ? 'Exceeded'
    : status === 'near_limit' ? 'Near limit'
    : status === 'unlimited' ? 'Unlimited'
    : 'OK';
  return (
    <Badge className={`${cls} border-0`}>
      <Icon className="w-3 h-3 mr-1" /> {label}
    </Badge>
  );
}

function UsageBar({ row }) {
  const tone = statusToTone(row.status);
  const barCls = TONE_BAR[tone] || TONE_BAR.slate;
  const percent = row.percent === null ? 0 : row.percent;
  return (
    <div className="bg-white rounded-2xl border border-slate-100 p-4" data-testid={`usage-row-${row.key || row.label}`}>
      <div className="flex items-center justify-between gap-2 mb-2">
        <div className="text-sm font-medium text-slate-700">{row.label}</div>
        <StatusPill status={row.status} />
      </div>
      <div className="flex items-baseline gap-2 text-xs text-slate-500 mb-2">
        <span className="text-base font-semibold text-slate-800 font-mono">{row.used}</span>
        {row.limit === null
          ? <span>/ unlimited</span>
          : <>
              <span>/ {row.limit}</span>
              <span className="text-slate-400">·</span>
              <span>{row.remaining} remaining</span>
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

function PlanCard({ plan, current }) {
  const highlighted = isCurrentPlan(plan, current);
  const formatLimit = (n) => n === null || n === undefined ? 'Unlimited' : n.toLocaleString();
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
            ${plan.monthly_price_placeholder ?? 0}<span className="ml-1">/ month placeholder</span>
          </div>
        </div>
        {highlighted && (
          <Badge className="bg-blue-600 text-white border-0" data-testid="current-plan-badge">
            Current plan
          </Badge>
        )}
      </div>
      <ul className="text-sm text-slate-600 space-y-1.5">
        <li>{formatLimit(plan.max_instagram_accounts)} Instagram accounts</li>
        <li>{formatLimit(plan.max_active_automations)} active automations</li>
        <li>{formatLimit(plan.monthly_comments_processed_limit)} comments processed / month</li>
        <li>{formatLimit(plan.monthly_public_replies_sent_limit)} public replies / month</li>
        <li>{formatLimit(plan.monthly_dms_sent_limit)} DMs / month</li>
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
        title="Plan upgrades will be available after billing is enabled"
        data-testid={`upgrade-btn-${plan.plan_key}`}
        className="mt-auto"
      >
        <Lock className="w-3 h-3 mr-2" />
        {highlighted ? 'Current' : 'Upgrade coming soon'}
      </Button>
    </section>
  );
}

export default function Billing() {
  const [current, setCurrent] = useState(null);
  const [plans, setPlans] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [planResp, plansResp] = await Promise.all([
        api.get('/plan/current'),
        api.get('/plans'),
      ]);
      setCurrent(planResp.data);
      setPlans(plansResp.data?.plans || []);
    } catch (err) {
      console.error('[Billing] load failed', err);
      const msg = err?.response?.data?.detail || 'Failed to load billing info';
      setError(typeof msg === 'string' ? msg : 'Failed to load billing info');
      toast.error(typeof msg === 'string' ? msg : 'Failed to load billing info');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
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
            Billing & usage
          </div>
          <h1 className="text-3xl font-bold font-display">Billing</h1>
          <p className="text-slate-500 mt-1 text-sm">
            Your current plan, this month's usage, and remaining quota.
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={load} disabled={loading}>
          <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </div>

      {/* Billing-not-enabled banner */}
      <div
        className="mb-6 rounded-2xl border border-amber-200 bg-amber-50 p-4 flex items-start gap-3"
        data-testid="billing-disabled-banner"
      >
        <Lock className="w-4 h-4 text-amber-700 mt-0.5 shrink-0" />
        <div className="text-sm text-amber-900">
          <div className="font-semibold mb-1">Billing is not enabled yet.</div>
          <div className="text-amber-800">
            Your current plan limits are enforced to keep the beta stable.
            Contact support during beta to change your plan. Plan upgrades
            with payment will be available later.
          </div>
        </div>
      </div>

      {error && (
        <div className="mb-4 rounded-xl border border-rose-200 bg-rose-50 p-3 text-sm text-rose-700">
          <AlertTriangle className="inline w-4 h-4 mr-1" />
          {error}
        </div>
      )}

      {loading && !current && (
        <div className="text-center py-16 text-slate-500">Loading…</div>
      )}

      {current && (
        <>
          {/* Current plan summary */}
          <section className="mb-6 rounded-2xl border border-slate-100 bg-white p-5">
            <div className="flex items-center justify-between mb-2">
              <div>
                <div className="text-xs text-slate-500 uppercase tracking-wide font-semibold">
                  Current plan
                </div>
                <div className="text-2xl font-bold text-slate-800" data-testid="current-plan-name">
                  {current.display_name || current.plan_key || 'Free'}
                </div>
                <div className="text-xs text-slate-500 mt-1">
                  Month: <span className="font-mono">{current.event_month}</span>
                  <span className="ml-3">
                    Billing:{' '}
                    <span className="font-semibold text-slate-700">
                      {current.billing_enabled ? 'Enabled' : 'Not enabled yet'}
                    </span>
                  </span>
                </div>
              </div>
            </div>
          </section>

          {/* Counter cards */}
          <section className="mb-6">
            <h2 className="text-sm font-semibold text-slate-700 mb-3">This month</h2>
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
              {accountRow && <UsageBar row={accountRow} />}
              {automationRow && <UsageBar row={automationRow} />}
              {usageRows.map((row) => (
                <UsageBar key={row.key} row={row} />
              ))}
            </div>
          </section>

          {/* Plan catalogue */}
          {plans && plans.length > 0 && (
            <section className="mb-6">
              <h2 className="text-sm font-semibold text-slate-700 mb-3">Plans</h2>
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                {plans.map((plan) => (
                  <PlanCard key={plan.plan_key} plan={plan} current={current} />
                ))}
              </div>
              <p className="text-xs text-slate-400 mt-3">
                Plan upgrades will be available after billing is enabled.
                During beta, contact support to change your plan.
              </p>
            </section>
          )}
        </>
      )}
    </div>
  );
}

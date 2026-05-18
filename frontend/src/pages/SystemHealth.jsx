import React, { useEffect, useState, useCallback } from 'react';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import {
  RefreshCw, Activity, CheckCircle2, AlertTriangle, XCircle,
  Webhook, Database, Cpu, Clock,
} from 'lucide-react';
import api from '../lib/api';
import { toast } from 'sonner';
import { loadFrontendRuntimeStatus } from '../lib/frontendRuntimeStatus';
import { useTranslation } from '../lib/i18n';

/**
 * Authenticated operations health page.
 *
 * Surfaces the data already exposed by GET /api/instagram/automation-health
 * (background tasks, webhook last-received/processed, account token health,
 * pending/failed job counts). No tokens or message bodies are shown — the
 * backend already strips them before this page sees them.
 */

function fmtTime(iso) {
  if (!iso) return '—';
  try {
    return new Date(iso).toLocaleString();
  } catch (_) {
    return iso;
  }
}

function ageSeconds(iso) {
  if (!iso) return null;
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return null;
  return Math.max(0, Math.round((Date.now() - t) / 1000));
}

function statusOf(task, ar) {
  if (!task) return { tone: 'slate', label: ar ? 'غير معروف' : 'unknown' };
  if (!task.running) {
    return { tone: 'rose', label: ar ? 'متوقّفة' : 'stopped' };
  }
  if ((task.consecutive_failures || 0) >= 3) {
    return { tone: 'orange', label: ar ? 'تعمل بصعوبة' : 'degraded' };
  }
  return { tone: 'emerald', label: ar ? 'تعمل' : 'running' };
}

function StatusDot({ tone }) {
  const map = {
    emerald: 'bg-emerald-500',
    orange: 'bg-orange-500',
    rose: 'bg-rose-500',
    slate: 'bg-slate-300',
  };
  return <span className={`inline-block w-2.5 h-2.5 rounded-full ${map[tone] || map.slate}`} />;
}

function Section({ title, icon: Icon, children, hint }) {
  return (
    <section className="bg-white rounded-2xl border border-slate-100 p-5">
      <div className="flex items-center gap-2 mb-3">
        {Icon && <Icon className="w-4 h-4 text-slate-500" />}
        <h2 className="text-sm font-semibold text-slate-700">{title}</h2>
        {hint && <span className="text-xs text-slate-400">{hint}</span>}
      </div>
      {children}
    </section>
  );
}

const SystemHealth = () => {
  const { lang } = useTranslation();
  const ar = lang === 'ar';
  const [data, setData] = useState(null);
  const [observability, setObservability] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [refreshedAt, setRefreshedAt] = useState(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const { data } = await api.get('/instagram/automation-health');
      setData(data);
      setRefreshedAt(new Date());
    } catch (err) {
      console.error('[SystemHealth] load failed', err);
      setError(err?.response?.data?.detail || (ar ? 'تعذّر تحميل حالة النظام' : 'Failed to load system health'));
      toast.error(ar ? 'تعذّر تحميل حالة النظام' : 'Failed to load system health');
    } finally {
      setLoading(false);
    }
    // Observability status is best-effort and DSN/key-free by contract.
    try {
      const { data: obs } = await api.get('/observability/status');
      // Layer in frontend's runtime-known config (DSN/key never echoed).
      const fe = await loadFrontendRuntimeStatus();
      setObservability({ ...obs, frontend_runtime: fe });
    } catch (_) {
      setObservability(null);
    }
  }, []);

  useEffect(() => {
    load();
    // Refresh every 30s in the background. No exponential storms — the
    // page is rarely the focused tab.
    const id = setInterval(load, 30000);
    return () => clearInterval(id);
  }, [load]);

  const tasks = (data && data.tasks) || {};
  const webhook = (data && data.webhook) || {};
  const accounts = (data && data.accounts) || [];
  const jobs = (data && data.jobs) || {};
  const config = (data && data.config) || {};

  const webhookAgeRecv = ageSeconds(webhook.last_received_at);
  const webhookAgeProc = ageSeconds(webhook.last_processed_at);

  return (
    <div className="p-4 sm:p-6 max-w-5xl mx-auto">
      <div className="flex flex-wrap items-end justify-between gap-3 mb-6">
        <div>
          <h1 className="text-3xl font-bold font-display">{ar ? 'حالة النظام' : 'System Health'}</h1>
          <p className="text-slate-500 mt-1 text-sm">
            {ar ? 'حالة التشغيل المباشرة. لا تظهر هنا أي رموز أو محتوى رسائل.' : 'Live operations status. Tokens and message bodies are never shown.'}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-xs text-slate-400">
            {ar ? 'تحديث تلقائي: 30 ثانية' : 'Auto-refresh: 30s'} {refreshedAt && <>• {ar ? 'آخر تحديث' : 'last'} {fmtTime(refreshedAt)}</>}
          </span>
          <Button variant="outline" size="sm" onClick={load} disabled={loading}>
            <RefreshCw className={`w-4 h-4 me-2 ${loading ? 'animate-spin' : ''}`} />
            {ar ? 'تحديث' : 'Refresh'}
          </Button>
        </div>
      </div>

      {error && (
        <div className="mb-4 rounded-xl border border-rose-200 bg-rose-50 p-3 text-sm text-rose-700">
          <AlertTriangle className="inline w-4 h-4 me-1" />
          {error}
        </div>
      )}

      <div className="grid gap-4 md:grid-cols-2">
        <Section title={ar ? 'الـWebhook' : 'Webhook'} icon={Webhook}>
          <div className="space-y-2 text-sm">
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'آخر استلام' : 'Last received'}</span>
              <span className="font-mono text-slate-700">
                {fmtTime(webhook.last_received_at)}
                {webhookAgeRecv !== null && (
                  <span className="text-xs text-slate-400 ms-2">{ar ? `(منذ ${webhookAgeRecv} ث)` : `(${webhookAgeRecv}s ago)`}</span>
                )}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'آخر معالجة' : 'Last processed'}</span>
              <span className="font-mono text-slate-700">
                {fmtTime(webhook.last_processed_at)}
                {webhookAgeProc !== null && (
                  <span className="text-xs text-slate-400 ms-2">{ar ? `(منذ ${webhookAgeProc} ث)` : `(${webhookAgeProc}s ago)`}</span>
                )}
              </span>
            </div>
          </div>
        </Section>

        <Section title={ar ? 'ربط Instagram' : 'Instagram connection'} icon={Activity}>
          {accounts.length === 0 && (
            <div className="text-sm text-slate-500">{ar ? 'لا يوجد حساب Instagram نشط.' : 'No active Instagram account.'}</div>
          )}
          {accounts.map((a, i) => (
            <div key={i} className="space-y-2 text-sm">
              <div className="flex items-center justify-between">
                <span className="text-slate-500">{ar ? 'الحساب النشط' : 'Active account'}</span>
                <span className="font-mono text-slate-700">
                  {a.instagramAccountId || '—'}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-slate-500">{ar ? 'الاتصال' : 'Connection'}</span>
                <span className="flex items-center gap-2">
                  {a.instagramConnected && a.connectionValid
                    ? <><CheckCircle2 className="w-4 h-4 text-emerald-500" /> {ar ? 'صالح' : 'valid'}</>
                    : <><XCircle className="w-4 h-4 text-rose-500" /> {ar ? 'يلزم إعادة الربط' : 'reconnect required'}</>}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-slate-500">{ar ? 'الرمز متوفّر' : 'Token present'}</span>
                <span>{a.tokenPresent ? (ar ? 'نعم' : 'yes') : (ar ? 'لا' : 'no')}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-slate-500">{ar ? 'طريقة المصادقة' : 'Auth method'}</span>
                <span className="font-mono">{a.auth_kind || '—'}</span>
              </div>
            </div>
          ))}
        </Section>

        <Section title={ar ? 'المهام الخلفية' : 'Background tasks'} icon={Cpu} hint={ar ? 'الـpoller والـwatchdog ومُتحقّق المتابعة' : 'poller, watchdog, follow-verifier'}>
          <div className="space-y-2">
            {Object.entries(tasks).map(([name, t]) => {
              const s = statusOf(t, ar);
              return (
                <div key={name} className="flex items-center justify-between text-sm">
                  <span className="flex items-center gap-2">
                    <StatusDot tone={s.tone} />
                    <span className="font-mono text-slate-700">{name}</span>
                    <Badge className="bg-slate-100 text-slate-700 border-0 text-[10px]">{s.label}</Badge>
                  </span>
                  <span className="text-xs text-slate-500">
                    {ar ? `إعادات: ${t.restarts || 0} • إخفاقات: ${t.consecutive_failures || 0}` : `restarts: ${t.restarts || 0} • fails: ${t.consecutive_failures || 0}`}
                  </span>
                </div>
              );
            })}
            {Object.keys(tasks).length === 0 && (
              <div className="text-sm text-slate-500">{ar ? 'لا توجد مهام خلفية مُسجّلة.' : 'No background tasks reported.'}</div>
            )}
          </div>
        </Section>

        <Section title={ar ? 'طابور رسائل التعليقات' : 'Comment-DM job queue'} icon={Database}>
          <div className="space-y-2 text-sm">
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'في الانتظار' : 'Pending'}</span>
              <span className="font-mono text-slate-700">
                {jobs.pending_comment_dm_sessions ?? 0}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'فشل' : 'Failed'}</span>
              <span className="font-mono text-slate-700">
                {jobs.failed_comment_dm_sessions ?? 0}
              </span>
            </div>
          </div>
        </Section>

        <Section title={ar ? 'الإعدادات' : 'Configuration'} icon={Clock}>
          <div className="space-y-2 text-sm">
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'فترة فحص التعليقات' : 'Comment poller interval'}</span>
              <span className="font-mono text-slate-700">
                {config.comment_poller_interval_seconds ?? '—'}{ar ? ' ث' : 's'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'فاحص التعليقات مُفعّل' : 'Comment poller enabled'}</span>
              <span>{config.comment_poller_enabled ? (ar ? 'نعم' : 'yes') : (ar ? 'لا' : 'no')}</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'فترة التحقّق من المتابعة' : 'Follow verifier interval'}</span>
              <span className="font-mono text-slate-700">
                {config.follow_verifier_interval_seconds ?? '—'}{ar ? ' ث' : 's'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'فترة الـwatchdog' : 'Watchdog interval'}</span>
              <span className="font-mono text-slate-700">
                {config.watchdog_interval_seconds ?? '—'}{ar ? ' ث' : 's'}
              </span>
            </div>
          </div>
        </Section>

        <Section title={ar ? 'المراقبة' : 'Observability'} icon={Activity} hint="Sentry + PostHog">
          <div className="space-y-2 text-sm" data-testid="observability-section">
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'Sentry (الخلفية)' : 'Backend Sentry'}</span>
              <span className="flex items-center gap-2">
                {observability?.backend?.sentry_configured
                  ? <><CheckCircle2 className="w-4 h-4 text-emerald-500" /> {ar ? 'مُهيّأ' : 'configured'}</>
                  : <><XCircle className="w-4 h-4 text-slate-400" /> {ar ? 'غير مُهيّأ' : 'not configured'}</>}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'Sentry (الواجهة)' : 'Frontend Sentry'}</span>
              <span className="flex items-center gap-2">
                {observability?.frontend_runtime?.sentry_configured
                  ? <><CheckCircle2 className="w-4 h-4 text-emerald-500" /> {ar ? 'مُهيّأ' : 'configured'}</>
                  : <><XCircle className="w-4 h-4 text-slate-400" /> {ar ? 'غير مُهيّأ' : 'not configured'}</>}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'PostHog (الواجهة)' : 'PostHog (frontend)'}</span>
              <span className="flex items-center gap-2">
                {observability?.frontend_runtime?.posthog_configured
                  ? <><CheckCircle2 className="w-4 h-4 text-emerald-500" /> {ar ? 'مُهيّأ' : 'configured'}</>
                  : <><XCircle className="w-4 h-4 text-slate-400" /> {ar ? 'غير مُهيّأ' : 'not configured'}</>}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'تسجيل الدخول عبر Google مُهيّأ' : 'Google Sign-In configured'}</span>
              <span className="flex items-center gap-2" data-testid="google-signin-configured">
                {observability?.frontend_runtime?.google_sign_in_configured
                  ? <><CheckCircle2 className="w-4 h-4 text-emerald-500" /> {ar ? 'نعم' : 'yes'}</>
                  : <><XCircle className="w-4 h-4 text-slate-400" /> {ar ? 'لا' : 'no'}</>}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'طلب إعدادات Google' : 'Google config request'}</span>
              <span className="font-mono text-xs text-slate-700" data-testid="google-config-request-status">
                {observability?.frontend_runtime?.google_config_request_attempted ? 'attempted' : 'not_attempted'}
                {' / '}
                {observability?.frontend_runtime?.google_config_request_ok ? 'ok' : 'not_ok'}
                {' / '}
                {observability?.frontend_runtime?.google_config_response_was_json ? 'json' : 'not_json'}
                {' / '}
                {observability?.frontend_runtime?.google_config_error_code || 'none'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'البيئة' : 'Environment'}</span>
              <span className="font-mono text-slate-700">
                {observability?.backend?.environment || '—'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'بصمة بناء الخلفية' : 'Backend build sha'}</span>
              <span className="font-mono text-slate-700">
                {observability?.backend?.build_sha || '—'}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">{ar ? 'بصمة بناء الواجهة' : 'Frontend build sha'}</span>
              <span className="font-mono text-slate-700">
                {observability?.frontend_runtime?.build_sha || '—'}
              </span>
            </div>
            <p className="text-xs text-slate-400 pt-2 border-t border-slate-100">
              {ar ? 'لا يتم إرجاع قيم DSN أو مفاتيح الـAPI من الخادم ولا تُعرض هنا أبداً.' : 'DSN values and API keys are never returned by the API or rendered here.'}
            </p>
          </div>
        </Section>
      </div>
    </div>
  );
};

export default SystemHealth;

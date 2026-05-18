/**
 * Phase 2.18Z — public service-status page.
 *
 * No auth required. Polls /api/status every 30 seconds and renders
 * each subsystem as a labelled tile in the style of
 * status.openai.com / status.stripe.com.
 */
import React, { useEffect, useState, useCallback } from 'react';
import { Link } from 'react-router-dom';
import { CheckCircle2, AlertTriangle, XCircle, HelpCircle, RefreshCw } from 'lucide-react';
import { API_BASE } from '../lib/api';
import { useTranslation } from '../lib/i18n';

const STATUS_LABELS = {
  en: { operational: 'Operational', partial_outage: 'Partial outage', major_outage: 'Major outage', unknown: 'Unknown' },
  ar: { operational: 'تعمل بشكل طبيعي', partial_outage: 'انقطاع جزئي', major_outage: 'انقطاع كبير', unknown: 'غير معروف' },
};

const STATUS_STYLE = {
  operational: { chip: 'bg-emerald-100 text-emerald-800 border-emerald-200', Icon: CheckCircle2, iconClass: 'text-emerald-600' },
  partial_outage: { chip: 'bg-amber-100 text-amber-800 border-amber-200', Icon: AlertTriangle, iconClass: 'text-amber-600' },
  major_outage: { chip: 'bg-rose-100 text-rose-800 border-rose-200', Icon: XCircle, iconClass: 'text-rose-600' },
  unknown: { chip: 'bg-slate-100 text-slate-800 border-slate-200', Icon: HelpCircle, iconClass: 'text-slate-500' },
};

function StatusChip({ status, labels }) {
  const s = STATUS_STYLE[status] || STATUS_STYLE.unknown;
  const Icon = s.Icon;
  return (
    <span className={`inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-medium ${s.chip}`}>
      <Icon className="w-3.5 h-3.5" />
      {labels[status] || labels.unknown}
    </span>
  );
}

const BANNER_CONTENT = {
  en: {
    operational: { title: 'All systems operational', body: 'Every MyChat subsystem is responding normally.' },
    partial_outage: { title: 'Partial service disruption', body: 'Some MyChat subsystems are degraded. Automations may run slower than usual.' },
    major_outage: { title: 'Major outage', body: 'One or more critical subsystems are unavailable. We are investigating.' },
    unknown: { title: 'Status unavailable', body: 'We could not determine the service status right now.' },
  },
  ar: {
    operational: { title: 'جميع الأنظمة تعمل بشكل طبيعي', body: 'جميع خدمات MyChat تستجيب بشكل طبيعي.' },
    partial_outage: { title: 'انقطاع جزئي في الخدمة', body: 'بعض خدمات MyChat تواجه تأخّراً. قد تعمل الأتمتات أبطأ من المعتاد.' },
    major_outage: { title: 'انقطاع كبير', body: 'إحدى الخدمات الأساسية غير متاحة. نحن نحقّق في الأمر.' },
    unknown: { title: 'لا تتوفّر بيانات الحالة', body: 'تعذّر علينا تحديد حالة الخدمة في هذه اللحظة.' },
  },
};

const BANNER_STYLE = {
  operational: { className: 'bg-emerald-50 border-emerald-200 text-emerald-900', Icon: CheckCircle2, iconClass: 'text-emerald-600' },
  partial_outage: { className: 'bg-amber-50 border-amber-200 text-amber-900', Icon: AlertTriangle, iconClass: 'text-amber-600' },
  major_outage: { className: 'bg-rose-50 border-rose-200 text-rose-900', Icon: XCircle, iconClass: 'text-rose-600' },
  unknown: { className: 'bg-slate-50 border-slate-200 text-slate-900', Icon: HelpCircle, iconClass: 'text-slate-500' },
};

export default function StatusPage() {
  const { lang } = useTranslation();
  const ar = lang === 'ar';
  const statusLabels = STATUS_LABELS[ar ? 'ar' : 'en'];
  const bannerContent = BANNER_CONTENT[ar ? 'ar' : 'en'];
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const load = useCallback(async () => {
    setError('');
    try {
      const r = await fetch(`${API_BASE}/status`, { cache: 'no-store' });
      if (!r.ok) throw new Error('bad status response');
      setData(await r.json());
    } catch (e) {
      setError(ar ? 'تعذّر الوصول إلى خدمة الحالة.' : 'Could not reach the status endpoint.');
    } finally {
      setLoading(false);
    }
  }, [ar]);

  useEffect(() => {
    load();
    const t = setInterval(load, 30_000);
    return () => clearInterval(t);
  }, [load]);

  const overall = data?.overall_status || 'unknown';
  const banner = { ...BANNER_STYLE[overall] || BANNER_STYLE.unknown, ...(bannerContent[overall] || bannerContent.unknown) };
  const BannerIcon = banner.Icon;

  return (
    <div className="min-h-screen bg-slate-50">
      <div className="max-w-3xl mx-auto px-4 py-10">
        <header className="mb-8 flex items-center justify-between">
          <Link to="/" className="flex items-center gap-2 text-slate-900">
            <span className="font-display font-extrabold text-xl">mychat</span>
            <span className="text-xs text-slate-500 font-mono">{ar ? 'الحالة' : 'status'}</span>
          </Link>
          <button
            type="button"
            onClick={load}
            className="inline-flex items-center gap-1 text-xs text-slate-600 hover:text-slate-900"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
            {ar ? 'تحديث' : 'Refresh'}
          </button>
        </header>

        <div className={`rounded-2xl border p-6 ${banner.className}`}>
          <div className="flex items-start gap-3">
            <BannerIcon className={`w-6 h-6 mt-0.5 ${banner.iconClass}`} />
            <div>
              <h1 className="font-display font-bold text-xl">{banner.title}</h1>
              <p className="text-sm mt-1">{banner.body}</p>
            </div>
          </div>
        </div>

        {error && (
          <div className="mt-4 rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm text-slate-600">
            {error}
          </div>
        )}

        <div className="mt-6 bg-white rounded-2xl border border-slate-100 overflow-hidden">
          <ul className="divide-y divide-slate-100">
            {(data?.components || []).map((c) => (
              <li key={c.key} className="flex items-center justify-between px-5 py-4 gap-4">
                <div className="min-w-0">
                  <div className="font-medium text-slate-900">{c.name}</div>
                  {c.detail && <div className="text-xs text-slate-500 mt-0.5">{c.detail}</div>}
                </div>
                <StatusChip status={c.status} labels={statusLabels} />
              </li>
            ))}
            {loading && !data && (
              <li className="px-5 py-6 text-sm text-slate-500 text-center">{ar ? 'جارٍ فحص الحالة…' : 'Checking status…'}</li>
            )}
          </ul>
        </div>

        {data?.checked_at && (
          <p className="mt-4 text-xs text-slate-400 text-center font-mono">
            {ar ? 'آخر فحص: ' : 'checked at '}{data.checked_at.replace('T', ' ').slice(0, 19)} UTC
          </p>
        )}

        <p className="mt-8 text-xs text-slate-400 text-center">
          {ar ? 'للحوادث غير المُسجّلة هنا، تواصل عبر ' : 'For incidents not yet reflected here, email '}
          <a href="mailto:mm.mohame172000@gmail.com" className="underline">{ar ? 'الدعم' : 'support'}</a>.
        </p>
      </div>
    </div>
  );
}

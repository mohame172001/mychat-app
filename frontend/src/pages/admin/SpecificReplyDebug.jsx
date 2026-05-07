import React, { useEffect, useState, useCallback } from 'react';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Badge } from '../../components/ui/badge';
import {
  ShieldAlert, CheckCircle2, XCircle, RefreshCw, Wrench, PlayCircle, Eye,
} from 'lucide-react';
import api from '../../lib/api';
import { toast } from 'sonner';
import { diagnosisPassFail } from '../../lib/specificReplyDiagnosis';

/**
 * Phase 1.4H admin debug page — TEMPORARY support tool.
 *
 * Not a product feature, not advertised in the sidebar, and reachable
 * only by typing the URL. The page first probes /api/admin/tools-enabled
 * and renders a clear "disabled" state when the backend has
 * ENABLE_ADMIN_REPAIR_TOOLS=false (the default in production). This
 * page should NOT be linked from anywhere user-facing.
 *
 * Privacy:
 *   The backend never returns raw comment / reply / DM text. This UI
 *   only renders length/hash/booleans.
 *
 * PASS labels here describe internal consistency of one comment's
 * state vs. its rule's saved configuration. PASS does NOT mean
 * "automation healthy overall" or "Phase 1.4 closed".
 */

// Debug-only convenience: the original failed id from the Phase 1.4
// investigation. Hidden in production builds so end-operators don't see
// project-internal ids.
const FAILED_COMMENT_ID = '18004285310876247';
const IS_DEV_BUILD = process.env.NODE_ENV !== 'production';

function StatusPill({ ok, label }) {
  const cls = ok
    ? 'bg-emerald-100 text-emerald-700'
    : 'bg-rose-100 text-rose-700';
  const Icon = ok ? CheckCircle2 : XCircle;
  return (
    <Badge className={`${cls} border-0`}>
      <Icon className="w-3 h-3 mr-1" /> {label}
    </Badge>
  );
}

function Field({ label, value, mono = true }) {
  const display = value === null || value === undefined || value === ''
    ? <span className="text-slate-400">—</span>
    : (mono ? <span className="font-mono">{String(value)}</span> : String(value));
  return (
    <div className="flex justify-between gap-3 text-sm py-1 border-b border-slate-100 last:border-b-0">
      <span className="text-slate-500">{label}</span>
      <span className="text-slate-700 text-right break-all">{display}</span>
    </div>
  );
}

function DiagnosisCard({ data }) {
  if (!data) return null;
  const verdict = diagnosisPassFail(data);
  const allPass = verdict.pass;
  const anyForbidden = data.forbidden_state_detected === true;

  return (
    <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
      <div className="flex items-center justify-between mb-3">
        <div className="flex items-center gap-2">
          <Eye className="w-4 h-4 text-slate-500" />
          <h3 className="text-sm font-semibold text-slate-700">Diagnosis</h3>
        </div>
        <div data-testid="diagnosis-pass-fail">
          {allPass
            ? <StatusPill ok label={verdict.label || 'Consistent'} />
            : <StatusPill ok={false} label={anyForbidden ? 'FAIL — forbidden state' : `FAIL — ${verdict.label || verdict.reason}`} />}
        </div>
      </div>

      <div className="grid gap-1 md:grid-cols-2">
        <Field label="comment_id" value={data.comment_id} />
        <Field label="ig_comment_id" value={data.ig_comment_id} />
        <Field label="media_id" value={data.media_id} />
        <Field label="matched_rule_id" value={data.matched_rule_id} />
        <Field label="matched_rule_scope" value={data.matched_rule_scope} />
        <Field label="reply_status" value={data.reply_status} />
        <Field label="dm_status" value={data.dm_status} />
        <Field label="action_status" value={data.action_status} />
        <Field label="public_reply_required" value={String(data.public_reply_required)} />
        <Field label="public_reply_source" value={data.public_reply_source} />
        <Field label="public_reply_text_length" value={data.public_reply_text_length} />
        <Field label="public_reply_text_hash" value={data.public_reply_text_hash} />
        <Field label="dm_required" value={String(data.dm_required)} />
        <Field label="dm_text_length" value={data.dm_text_length} />
        <Field label="dm_text_hash" value={data.dm_text_hash} />
        <Field label="reply_provider_response_ok" value={String(data.reply_provider_response_ok)} />
        <Field label="reply_provider_comment_id_exists" value={String(data.reply_provider_comment_id_exists)} />
        <Field label="reply_skip_reason" value={data.reply_skip_reason} />
        <Field label="reply_attempted_at" value={data.reply_attempted_at} />
        <Field label="replied_at" value={data.replied_at} />
        <Field label="dm_attempted_at" value={data.dm_attempted_at} />
        <Field label="finalDmSentAt" value={data.finalDmSentAt} />
        <Field label="next_retry_at" value={data.next_retry_at} />
        <Field label="attempts" value={data.attempts} />
        <Field label="queue_lock_until" value={data.queue_lock_until} />
        <Field label="forbidden_state_detected" value={String(data.forbidden_state_detected)} />
        <Field label="repairable" value={String(data.repairable)} />
        <Field label="repair_reason" value={data.repair_reason} />
      </div>
    </section>
  );
}

function ResultCard({ title, icon: Icon, data }) {
  if (!data) return null;
  return (
    <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
      <div className="flex items-center gap-2 mb-3">
        <Icon className="w-4 h-4 text-slate-500" />
        <h3 className="text-sm font-semibold text-slate-700">{title}</h3>
        {typeof data.ok === 'boolean' && <StatusPill ok={data.ok} label={data.ok ? 'OK' : 'NOT OK'} />}
      </div>
      <div className="grid gap-1 md:grid-cols-2">
        {Object.entries(data).map(([key, value]) => {
          if (value && typeof value === 'object') return null;
          return <Field key={key} label={key} value={String(value)} />;
        })}
      </div>
    </section>
  );
}

export default function SpecificReplyDebug() {
  const [enabled, setEnabled] = useState(null); // null = loading
  const [isAdmin, setIsAdmin] = useState(false);
  const [igCommentId, setIgCommentId] = useState('');
  const [diagnosis, setDiagnosis] = useState(null);
  const [repairResult, setRepairResult] = useState(null);
  const [retryResult, setRetryResult] = useState(null);
  const [loading, setLoading] = useState({});

  const setBusy = (key, val) => setLoading(prev => ({ ...prev, [key]: val }));

  useEffect(() => {
    (async () => {
      try {
        const { data } = await api.get('/admin/tools-enabled');
        setEnabled(!!data?.enabled);
        setIsAdmin(!!data?.is_admin);
      } catch (_) {
        setEnabled(false);
      }
    })();
  }, []);

  const onDiagnose = useCallback(async () => {
    const id = (igCommentId || '').trim();
    if (!id) return;
    setBusy('diagnose', true);
    setDiagnosis(null);
    setRepairResult(null);
    setRetryResult(null);
    try {
      const { data } = await api.get(
        `/admin/comments/${encodeURIComponent(id)}/specific-reply-diagnosis`
      );
      setDiagnosis(data);
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Diagnosis failed';
      toast.error(typeof msg === 'string' ? msg : JSON.stringify(msg));
    } finally {
      setBusy('diagnose', false);
    }
  }, [igCommentId]);

  const onRepair = useCallback(async () => {
    const id = (igCommentId || '').trim();
    if (!id) return;
    if (!window.confirm(`Repair comment ${id}? This re-queues the public reply only and will NOT resend the DM.`)) return;
    setBusy('repair', true);
    try {
      const { data } = await api.post(
        `/admin/comments/${encodeURIComponent(id)}/repair-specific-public-reply`
      );
      setRepairResult(data);
      if (data?.repaired) toast.success('Repaired — public reply re-queued. DM untouched.');
      else toast.info(`Not repaired: ${data?.reason || 'unknown'}`);
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Repair failed';
      toast.error(typeof msg === 'string' ? msg : JSON.stringify(msg));
    } finally {
      setBusy('repair', false);
    }
  }, [igCommentId]);

  const onProcessRetry = useCallback(async () => {
    const id = (igCommentId || '').trim();
    if (!id) return;
    setBusy('retry', true);
    try {
      const { data } = await api.post(
        `/admin/comments/${encodeURIComponent(id)}/process-retry-now`
      );
      setRetryResult(data);
      if (data?.reply_provider_response_ok) toast.success('Public reply confirmed by Instagram.');
      else if (data?.public_reply_attempted) toast.info('Retry attempted — see result card.');
      else toast.info('Retry not attempted (see result).');
    } catch (err) {
      const msg = err?.response?.data?.detail || 'Retry failed';
      toast.error(typeof msg === 'string' ? msg : JSON.stringify(msg));
    } finally {
      setBusy('retry', false);
    }
  }, [igCommentId]);

  if (enabled === null) {
    return (
      <div className="p-6 text-slate-500">Checking admin tools…</div>
    );
  }
  if (!enabled) {
    return (
      <div className="p-6 max-w-3xl mx-auto">
        <div className="bg-white rounded-2xl border border-slate-100 p-6">
          <div className="flex items-center gap-2 text-rose-600 mb-2">
            <ShieldAlert className="w-5 h-5" />
            <h1 className="text-lg font-semibold">Not available</h1>
          </div>
          <p className="text-sm text-slate-600">
            This page is internal support tooling and is disabled by default.
            If you are a user looking for your comments or automations,
            please go back to the Comments page.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="p-4 sm:p-6 max-w-4xl mx-auto" data-testid="specific-reply-debug-page">
      <div className="mb-6">
        <div className="flex items-center gap-2 text-amber-700 mb-1">
          <ShieldAlert className="w-4 h-4" />
          <span className="text-xs font-semibold uppercase tracking-wide">Admin debug</span>
          {isAdmin && <Badge className="bg-blue-100 text-blue-700 border-0 text-[10px]">admin</Badge>}
        </div>
        <h1 className="text-3xl font-bold font-display">Specific reply debug</h1>
        <p className="text-slate-500 mt-1 text-sm">
          Diagnose, repair, and retry a single comment's specific-post-rule
          public reply. Never resends DM. Never exposes raw comment, reply,
          or DM text.
        </p>
      </div>

      <section className="bg-white rounded-2xl border border-slate-100 p-5 mb-4">
        <label className="block text-xs font-semibold text-slate-600 uppercase tracking-wide mb-2">
          ig_comment_id
        </label>
        <div className="flex flex-col gap-2 sm:flex-row">
          <Input
            value={igCommentId}
            onChange={(e) => setIgCommentId(e.target.value)}
            placeholder="e.g. 18004285310876247"
            data-testid="ig-comment-id-input"
            className="font-mono"
          />
          {IS_DEV_BUILD && (
            <Button
              onClick={() => setIgCommentId(FAILED_COMMENT_ID)}
              variant="ghost"
              size="sm"
              className="rounded-full"
              type="button"
              title="Dev-only shortcut for the Phase 1.4 investigation comment"
            >
              Paste dev test id
            </Button>
          )}
        </div>
        <div className="mt-3 flex flex-wrap gap-2">
          <Button
            onClick={onDiagnose}
            disabled={!igCommentId.trim() || !!loading.diagnose}
            data-testid="diagnose-btn"
          >
            <Eye className="w-4 h-4 mr-2" />
            {loading.diagnose ? 'Diagnosing…' : 'Diagnose'}
          </Button>
          <Button
            onClick={onRepair}
            variant="outline"
            disabled={!igCommentId.trim() || !!loading.repair || !diagnosis?.repairable}
            data-testid="repair-btn"
          >
            <Wrench className="w-4 h-4 mr-2" />
            {loading.repair ? 'Repairing…' : 'Repair'}
          </Button>
          <Button
            onClick={onProcessRetry}
            variant="outline"
            disabled={!igCommentId.trim() || !!loading.retry}
            data-testid="retry-btn"
          >
            <PlayCircle className="w-4 h-4 mr-2" />
            {loading.retry ? 'Running…' : 'Process retry now'}
          </Button>
          {diagnosis && (
            <Button onClick={onDiagnose} variant="ghost" size="sm" className="ml-auto">
              <RefreshCw className={`w-3 h-3 mr-1 ${loading.diagnose ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
          )}
        </div>
      </section>

      <DiagnosisCard data={diagnosis} />
      <ResultCard title="Repair result" icon={Wrench} data={repairResult} />
      <ResultCard title="Retry result" icon={PlayCircle} data={retryResult} />

      <p className="text-xs text-slate-400 mt-4">
        Repair sets <span className="font-mono">reply_status=failed_retryable</span> +
        <span className="font-mono"> skip_reason=public_reply_required_not_attempted</span> and
        re-queues the public reply only. Repair never sends an Instagram message
        and never modifies a successful DM state. Process retry uses the
        provider-proof-guarded retry endpoint and short-circuits when proof
        already exists.
      </p>
    </div>
  );
}

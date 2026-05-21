import React, { useCallback, useEffect, useState } from 'react';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Badge } from '../../components/ui/badge';
import {
  Activity, RefreshCw, Copy, CheckCircle2, AlertTriangle, XCircle,
  Webhook, MessageSquare, Plug, ShieldAlert, Eraser,
} from 'lucide-react';
import api from '../../lib/api';
import { toast } from 'sonner';

/**
 * Admin-only Instagram automation diagnostics console.
 *
 * Not advertised in the sidebar — operators reach this page by typing
 * the URL (/app/admin/instagram-diagnostics). Behind RBAC because every
 * underlying endpoint enforces admin.users.view / admin.plans.assign.
 *
 * Purpose: surface the four production diagnostic endpoints in one
 * page with Copy JSON buttons, so the operator can capture evidence
 * without DevTools when troubleshooting multi-account automation
 * issues (Account 1 quick-reply continuation, Account 2 post-specific
 * rule not firing, etc).
 *
 * Privacy contract — this page renders the backend responses verbatim
 * but those responses are already sanitized:
 *   - external IG ids are partially redacted via _safe_partial_identifier
 *   - no access tokens, Authorization headers, full webhook bodies,
 *     full DM/comment text, or secrets are present in the responses
 * Nothing on this page sends auth secrets back to the server beyond
 * the standard Bearer token the rest of the app uses.
 */

const PANELS = [
  {
    key: 'automation_trace',
    title: 'Automation trace (per IG account)',
    description:
      'Last comments, sessions, dm logs, and rules per linked Instagram account. Use this to compare Account 1 vs Account 2 in one view.',
    Icon: Activity,
    path: '/admin/instagram/automation-trace',
  },
  {
    key: 'subscription_state',
    title: 'Meta webhook subscription state',
    description:
      'Asks Meta directly which fields each linked account is currently subscribed to. Missing "messages"/"messaging_postbacks" explains silent click failures; missing "comments" explains silent comment failures.',
    Icon: Plug,
    path: '/admin/instagram/subscription-state-all',
  },
  {
    key: 'webhook_log',
    title: 'Recent raw webhook log',
    description:
      'Last 50 webhook deliveries from Meta. Confirms whether comment/messaging events are actually arriving at our backend for each account.',
    Icon: Webhook,
    path: '/admin/webhook-log/recent?limit=50',
  },
  {
    key: 'comment_dm_sessions',
    title: 'Recent comment-DM sessions',
    description:
      'Last 50 comment-DM sessions for the caller. Shows stuck-state sessions that may be blocking re-opens via dedupe.',
    Icon: MessageSquare,
    path: '/admin/comment-dm-sessions/recent?limit=50',
  },
];

function statusBadge(state) {
  if (state === 'success') {
    return (
      <Badge className="bg-emerald-100 text-emerald-700 border-0">
        <CheckCircle2 className="w-3 h-3 me-1" /> Loaded
      </Badge>
    );
  }
  if (state === 'loading') {
    return (
      <Badge className="bg-slate-100 text-slate-600 border-0">
        <RefreshCw className="w-3 h-3 me-1 animate-spin" /> Loading…
      </Badge>
    );
  }
  if (state === 'forbidden') {
    return (
      <Badge className="bg-amber-100 text-amber-800 border-0">
        <ShieldAlert className="w-3 h-3 me-1" /> Forbidden
      </Badge>
    );
  }
  if (state === 'error') {
    return (
      <Badge className="bg-rose-100 text-rose-700 border-0">
        <XCircle className="w-3 h-3 me-1" /> Failed
      </Badge>
    );
  }
  return null;
}

function JsonPanel({ panel, snapshot, onLoad }) {
  const { Icon } = panel;
  const onCopy = useCallback(async () => {
    if (!snapshot?.data) return;
    try {
      const text = JSON.stringify(snapshot.data, null, 2);
      await navigator.clipboard.writeText(text);
      toast.success('JSON copied to clipboard');
    } catch (_) {
      toast.error('Clipboard copy failed — select + Ctrl+C the panel text');
    }
  }, [snapshot]);
  return (
    <section
      className="bg-white rounded-2xl border border-slate-200 p-4 mb-4"
      data-testid={`diag-panel-${panel.key}`}
    >
      <header className="flex flex-wrap items-center gap-3 mb-3">
        <Icon className="w-4 h-4 text-slate-500" />
        <div className="flex-1 min-w-[180px]">
          <div className="font-semibold text-slate-800">{panel.title}</div>
          <div className="text-xs text-slate-500">{panel.description}</div>
        </div>
        {statusBadge(snapshot?.state)}
        <Button
          size="sm"
          variant="outline"
          onClick={() => onLoad(panel)}
          disabled={snapshot?.state === 'loading'}
          data-testid={`diag-load-${panel.key}`}
        >
          <RefreshCw className={`w-3 h-3 me-1 ${snapshot?.state === 'loading' ? 'animate-spin' : ''}`} />
          {snapshot?.state === 'success' ? 'Reload' : 'Load'}
        </Button>
        <Button
          size="sm"
          variant="outline"
          onClick={onCopy}
          disabled={!snapshot?.data}
          data-testid={`diag-copy-${panel.key}`}
        >
          <Copy className="w-3 h-3 me-1" />
          Copy JSON
        </Button>
      </header>
      {snapshot?.state === 'forbidden' && (
        <div className="text-xs text-amber-800 bg-amber-50 border border-amber-200 rounded-md p-2 mb-2">
          The backend rejected this request with HTTP 403. Your account does not have the
          <span className="font-mono"> admin.users.view </span>
          permission. Sign in as an admin/owner and reload.
        </div>
      )}
      {snapshot?.state === 'error' && snapshot?.error && (
        <div className="text-xs text-rose-700 bg-rose-50 border border-rose-200 rounded-md p-2 mb-2">
          {snapshot.error}
        </div>
      )}
      {snapshot?.data ? (
        <pre
          className="text-xs bg-slate-950 text-slate-100 rounded-md p-3 overflow-x-auto max-h-[480px]"
          data-testid={`diag-json-${panel.key}`}
        >
{JSON.stringify(snapshot.data, null, 2)}
        </pre>
      ) : snapshot?.state === 'loading' ? (
        <div className="text-xs text-slate-500">Asking the backend…</div>
      ) : (
        <div className="text-xs text-slate-500">No data yet. Click Load.</div>
      )}
    </section>
  );
}

/**
 * Recent test flows — operator-friendly view of recent comment-DM
 * sessions with per-row reset buttons. The operator never has to copy
 * or paste internal ids: each row owns its session_id and the reset
 * call uses that id directly via
 * POST /api/admin/instagram/reset-flow-by-session-id.
 *
 * Backed by GET /api/admin/instagram/recent-flows which returns
 * sanitized rows + blocking_reason + stop_reason already classified.
 */
function statusToTone(blocking, completed) {
  if (completed) return { bg: 'bg-slate-100 text-slate-600', label: 'Completed' };
  if (blocking) return { bg: 'bg-amber-100 text-amber-800', label: 'Blocking' };
  return { bg: 'bg-emerald-100 text-emerald-700', label: 'Stale (auto-expires)' };
}

function FlowRow({ flow, onResetDone }) {
  const [dryRun, setDryRun] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);
  const completed = !!flow.finalDmSentAt || (flow.status || '').toLowerCase() === 'completed';
  const blocking = flow.blocking_reason && flow.blocking_reason !== 'stale_pending_auto_expires';
  const tone = statusToTone(blocking, completed);

  const runDryRun = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      const r = await api.post('/admin/instagram/reset-flow-by-session-id', {
        session_id: flow.session_id,
        dry_run: true,
        confirm: false,
      });
      setDryRun(r.data);
      const sCount = (r.data?.would_delete_sessions || []).length;
      const cCount = (r.data?.would_clear_opening_dedupe_on_comments || []).length;
      toast.success(`Dry run: would delete ${sCount} session(s), clear ${cCount} dedupe key(s).`);
    } catch (err) {
      const detail = err?.response?.data?.detail || err?.message || 'Request failed';
      setError(String(detail).slice(0, 240));
      toast.error(typeof detail === 'string' ? detail : 'Dry run failed');
    } finally {
      setBusy(false);
    }
  }, [flow.session_id]);

  const runConfirm = useCallback(async () => {
    if (!window.confirm(
      'This will DELETE this comment-DM session and CLEAR opening_dedupe_key on the matching comment so the same commenter can reopen this flow. dm_logs are kept. Proceed?',
    )) return;
    setBusy(true);
    setError(null);
    try {
      const r = await api.post('/admin/instagram/reset-flow-by-session-id', {
        session_id: flow.session_id,
        dry_run: false,
        confirm: true,
      });
      toast.success(`Reset applied: ${r.data?.sessions_deleted || 0} session(s) deleted, ${r.data?.comments_cleared || 0} dedupe key(s) cleared.`);
      setDryRun({ ...r.data, _applied: true });
      if (onResetDone) onResetDone();
    } catch (err) {
      const detail = err?.response?.data?.detail || err?.message || 'Request failed';
      setError(String(detail).slice(0, 240));
      toast.error(typeof detail === 'string' ? detail : 'Reset failed');
    } finally {
      setBusy(false);
    }
  }, [flow.session_id, onResetDone]);

  const ageLabel = flow.age_seconds == null
    ? '—'
    : flow.age_seconds < 60
      ? `${flow.age_seconds}s ago`
      : flow.age_seconds < 3600
        ? `${Math.round(flow.age_seconds / 60)}m ago`
        : flow.age_seconds < 86400
          ? `${Math.round(flow.age_seconds / 3600)}h ago`
          : `${Math.round(flow.age_seconds / 86400)}d ago`;

  return (
    <div
      className="border-t border-slate-100 px-3 py-3"
      data-testid={`recent-flow-row-${flow.session_id}`}
    >
      <div className="flex flex-wrap items-start gap-3">
        <div className="flex-1 min-w-[240px]">
          <div className="flex items-center gap-2 mb-1">
            <span className="font-semibold text-slate-800">@{flow.instagram_username || flow.instagram_account_id_partial}</span>
            <Badge className={`${tone.bg} border-0`}>{tone.label}</Badge>
            <span className="text-xs text-slate-500">{ageLabel}</span>
          </div>
          <div className="text-xs text-slate-500">
            {flow.rule_name ? <><span className="font-semibold">{flow.rule_name}</span> · </> : null}
            {flow.rule_post_scope || 'unknown scope'} · media{' '}
            <span className="font-mono">{flow.media_id_partial || '—'}</span> · commenter{' '}
            <span className="font-mono">{flow.commenter_id_partial || '—'}</span>
          </div>
          <div className="text-xs text-slate-500 mt-1">
            stage: <span className="font-mono">{flow.stage || '—'}</span> ·
            status: <span className="font-mono">{flow.status || '—'}</span> ·
            stop_reason: <span className="font-mono">{flow.stop_reason || '—'}</span>
          </div>
          {flow.quick_reply_log && (
            <div className="text-xs text-slate-500 mt-1">
              last button event: <span className="font-mono">{flow.quick_reply_log.event_kind}</span> · <span className="font-mono">{flow.quick_reply_log.status}</span>
              {flow.quick_reply_log.skip_reason ? <> · skip <span className="font-mono">{flow.quick_reply_log.skip_reason}</span></> : null}
            </div>
          )}
        </div>
        <div className="flex flex-wrap gap-2">
          <Button
            size="sm"
            variant="outline"
            onClick={runDryRun}
            disabled={busy}
            data-testid={`recent-flow-dryrun-${flow.session_id}`}
          >
            <RefreshCw className={`w-3 h-3 me-1 ${busy ? 'animate-spin' : ''}`} />
            Dry run reset
          </Button>
          <Button
            size="sm"
            onClick={runConfirm}
            disabled={busy || !dryRun || dryRun._applied}
            className="bg-rose-600 hover:bg-rose-700 text-white"
            data-testid={`recent-flow-confirm-${flow.session_id}`}
          >
            <Eraser className="w-3 h-3 me-1" />
            {dryRun?._applied ? 'Reset applied' : 'Confirm reset'}
          </Button>
        </div>
      </div>
      {error && (
        <div className="mt-2 text-xs text-rose-700 bg-rose-50 border border-rose-200 rounded-md p-2">
          {error}
        </div>
      )}
      {dryRun && !dryRun._applied && (
        <div className="mt-2 text-xs text-slate-600 bg-slate-50 border border-slate-200 rounded-md p-2">
          Dry-run preview: would delete{' '}
          <span className="font-mono">{(dryRun.would_delete_sessions || []).length}</span>{' '}
          session(s), clear{' '}
          <span className="font-mono">{(dryRun.would_clear_opening_dedupe_on_comments || []).length}</span>{' '}
          comment dedupe key(s). Click Confirm reset to apply.
        </div>
      )}
    </div>
  );
}

/**
 * Inline hint card rendered under a Recent comment events row when
 * the matched rule is a one-shot reply+DM rule (no button flow). The
 * card explains the gap, offers the rule editor, and offers a one-
 * click "repair" that promotes the rule into a button flow with
 * sensible defaults derived from the rule's own copy.
 */
function RuleHasNoDeferredFlowHint({ ev }) {
  const [busy, setBusy] = useState(false);
  const [preview, setPreview] = useState(null);
  const [applied, setApplied] = useState(false);
  const [error, setError] = useState(null);

  const runDryRun = useCallback(async () => {
    if (!ev.matched_rule_id) return;
    setBusy(true);
    setError(null);
    try {
      const r = await api.post('/admin/instagram/repair-rule-to-button-flow', {
        rule_id: ev.matched_rule_id,
        dry_run: true,
        confirm: false,
      });
      setPreview(r.data);
      toast.success('Repair preview ready — review before confirming.');
    } catch (err) {
      const detail = err?.response?.data?.detail || err?.message || 'Request failed';
      setError(String(detail).slice(0, 240));
      toast.error(typeof detail === 'string' ? detail : 'Dry run failed');
    } finally {
      setBusy(false);
    }
  }, [ev.matched_rule_id]);

  const runConfirm = useCallback(async () => {
    if (!ev.matched_rule_id || !preview) return;
    if (!window.confirm(
      'This will set opening_dm_text + opening_dm_button_text + follow_up_enabled + follow_up_text on this rule using defaults derived from your existing copy. dm_text is preserved. You can fine-tune the new fields in the normal rule editor afterwards. Proceed?',
    )) return;
    setBusy(true);
    setError(null);
    try {
      const r = await api.post('/admin/instagram/repair-rule-to-button-flow', {
        rule_id: ev.matched_rule_id,
        dry_run: false,
        confirm: true,
      });
      setApplied(true);
      setPreview({ ...r.data, _applied: true });
      toast.success('Rule repaired — next comment will create a session.');
    } catch (err) {
      const detail = err?.response?.data?.detail || err?.message || 'Request failed';
      setError(String(detail).slice(0, 240));
      toast.error(typeof detail === 'string' ? detail : 'Repair failed');
    } finally {
      setBusy(false);
    }
  }, [ev.matched_rule_id, preview]);

  const cls = ev.rule_deferred_flow || {};
  return (
    <div
      className="mt-2 text-xs bg-amber-50 border border-amber-200 rounded-md p-2 text-amber-900"
      data-testid={`rule-deferred-flow-hint-${ev.comment_doc_id}`}
    >
      <div className="font-semibold mb-1">
        This rule is configured as a one-shot reply + DM, not a button-driven flow.
      </div>
      <div className="text-amber-800">
        The reply and opening DM did send successfully. The system does not create a session
        because the rule is missing the button + next-step fields a button-driven flow needs.
        The recipient cannot continue past the opening DM because there is no button on it.
      </div>
      <div className="mt-2">
        <div className="font-semibold mb-0.5">Fields present:</div>
        <div className="font-mono">
          {(cls.present_deferred_fields || []).length
            ? cls.present_deferred_fields.join(', ')
            : '(none of the deferred-flow fields are populated)'}
        </div>
      </div>
      <div className="mt-2">
        <div className="font-semibold mb-0.5">Add these to enable the button flow:</div>
        <div className="font-mono">
          {(cls.button_flow_missing || []).join(', ') || '(none)'}
        </div>
      </div>
      {cls.one_shot_dm_only && (
        <div className="mt-2 text-amber-800">
          Detected legacy one-shot <span className="font-mono">dm_text</span>: the rule keeps
          sending a single DM and never opens a session until it gets an opening DM with a button
          and a next step.
        </div>
      )}

      <div className="mt-3 flex flex-wrap gap-2">
        <Button
          size="sm"
          variant="outline"
          onClick={runDryRun}
          disabled={busy || applied || !ev.matched_rule_id}
          data-testid={`repair-dryrun-${ev.matched_rule_id}`}
        >
          <RefreshCw className={`w-3 h-3 me-1 ${busy ? 'animate-spin' : ''}`} />
          Preview repair
        </Button>
        <Button
          size="sm"
          onClick={runConfirm}
          disabled={busy || applied || !preview || preview?._applied}
          className="bg-emerald-600 hover:bg-emerald-700 text-white"
          data-testid={`repair-confirm-${ev.matched_rule_id}`}
        >
          <CheckCircle2 className="w-3 h-3 me-1" />
          {applied ? 'Repaired' : 'Apply repair'}
        </Button>
        {ev.matched_rule_id && (
          <Button
            size="sm"
            variant="outline"
            onClick={() => window.open(`/app/automations/${encodeURIComponent(ev.matched_rule_id)}`, '_blank')}
            data-testid={`open-rule-${ev.matched_rule_id}`}
          >
            Open rule editor
          </Button>
        )}
      </div>

      {error && (
        <div className="mt-2 text-rose-700 bg-rose-50 border border-rose-200 rounded-md p-2">
          {error}
        </div>
      )}
      {preview && !preview._applied && (
        <div className="mt-2 bg-white border border-amber-200 rounded-md p-2 text-amber-900">
          <div className="font-semibold mb-1">Preview (dry-run, no changes saved yet)</div>
          <div>
            after: <span className="font-mono">enabled={String(preview.after?.enabled)}</span>{' '}
            · <span className="font-mono">button_flow_ready={String(preview.after?.button_flow_ready)}</span>
          </div>
          <div className="mt-1 text-amber-800">
            Will populate: opening_dm_text (from your dm_text), opening_dm_button_text, follow_up_enabled, follow_up_text.
            Your existing dm_text is preserved unchanged.
          </div>
        </div>
      )}
      {applied && (
        <div className="mt-2 bg-emerald-50 border border-emerald-200 rounded-md p-2 text-emerald-800">
          <div className="font-semibold mb-1">Rule repaired.</div>
          The next comment that matches this rule will create a comment-DM session and
          deliver a button. You can fine-tune the button label, the follow-up text, or
          switch to a link/follow-gate next step in the normal automation editor.
        </div>
      )}
    </div>
  );
}


/**
 * Recent comment events — exposes the comments collection rows so
 * the operator can see every comment that reached the backend, not
 * just the ones that produced a session. This is the panel that
 * answers "I commented but nothing happened — did it even arrive?".
 */
function RecentCommentEventsPanel() {
  const [state, setState] = useState('idle');
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  const load = useCallback(async () => {
    setState('loading');
    setError(null);
    try {
      const r = await api.get('/admin/instagram/recent-comment-events?limit=30');
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

  const events = data?.events || [];
  const accounts = data?.accounts || [];

  return (
    <section
      className="bg-white rounded-2xl border border-slate-200 p-4 mb-4"
      data-testid="diag-panel-recent_comment_events"
    >
      <header className="flex flex-wrap items-center gap-3 mb-3">
        <MessageSquare className="w-4 h-4 text-slate-500" />
        <div className="flex-1 min-w-[180px]">
          <div className="font-semibold text-slate-800">Recent comment events</div>
          <div className="text-xs text-slate-500">
            Newest comments that reached the backend, regardless of whether they produced a
            session. If a fresh comment is NOT in this list, the webhook never arrived or
            polling hasn't picked it up yet — check Subscription state and the raw webhook log.
          </div>
        </div>
        {state === 'loading' && (
          <Badge className="bg-slate-100 text-slate-600 border-0">
            <RefreshCw className="w-3 h-3 me-1 animate-spin" /> Loading…
          </Badge>
        )}
        {state === 'success' && (
          <Badge className="bg-emerald-100 text-emerald-700 border-0">
            <CheckCircle2 className="w-3 h-3 me-1" /> {data?.count ?? 0} events
          </Badge>
        )}
        {state === 'forbidden' && (
          <Badge className="bg-amber-100 text-amber-800 border-0">
            <ShieldAlert className="w-3 h-3 me-1" /> Forbidden
          </Badge>
        )}
        {state === 'error' && (
          <Badge className="bg-rose-100 text-rose-700 border-0">
            <XCircle className="w-3 h-3 me-1" /> Failed
          </Badge>
        )}
        <Button size="sm" variant="outline" onClick={load} disabled={state === 'loading'}
                data-testid="recent-comment-events-reload">
          <RefreshCw className={`w-3 h-3 me-1 ${state === 'loading' ? 'animate-spin' : ''}`} />
          Reload
        </Button>
      </header>
      {error && (
        <div className="text-xs text-rose-700 bg-rose-50 border border-rose-200 rounded-md p-2 mb-2">
          {error}
        </div>
      )}
      {accounts.length > 0 && (
        <div className="grid sm:grid-cols-2 gap-2 mb-3" data-testid="recent-comment-events-account-summary">
          {accounts.map((a) => (
            <div
              key={a.instagram_account_id_partial}
              className="text-xs border border-slate-100 rounded-md p-2 bg-slate-50"
            >
              <div className="font-semibold text-slate-700">@{a.instagram_username || a.instagram_account_id_partial}</div>
              <div className="text-slate-500">
                last comment seen:{' '}
                {a.last_comment_event_at
                  ? <span className="font-mono">{new Date(a.last_comment_event_at).toLocaleString()}</span>
                  : <span className="text-rose-700 font-semibold">never (no comment has reached the backend)</span>}
              </div>
            </div>
          ))}
        </div>
      )}
      {state === 'success' && events.length === 0 && (
        <div className="text-xs text-slate-500 py-4 text-center">
          No recent comment events. If you just commented, wait ~15 seconds (poller) and reload.
          If still empty: open the Recent raw webhook log panel below and check whether the
          webhook arrived at all.
        </div>
      )}
      {events.length > 0 && (
        <div className="rounded-md border border-slate-100">
          {events.map((ev) => {
            const blocked = !ev.session_created;
            const tone = ev.action_status === 'success'
              ? 'bg-emerald-100 text-emerald-700'
              : ev.action_status === 'partial_success'
                ? 'bg-amber-100 text-amber-800'
                : blocked
                  ? 'bg-rose-100 text-rose-700'
                  : 'bg-slate-100 text-slate-600';
            const ageLabel = ev.age_seconds == null
              ? '—'
              : ev.age_seconds < 60 ? `${ev.age_seconds}s ago`
              : ev.age_seconds < 3600 ? `${Math.round(ev.age_seconds / 60)}m ago`
              : ev.age_seconds < 86400 ? `${Math.round(ev.age_seconds / 3600)}h ago`
              : `${Math.round(ev.age_seconds / 86400)}d ago`;
            return (
              <div
                key={ev.comment_doc_id}
                className="border-t border-slate-100 px-3 py-3"
                data-testid={`recent-comment-event-${ev.comment_doc_id}`}
              >
                <div className="flex flex-wrap items-start gap-3">
                  <div className="flex-1 min-w-[260px]">
                    <div className="flex items-center gap-2 mb-1">
                      <span className="font-semibold text-slate-800">@{ev.instagram_username || ev.instagram_account_id_partial}</span>
                      <Badge className={`${tone} border-0`}>
                        {ev.action_status || 'unknown'}
                      </Badge>
                      <span className="text-xs text-slate-500">{ageLabel}</span>
                      <Badge className="bg-slate-100 text-slate-600 border-0 text-[10px]">
                        source: {ev.source || 'unknown'}
                      </Badge>
                    </div>
                    <div className="text-xs text-slate-500">
                      {ev.matched_rule_name ? <><span className="font-semibold">{ev.matched_rule_name}</span> · </> : null}
                      scope <span className="font-mono">{ev.matched_rule_scope || 'none'}</span> ·
                      media <span className="font-mono">{ev.media_id_partial || '—'}</span> ·
                      commenter <span className="font-mono">{ev.commenter_id_partial || '—'}</span>
                      {ev.commenter_username ? <> (<span className="font-mono">{ev.commenter_username}</span>)</> : null}
                    </div>
                    <div className="text-xs text-slate-500 mt-1">
                      reply <span className="font-mono">{ev.reply_status || '—'}</span> ·
                      DM <span className="font-mono">{ev.dm_status || '—'}</span>
                      {ev.skip_reason ? <> · skip <span className="font-mono">{ev.skip_reason}</span></> : null}
                      {ev.reply_failure_reason ? <> · reply_err <span className="font-mono">{ev.reply_failure_reason}</span></> : null}
                      {ev.dm_failure_reason ? <> · dm_err <span className="font-mono">{ev.dm_failure_reason}</span></> : null}
                    </div>
                    <div className="text-xs text-slate-500 mt-1">
                      session_created:{' '}
                      <span className={`font-mono ${ev.session_created ? 'text-emerald-700' : 'text-rose-700'}`}>
                        {String(ev.session_created)}
                      </span>
                      {!ev.session_created && ev.no_session_reason && (
                        <> · reason <span className="font-mono">{ev.no_session_reason}</span></>
                      )}
                      {ev.related_session_id && (
                        <> · session <span className="font-mono">{ev.related_session_id}</span></>
                      )}
                    </div>
                    {ev.no_session_reason === 'rule_has_no_deferred_flow' && ev.rule_deferred_flow && (
                      <RuleHasNoDeferredFlowHint ev={ev} />
                    )}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}


function RecentTestFlowsPanel() {
  const [state, setState] = useState('idle');
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);

  const load = useCallback(async () => {
    setState('loading');
    setError(null);
    try {
      const r = await api.get('/admin/instagram/recent-flows?limit=20');
      setData(r.data);
      setState('success');
    } catch (err) {
      const status = err?.response?.status;
      const detail = err?.response?.data?.detail || err?.message || 'Request failed';
      setError(String(detail).slice(0, 240));
      setState(status === 401 || status === 403 ? 'forbidden' : 'error');
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const flows = data?.flows || [];
  return (
    <section
      className="bg-white rounded-2xl border border-slate-200 p-4 mb-4"
      data-testid="diag-panel-recent_test_flows"
    >
      <header className="flex flex-wrap items-center gap-3 mb-3">
        <Activity className="w-4 h-4 text-slate-500" />
        <div className="flex-1 min-w-[180px]">
          <div className="font-semibold text-slate-800">Recent test flows</div>
          <div className="text-xs text-slate-500">
            Newest comment-DM sessions per linked Instagram account. Each row shows whether it is
            blocking new tests for the same commenter+post+rule, and why the flow stopped. Click
            "Dry run reset" then "Confirm reset" on a single row to unblock retesting — no ids to
            copy by hand.
          </div>
        </div>
        {state === 'loading' && (
          <Badge className="bg-slate-100 text-slate-600 border-0">
            <RefreshCw className="w-3 h-3 me-1 animate-spin" /> Loading…
          </Badge>
        )}
        {state === 'success' && (
          <Badge className="bg-emerald-100 text-emerald-700 border-0">
            <CheckCircle2 className="w-3 h-3 me-1" /> {data?.count ?? 0} loaded
          </Badge>
        )}
        {state === 'forbidden' && (
          <Badge className="bg-amber-100 text-amber-800 border-0">
            <ShieldAlert className="w-3 h-3 me-1" /> Forbidden
          </Badge>
        )}
        {state === 'error' && (
          <Badge className="bg-rose-100 text-rose-700 border-0">
            <XCircle className="w-3 h-3 me-1" /> Failed
          </Badge>
        )}
        <Button size="sm" variant="outline" onClick={load} disabled={state === 'loading'} data-testid="recent-flows-reload">
          <RefreshCw className={`w-3 h-3 me-1 ${state === 'loading' ? 'animate-spin' : ''}`} />
          Reload
        </Button>
      </header>
      {error && (
        <div className="text-xs text-rose-700 bg-rose-50 border border-rose-200 rounded-md p-2 mb-2">
          {error}
        </div>
      )}
      {data?.has_blocking_flows && (
        <div className="text-xs text-amber-900 bg-amber-50 border border-amber-200 rounded-md p-2 mb-3">
          {data.blocking_count} blocking flow{data.blocking_count === 1 ? '' : 's'} found.
          The same commenter on the same post+rule cannot reopen these flows until they complete,
          fail visibly, or auto-expire (TTL: {Math.round((data.stale_pending_ttl_seconds || 0) / 3600)}h).
          Use the per-row reset below to unblock a single test without affecting other accounts or commenters.
        </div>
      )}
      {state === 'success' && flows.length === 0 && (
        <div className="text-xs text-slate-700 py-4 px-3 bg-amber-50 border border-amber-200 rounded-md">
          <div className="font-semibold mb-1">No comment-DM sessions found.</div>
          <div className="text-slate-600">
            This means no recent comment has reached the point where a session is created.
            That can mean:
            <ul className="list-disc list-inside mt-1 space-y-0.5">
              <li>The webhook did not arrive (Meta delivery issue or signature failure)</li>
              <li>The rule did not match (e.g. media_id mismatch on a post-specific rule)</li>
              <li>The opening DM failed before session creation</li>
            </ul>
            <div className="mt-2">
              Scroll to <span className="font-semibold">Recent comment events</span> below to see every
              comment that did reach the backend, including failures and skip reasons — that
              panel is the one to inspect when the flow list is empty.
            </div>
          </div>
        </div>
      )}
      {flows.length > 0 && (
        <div className="rounded-md border border-slate-100">
          {flows.map((flow) => (
            <FlowRow key={flow.session_id} flow={flow} onResetDone={load} />
          ))}
        </div>
      )}
    </section>
  );
}


/**
 * Targeted reset for a single (account, automation, media, commenter)
 * comment-DM flow state. Always requires dry-run preview first, then
 * an explicit Confirm click. Backed by
 * POST /api/admin/instagram/reset-test-flow.
 */
function ResetTestFlowPanel() {
  const [igId, setIgId] = useState('');
  const [automationId, setAutomationId] = useState('');
  const [mediaId, setMediaId] = useState('');
  const [commenterId, setCommenterId] = useState('');
  const [busy, setBusy] = useState(false);
  const [snapshot, setSnapshot] = useState(null);

  const allFieldsFilled = igId.trim() && automationId.trim() && mediaId.trim() && commenterId.trim();

  const sendReset = useCallback(async ({ dryRun }) => {
    if (!allFieldsFilled) {
      toast.error('All four fields are required.');
      return;
    }
    setBusy(true);
    try {
      const r = await api.post('/admin/instagram/reset-test-flow', {
        instagram_account_id: igId.trim(),
        automation_id: automationId.trim(),
        media_id: mediaId.trim(),
        commenter_id: commenterId.trim(),
        dry_run: dryRun,
        confirm: !dryRun,
      });
      setSnapshot({ state: 'success', data: r.data, error: null, mode: dryRun ? 'dry_run' : 'confirm' });
      if (dryRun) {
        const sCount = (r.data?.would_delete_sessions || []).length;
        const cCount = (r.data?.would_clear_opening_dedupe_on_comments || []).length;
        toast.success(`Dry run: would delete ${sCount} session(s), clear ${cCount} comment dedupe key(s).`);
      } else {
        toast.success(`Reset applied: ${r.data?.sessions_deleted || 0} session(s) deleted, ${r.data?.comments_cleared || 0} comment(s) cleared.`);
      }
    } catch (err) {
      const status = err?.response?.status;
      const detail = err?.response?.data?.detail || err?.message || 'Request failed';
      setSnapshot({
        state: status === 401 || status === 403 ? 'forbidden' : 'error',
        data: null,
        error: String(detail).slice(0, 240),
        mode: dryRun ? 'dry_run' : 'confirm',
      });
      toast.error(typeof detail === 'string' ? detail : 'Request failed');
    } finally {
      setBusy(false);
    }
  }, [igId, automationId, mediaId, commenterId, allFieldsFilled]);

  const onCopy = useCallback(async () => {
    if (!snapshot?.data) return;
    try {
      await navigator.clipboard.writeText(JSON.stringify(snapshot.data, null, 2));
      toast.success('Reset result copied');
    } catch (_) {
      toast.error('Clipboard copy failed');
    }
  }, [snapshot]);

  return (
    <section
      className="bg-white rounded-2xl border border-slate-200 p-4 mb-4"
      data-testid="diag-panel-reset_test_flow"
    >
      <header className="flex flex-wrap items-center gap-3 mb-3">
        <Eraser className="w-4 h-4 text-slate-500" />
        <div className="flex-1 min-w-[180px]">
          <div className="font-semibold text-slate-800">Reset one test flow</div>
          <div className="text-xs text-slate-500">
            Targeted dedupe + session reset for a single
            (account, automation, media, commenter) tuple so the same tester can
            reopen the same post+rule. Dry-run first — confirm only after
            inspecting the preview. Never touches dm_logs.
          </div>
        </div>
        {snapshot?.state === 'forbidden' && (
          <Badge className="bg-amber-100 text-amber-800 border-0">
            <ShieldAlert className="w-3 h-3 me-1" /> Forbidden
          </Badge>
        )}
        {snapshot?.state === 'error' && (
          <Badge className="bg-rose-100 text-rose-700 border-0">
            <XCircle className="w-3 h-3 me-1" /> Failed
          </Badge>
        )}
        {snapshot?.state === 'success' && snapshot.mode === 'dry_run' && (
          <Badge className="bg-slate-100 text-slate-700 border-0">Dry run</Badge>
        )}
        {snapshot?.state === 'success' && snapshot.mode === 'confirm' && (
          <Badge className="bg-emerald-100 text-emerald-700 border-0">
            <CheckCircle2 className="w-3 h-3 me-1" /> Applied
          </Badge>
        )}
      </header>

      <div className="grid sm:grid-cols-2 gap-2 mb-3">
        <Input
          placeholder="instagram_account_id (numeric IG id)"
          value={igId}
          onChange={(e) => setIgId(e.target.value)}
          data-testid="reset-input-ig"
          autoComplete="off"
        />
        <Input
          placeholder="automation_id (rule id)"
          value={automationId}
          onChange={(e) => setAutomationId(e.target.value)}
          data-testid="reset-input-automation"
          autoComplete="off"
        />
        <Input
          placeholder="media_id (post id)"
          value={mediaId}
          onChange={(e) => setMediaId(e.target.value)}
          data-testid="reset-input-media"
          autoComplete="off"
        />
        <Input
          placeholder="commenter_id / recipient_id"
          value={commenterId}
          onChange={(e) => setCommenterId(e.target.value)}
          data-testid="reset-input-commenter"
          autoComplete="off"
        />
      </div>

      <div className="flex flex-wrap gap-2 mb-3">
        <Button
          variant="outline"
          onClick={() => sendReset({ dryRun: true })}
          disabled={busy || !allFieldsFilled}
          data-testid="reset-dryrun-btn"
        >
          <RefreshCw className={`w-3 h-3 me-1 ${busy ? 'animate-spin' : ''}`} />
          Dry run reset
        </Button>
        <Button
          onClick={() => {
            if (window.confirm(
              'This will DELETE matching comment_dm_sessions and CLEAR opening_dedupe_key on matching comments for this exact (account, automation, media, commenter) tuple. dm_logs are kept. Proceed?'
            )) {
              sendReset({ dryRun: false });
            }
          }}
          disabled={busy || !allFieldsFilled || snapshot?.state !== 'success' || snapshot?.mode !== 'dry_run'}
          data-testid="reset-confirm-btn"
          className="bg-rose-600 hover:bg-rose-700 text-white"
        >
          <Eraser className="w-3 h-3 me-1" />
          Confirm reset
        </Button>
        <Button
          variant="outline"
          onClick={onCopy}
          disabled={!snapshot?.data}
          data-testid="reset-copy-btn"
        >
          <Copy className="w-3 h-3 me-1" />
          Copy result JSON
        </Button>
      </div>
      {!allFieldsFilled && (
        <div className="text-xs text-slate-500 mb-2">
          All four fields are required. Get them from the automation_trace JSON
          (rule_id, media_id partial, commenter_id partial — paste the full id
          values that produced those partials).
        </div>
      )}
      {snapshot?.state === 'forbidden' && (
        <div className="text-xs text-amber-800 bg-amber-50 border border-amber-200 rounded-md p-2 mb-2">
          The backend rejected this request with HTTP 403. Your account does
          not have the <span className="font-mono">admin.plans.assign</span>{' '}
          permission required to mutate state.
        </div>
      )}
      {snapshot?.state === 'error' && snapshot.error && (
        <div className="text-xs text-rose-700 bg-rose-50 border border-rose-200 rounded-md p-2 mb-2">
          {snapshot.error}
        </div>
      )}
      {snapshot?.data && (
        <pre
          className="text-xs bg-slate-950 text-slate-100 rounded-md p-3 overflow-x-auto max-h-[420px]"
          data-testid="reset-json"
        >
{JSON.stringify(snapshot.data, null, 2)}
        </pre>
      )}
    </section>
  );
}


export default function InstagramDiagnostics() {
  const [snapshots, setSnapshots] = useState({});
  const [running, setRunning] = useState(false);

  const onLoad = useCallback(async (panel) => {
    setSnapshots((prev) => ({
      ...prev,
      [panel.key]: { state: 'loading', data: null, error: null },
    }));
    try {
      const r = await api.get(panel.path, { timeout: 15000 });
      setSnapshots((prev) => ({
        ...prev,
        [panel.key]: { state: 'success', data: r.data, error: null },
      }));
    } catch (err) {
      const status = err?.response?.status;
      const detail = err?.response?.data?.detail || err?.message || 'Request failed';
      let newState = 'error';
      if (status === 401 || status === 403) newState = 'forbidden';
      setSnapshots((prev) => ({
        ...prev,
        [panel.key]: { state: newState, data: null, error: String(detail).slice(0, 240) },
      }));
    }
  }, []);

  const onLoadAll = useCallback(async () => {
    setRunning(true);
    try {
      await Promise.all(PANELS.map((p) => onLoad(p)));
      toast.success('All diagnostics loaded');
    } finally {
      setRunning(false);
    }
  }, [onLoad]);

  const onCopyAll = useCallback(async () => {
    const all = {};
    for (const panel of PANELS) {
      const snap = snapshots[panel.key];
      all[panel.key] = snap?.data ?? null;
    }
    try {
      await navigator.clipboard.writeText(JSON.stringify(all, null, 2));
      toast.success('All diagnostics JSON copied to clipboard');
    } catch (_) {
      toast.error('Clipboard copy failed — copy each panel individually');
    }
  }, [snapshots]);

  return (
    <div className="p-4 sm:p-6 max-w-5xl mx-auto" data-testid="instagram-diagnostics-page">
      <header className="flex flex-wrap items-end justify-between gap-3 mb-4">
        <div>
          <div className="flex items-center gap-2 text-slate-500 text-xs uppercase tracking-wide font-semibold mb-1">
            <Activity className="w-4 h-4" />
            Admin · Instagram automation diagnostics
          </div>
          <h1 className="text-2xl font-bold text-slate-800">Instagram automation diagnostics</h1>
          <p className="text-sm text-slate-500 mt-1 max-w-[640px]">
            Read-only safe snapshots for triaging multi-account Instagram automation
            failures. Each panel calls a single admin-protected backend endpoint. Use Copy JSON
            to share evidence without exposing secrets — responses are pre-sanitized by the backend.
          </p>
        </div>
        <div className="flex gap-2">
          <Button onClick={onLoadAll} disabled={running} data-testid="diag-load-all">
            <RefreshCw className={`w-4 h-4 me-2 ${running ? 'animate-spin' : ''}`} />
            Load all diagnostics
          </Button>
          <Button variant="outline" onClick={onCopyAll} data-testid="diag-copy-all">
            <Copy className="w-4 h-4 me-2" />
            Copy combined JSON
          </Button>
        </div>
      </header>

      <div className="mb-4 rounded-2xl border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900 flex gap-2 items-start">
        <AlertTriangle className="w-4 h-4 mt-0.5 shrink-0" />
        <div>
          <div className="font-semibold mb-1">Privacy contract</div>
          <div>
            Responses already redact external Instagram ids and never include tokens, full
            webhook bodies, or full DM/comment text. Even so, treat the copied JSON as internal
            — paste only into your team's private debugging channel.
          </div>
        </div>
      </div>

      <RecentTestFlowsPanel />

      <RecentCommentEventsPanel />

      <ResetTestFlowPanel />

      {PANELS.map((panel) => (
        <JsonPanel
          key={panel.key}
          panel={panel}
          snapshot={snapshots[panel.key]}
          onLoad={onLoad}
        />
      ))}
    </div>
  );
}

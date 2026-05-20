import React, { useCallback, useState } from 'react';
import { Button } from '../../components/ui/button';
import { Badge } from '../../components/ui/badge';
import {
  Activity, RefreshCw, Copy, CheckCircle2, AlertTriangle, XCircle,
  Webhook, MessageSquare, Plug, ShieldAlert,
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

import React, { useEffect, useState, useCallback } from 'react';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import {
  RefreshCw, Activity, CheckCircle2, AlertTriangle, XCircle,
  Webhook, Database, Cpu, Clock,
} from 'lucide-react';
import api from '../lib/api';
import { toast } from 'sonner';

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

function statusOf(task) {
  if (!task) return { tone: 'slate', label: 'unknown' };
  if (!task.running) {
    return { tone: 'rose', label: 'stopped' };
  }
  if ((task.consecutive_failures || 0) >= 3) {
    return { tone: 'orange', label: 'degraded' };
  }
  return { tone: 'emerald', label: 'running' };
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
  const [data, setData] = useState(null);
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
      setError(err?.response?.data?.detail || 'Failed to load system health');
      toast.error('Failed to load system health');
    } finally {
      setLoading(false);
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
          <h1 className="text-3xl font-bold font-display">System Health</h1>
          <p className="text-slate-500 mt-1 text-sm">
            Live operations status. Tokens and message bodies are never shown.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-xs text-slate-400">
            Auto-refresh: 30s {refreshedAt && <>• last {fmtTime(refreshedAt)}</>}
          </span>
          <Button variant="outline" size="sm" onClick={load} disabled={loading}>
            <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
            Refresh
          </Button>
        </div>
      </div>

      {error && (
        <div className="mb-4 rounded-xl border border-rose-200 bg-rose-50 p-3 text-sm text-rose-700">
          <AlertTriangle className="inline w-4 h-4 mr-1" />
          {error}
        </div>
      )}

      <div className="grid gap-4 md:grid-cols-2">
        <Section title="Webhook" icon={Webhook}>
          <div className="space-y-2 text-sm">
            <div className="flex items-center justify-between">
              <span className="text-slate-500">Last received</span>
              <span className="font-mono text-slate-700">
                {fmtTime(webhook.last_received_at)}
                {webhookAgeRecv !== null && (
                  <span className="text-xs text-slate-400 ml-2">({webhookAgeRecv}s ago)</span>
                )}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">Last processed</span>
              <span className="font-mono text-slate-700">
                {fmtTime(webhook.last_processed_at)}
                {webhookAgeProc !== null && (
                  <span className="text-xs text-slate-400 ml-2">({webhookAgeProc}s ago)</span>
                )}
              </span>
            </div>
          </div>
        </Section>

        <Section title="Instagram connection" icon={Activity}>
          {accounts.length === 0 && (
            <div className="text-sm text-slate-500">No active Instagram account.</div>
          )}
          {accounts.map((a, i) => (
            <div key={i} className="space-y-2 text-sm">
              <div className="flex items-center justify-between">
                <span className="text-slate-500">Active account</span>
                <span className="font-mono text-slate-700">
                  {a.instagramAccountId || '—'}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-slate-500">Connection</span>
                <span className="flex items-center gap-2">
                  {a.instagramConnected && a.connectionValid
                    ? <><CheckCircle2 className="w-4 h-4 text-emerald-500" /> valid</>
                    : <><XCircle className="w-4 h-4 text-rose-500" /> reconnect required</>}
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-slate-500">Token present</span>
                <span>{a.tokenPresent ? 'yes' : 'no'}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-slate-500">Auth method</span>
                <span className="font-mono">{a.auth_kind || '—'}</span>
              </div>
            </div>
          ))}
        </Section>

        <Section title="Background tasks" icon={Cpu} hint="poller, watchdog, follow-verifier">
          <div className="space-y-2">
            {Object.entries(tasks).map(([name, t]) => {
              const s = statusOf(t);
              return (
                <div key={name} className="flex items-center justify-between text-sm">
                  <span className="flex items-center gap-2">
                    <StatusDot tone={s.tone} />
                    <span className="font-mono text-slate-700">{name}</span>
                    <Badge className="bg-slate-100 text-slate-700 border-0 text-[10px]">{s.label}</Badge>
                  </span>
                  <span className="text-xs text-slate-500">
                    restarts: {t.restarts || 0} • fails: {t.consecutive_failures || 0}
                  </span>
                </div>
              );
            })}
            {Object.keys(tasks).length === 0 && (
              <div className="text-sm text-slate-500">No background tasks reported.</div>
            )}
          </div>
        </Section>

        <Section title="Comment-DM job queue" icon={Database}>
          <div className="space-y-2 text-sm">
            <div className="flex items-center justify-between">
              <span className="text-slate-500">Pending</span>
              <span className="font-mono text-slate-700">
                {jobs.pending_comment_dm_sessions ?? 0}
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">Failed</span>
              <span className="font-mono text-slate-700">
                {jobs.failed_comment_dm_sessions ?? 0}
              </span>
            </div>
          </div>
        </Section>

        <Section title="Configuration" icon={Clock}>
          <div className="space-y-2 text-sm">
            <div className="flex items-center justify-between">
              <span className="text-slate-500">Comment poller interval</span>
              <span className="font-mono text-slate-700">
                {config.comment_poller_interval_seconds ?? '—'}s
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">Comment poller enabled</span>
              <span>{config.comment_poller_enabled ? 'yes' : 'no'}</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">Follow verifier interval</span>
              <span className="font-mono text-slate-700">
                {config.follow_verifier_interval_seconds ?? '—'}s
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-slate-500">Watchdog interval</span>
              <span className="font-mono text-slate-700">
                {config.watchdog_interval_seconds ?? '—'}s
              </span>
            </div>
          </div>
        </Section>
      </div>
    </div>
  );
};

export default SystemHealth;

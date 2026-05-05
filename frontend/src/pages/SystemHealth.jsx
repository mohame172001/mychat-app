import React, { useEffect, useState } from 'react';
import { Activity, RefreshCw, CheckCircle2, AlertTriangle } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import api from '../lib/api';
import { toast } from 'sonner';

const SystemHealth = () => {
  const [health, setHealth] = useState(null);
  const [loading, setLoading] = useState(true);

  const loadHealth = async () => {
    setLoading(true);
    try {
      const { data } = await api.get('/instagram/automation-health');
      setHealth(data);
    } catch (err) {
      toast.error(err?.response?.data?.detail || 'Failed to load system health');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadHealth();
  }, []);

  const tasks = health?.tasks || {};
  const taskRows = Object.entries(tasks);

  return (
    <div className="p-4 sm:p-6 max-w-5xl mx-auto">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between mb-6">
        <div>
          <h1 className="text-3xl font-bold font-display flex items-center gap-2">
            <Activity className="w-7 h-7" /> System Health
          </h1>
          <p className="text-slate-500 mt-1">Background automation workers, webhook timing, and safe token status.</p>
        </div>
        <Button variant="outline" size="sm" onClick={loadHealth} disabled={loading}>
          <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </div>

      {loading && !health && (
        <div className="text-center py-20 text-slate-500">Loading...</div>
      )}

      {health && (
        <div className="space-y-5">
          <section className="bg-white rounded-2xl border border-slate-100 p-5">
            <h2 className="font-semibold mb-3">Background workers</h2>
            <div className="grid gap-3 md:grid-cols-2">
              {taskRows.length === 0 && <div className="text-sm text-slate-500">No worker status reported.</div>}
              {taskRows.map(([name, task]) => (
                <div key={name} className="rounded-xl border border-slate-100 p-4">
                  <div className="flex items-center justify-between gap-3">
                    <div className="font-medium">{name}</div>
                    <Badge className={task.running ? 'bg-emerald-100 text-emerald-700 border-0' : 'bg-amber-100 text-amber-700 border-0'}>
                      {task.running ? <CheckCircle2 className="w-3 h-3 mr-1" /> : <AlertTriangle className="w-3 h-3 mr-1" />}
                      {task.running ? 'Running' : 'Stopped'}
                    </Badge>
                  </div>
                  <div className="mt-3 grid gap-1 text-xs text-slate-500">
                    <div>Last tick: {task.last_tick_at || 'none'}</div>
                    <div>Last success: {task.last_success_at || 'none'}</div>
                    <div>Restarts: {task.restarts || 0}</div>
                    <div>Failures: {task.consecutive_failures || 0}</div>
                  </div>
                </div>
              ))}
            </div>
          </section>

          <section className="grid gap-5 md:grid-cols-2">
            <div className="bg-white rounded-2xl border border-slate-100 p-5">
              <h2 className="font-semibold mb-3">Webhook</h2>
              <div className="space-y-2 text-sm text-slate-600">
                <div>Last received: {health.webhook?.last_received_at || 'none'}</div>
                <div>Last processed: {health.webhook?.last_processed_at || 'none'}</div>
              </div>
            </div>
            <div className="bg-white rounded-2xl border border-slate-100 p-5">
              <h2 className="font-semibold mb-3">Queue</h2>
              <div className="space-y-2 text-sm text-slate-600">
                <div>Pending DM sessions: {health.jobs?.pending_comment_dm_sessions ?? 0}</div>
                <div>Failed DM sessions: {health.jobs?.failed_comment_dm_sessions ?? 0}</div>
                <div>Queue interval: {health.config?.automation_queue_interval_seconds ?? '-'}s</div>
                <div>Queue batch size: {health.config?.automation_queue_batch_size ?? '-'}</div>
              </div>
            </div>
          </section>
        </div>
      )}
    </div>
  );
};

export default SystemHealth;

import React, { useState, useEffect, useCallback } from 'react';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Badge } from '../components/ui/badge';
import { Switch } from '../components/ui/switch';
import { Inbox, Loader2, Trash2, RefreshCcw, Play, Bug, Wifi } from 'lucide-react';
import { toast } from 'sonner';
import api from '../lib/api';

const MATCH_MODES = [
  { value: 'contains', label: 'contains' },
  { value: 'exact', label: 'exact' },
  { value: 'starts_with', label: 'starts with' },
];

const STATUS_COLORS = {
  replied: 'bg-emerald-100 text-emerald-700',
  matched: 'bg-blue-100 text-blue-700',
  failed: 'bg-rose-100 text-rose-700',
  skipped: 'bg-slate-100 text-slate-600',
  received: 'bg-amber-100 text-amber-700',
};

const fmtTime = (iso) => {
  if (!iso) return '—';
  try { return new Date(iso).toLocaleString(); } catch { return iso; }
};

const DmAutomation = () => {
  const [diag, setDiag] = useState(null);
  const [diagLoading, setDiagLoading] = useState(false);
  const [rules, setRules] = useState([]);
  const [logs, setLogs] = useState([]);
  const [form, setForm] = useState({ name: '', keyword: '', matchMode: 'contains', replyText: '', isActive: true });
  const [saving, setSaving] = useState(false);
  const [testText, setTestText] = useState('');
  const [testResult, setTestResult] = useState(null);
  const [debug, setDebug] = useState(null);
  const [debugLoading, setDebugLoading] = useState(false);
  const [resubLoading, setResubLoading] = useState(false);

  const loadAll = useCallback(async () => {
    try {
      const [r, l, d] = await Promise.all([
        api.get('/instagram/dm/rules'),
        api.get('/instagram/dm/logs?limit=50'),
        api.get('/instagram/dm/diagnostics'),
      ]);
      setRules(r.data.items || []);
      setLogs(l.data.items || []);
      setDiag(d.data);
    } catch (e) {
      toast.error(e?.response?.data?.detail || 'Failed to load DM data');
    }
  }, []);

  useEffect(() => { loadAll(); }, [loadAll]);

  const refreshDiag = async () => {
    setDiagLoading(true);
    try {
      const { data } = await api.get('/instagram/dm/diagnostics');
      setDiag(data);
    } catch (e) {
      toast.error(e?.response?.data?.detail || 'Diagnostics failed');
    } finally {
      setDiagLoading(false);
    }
  };

  const saveRule = async () => {
    if (!form.name.trim() || !form.keyword.trim() || !form.replyText.trim()) {
      toast.error('Name, keyword and reply are required');
      return;
    }
    setSaving(true);
    try {
      await api.post('/instagram/dm/rules', form);
      toast.success('Rule created');
      setForm({ name: '', keyword: '', matchMode: 'contains', replyText: '', isActive: true });
      await loadAll();
    } catch (e) {
      toast.error(e?.response?.data?.detail || 'Failed to create rule');
    } finally {
      setSaving(false);
    }
  };

  const toggleActive = async (rule) => {
    try {
      await api.patch(`/instagram/dm/rules/${rule.id}`, { isActive: !rule.isActive });
      await loadAll();
    } catch (e) {
      toast.error('Failed to toggle');
    }
  };

  const deleteRule = async (rule) => {
    if (!window.confirm(`Delete rule "${rule.name}"?`)) return;
    try {
      await api.delete(`/instagram/dm/rules/${rule.id}`);
      toast.success('Deleted');
      await loadAll();
    } catch (e) {
      toast.error('Failed to delete');
    }
  };

  const runDebug = async () => {
    setDebugLoading(true);
    try {
      const { data } = await api.get('/instagram/dm/debug-latest');
      setDebug(data);
    } catch (e) {
      toast.error(e?.response?.data?.detail || 'Debug failed');
    } finally {
      setDebugLoading(false);
    }
  };

  const resubscribe = async () => {
    setResubLoading(true);
    try {
      const { data } = await api.post('/instagram/dm/resubscribe');
      if (data.messagesSubscribed) {
        toast.success('Resubscribed: messages field is active');
      } else {
        toast.error('Resubscribe call returned, but messages still not in subscribed_fields');
      }
      await runDebug();
    } catch (e) {
      toast.error(e?.response?.data?.detail || 'Resubscribe failed');
    } finally {
      setResubLoading(false);
    }
  };

  const runTest = async () => {
    if (!testText.trim()) { toast.error('Enter sample text'); return; }
    try {
      const { data } = await api.post('/instagram/dm/test-rule', { text: testText });
      setTestResult(data);
    } catch (e) {
      toast.error(e?.response?.data?.detail || 'Test failed');
    }
  };

  return (
    <div className="p-8 max-w-6xl mx-auto">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="font-display text-3xl font-extrabold tracking-tight flex items-center gap-2">
            <Inbox className="w-7 h-7" /> Instagram DM Automation
          </h1>
          <p className="mt-1 text-slate-600">Auto-reply to direct messages based on keyword rules. Independent of comments.</p>
        </div>
        <Button onClick={refreshDiag} variant="outline" className="rounded-xl" disabled={diagLoading}>
          {diagLoading ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <RefreshCcw className="w-4 h-4 mr-2" />}
          Refresh status
        </Button>
      </div>

      {/* Status pills */}
      {diag && (
        <Card className="mt-6 p-4 rounded-2xl border-slate-100">
          <div className="flex flex-wrap gap-2">
            {[
              ['Instagram connected', diag.connected],
              ['Messaging webhook subscribed', diag.messagingWebhookSubscribed],
              [`Active DM rules: ${diag.activeDmRules}`, diag.activeDmRules > 0],
            ].map(([k, v]) => (
              <Badge key={k} className={`rounded-full border-0 ${v ? 'bg-emerald-100 text-emerald-700' : 'bg-rose-100 text-rose-700'}`}>
                {v ? '✓' : '✗'} {k}
              </Badge>
            ))}
            <Badge className="rounded-full border-0 bg-slate-100 text-slate-700">
              Recent messaging events: {diag.recentMessagingEvents ?? 0}
            </Badge>
            <Badge className="rounded-full border-0 bg-slate-100 text-slate-700">
              Last DM: {diag.lastMessageAt ? fmtTime(diag.lastMessageAt) : 'none'}
            </Badge>
            <Badge className={`rounded-full border-0 ${STATUS_COLORS[diag.lastReplyStatus] || 'bg-slate-100 text-slate-700'}`}>
              Last reply: {diag.lastReplyStatus || 'none'}
            </Badge>
          </div>
          {diag.blockerReason && (
            <div className="mt-3 p-2 rounded-md bg-amber-50 border border-amber-200 text-amber-900 text-sm">
              <b>Blocker:</b> {diag.blockerReason}
            </div>
          )}
          {diag.subscriptionError && (
            <div className="mt-2 text-xs text-rose-700">Subscription check error: {diag.subscriptionError}</div>
          )}
        </Card>
      )}

      {/* DM debug panel */}
      <Card className="mt-6 p-6 rounded-2xl border-slate-100">
        <div className="flex items-center justify-between flex-wrap gap-2">
          <div>
            <h3 className="font-display font-bold text-lg flex items-center gap-2">
              <Bug className="w-5 h-5" /> DM debug
            </h3>
            <p className="text-sm text-slate-500">Reads live production data: webhook_log, dm_logs, dm_rules, and the IG account's subscribed_fields. No tokens or raw payloads exposed.</p>
          </div>
          <div className="flex gap-2">
            <Button onClick={resubscribe} disabled={resubLoading} variant="outline" className="rounded-xl">
              {resubLoading ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Wifi className="w-4 h-4 mr-2" />}
              Resubscribe messaging webhook
            </Button>
            <Button onClick={runDebug} disabled={debugLoading} className="rounded-xl bg-slate-900 text-white">
              {debugLoading ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Bug className="w-4 h-4 mr-2" />}
              Run DM debug
            </Button>
          </div>
        </div>

        {debug && (
          <div className="mt-4 space-y-4">
            {/* Identity panel */}
            <div className="p-3 rounded-xl border border-slate-200 bg-slate-50 text-xs">
              <div className="font-semibold mb-1 text-sm">Identity</div>
              <div className="grid md:grid-cols-2 gap-x-6 gap-y-1">
                <div>DB ig_user_id: <span className="font-mono">{debug.identity?.dbIgUserIdRedacted || '—'}</span></div>
                <div>Graph /me id: <span className="font-mono">{debug.identity?.graphMeIdRedacted || '—'}</span></div>
                <div>Username: {debug.identity?.graphUsername || '—'}</div>
                <div>Account type: {debug.identity?.graphAccountType || '—'}</div>
                <div>Subscribed_apps for: <span className="font-mono">{debug.identity?.subscribedAppsCheckedForIgUserIdRedacted || '—'}</span></div>
                <div>ID match: <span className={debug.identity?.idMatch ? 'text-emerald-700' : 'text-rose-700'}>{debug.identity?.idMatch ? 'yes' : 'no'}</span></div>
              </div>
              <div className="mt-1">Recent webhook entry IDs: <span className="font-mono">{(debug.identity?.latestWebhookEntryIds || []).join(', ') || '—'}</span></div>
              <div>Recent recipient IDs: <span className="font-mono">{(debug.identity?.latestWebhookRecipientIds || []).join(', ') || '—'}</span></div>
              <div>Recent sender IDs: <span className="font-mono">{(debug.identity?.latestWebhookSenderIds || []).join(', ') || '—'}</span></div>
              {debug.identity?.mismatchReason && (
                <div className="mt-1 text-rose-700">Mismatch: {debug.identity.mismatchReason}</div>
              )}
            </div>

            {/* Webhook config panel */}
            <div className="p-3 rounded-xl border border-slate-200 bg-slate-50 text-xs">
              <div className="font-semibold mb-1 text-sm">Webhook config</div>
              <div className="grid md:grid-cols-2 gap-x-6 gap-y-1">
                <div>Callback URL: <span className="font-mono break-all">{debug.webhookConfig?.callbackUrlUsedByRuntime}</span></div>
                <div>Path: <span className="font-mono">{debug.webhookConfig?.expectedWebhookPath}</span></div>
                <div>Verify token configured: {debug.webhookConfig?.verifyTokenConfigured ? '✓' : '✗'}</div>
                <div>App ID configured: {debug.webhookConfig?.appIdConfigured ? '✓' : '✗'}</div>
                <div>App secret configured: {debug.webhookConfig?.appSecretConfigured ? '✓' : '✗'}</div>
                <div>Signature validation: {debug.webhookConfig?.signatureValidationEnabled ? 'on' : 'off'}</div>
                <div>Graph API: {debug.webhookConfig?.graphApiVersion} @ {debug.webhookConfig?.graphHost}</div>
                <div>Webhook events stored: {debug.webhookConfig?.webhookEventsStored}</div>
              </div>
            </div>

            {/* Processor panel */}
            <div className="p-3 rounded-xl border border-slate-200 bg-slate-50 text-xs">
              <div className="font-semibold mb-1 text-sm">Processor</div>
              <div className="grid md:grid-cols-2 gap-x-6 gap-y-1">
                <div>Webhook messaging events seen: {debug.processor?.webhookEventsCount}</div>
                <div>dm_logs for current user: {debug.processor?.dmLogsForCurrentUser}</div>
                <div>dm_logs global recent: {debug.processor?.dmLogsGlobalRecent}</div>
                <div>Unmapped messaging events: {debug.processor?.unmappedMessagingEvents}</div>
              </div>
              {debug.processor?.recentSkipReasons?.length > 0 && (
                <div className="mt-1">Recent skip reasons: {debug.processor.recentSkipReasons.join(', ')}</div>
              )}
            </div>

            <div className="text-[11px] text-slate-500">
              Note: First verify ID mapping and webhook payload shape. In Development mode, app roles may affect some tests, but do not assume this is the cause until ID mapping and payload handling are proven correct.
            </div>

            {/* Decision pills */}
            <div className="flex flex-wrap gap-2">
              {[
                ['Messaging webhook received', debug.lastDecision?.webhookReceived],
                ['Message text extracted', debug.lastDecision?.messageParsed],
                ['Active rule matched', debug.lastDecision?.ruleMatched],
                ['Reply attempted', debug.lastDecision?.sendAttempted],
                ['Reply sent', debug.lastDecision?.replySent],
              ].map(([k, v]) => (
                <Badge key={k} className={`rounded-full border-0 ${v ? 'bg-emerald-100 text-emerald-700' : 'bg-rose-100 text-rose-700'}`}>
                  {v ? '✓' : '✗'} {k}: {v ? 'yes' : 'no'}
                </Badge>
              ))}
            </div>

            {/* Blocker / fix */}
            {debug.lastDecision?.blocker ? (
              <div className="p-3 rounded-xl bg-rose-50 border border-rose-200 text-sm">
                <div className="font-semibold text-rose-900">Blocker: {debug.lastDecision.blocker}</div>
                {debug.lastDecision.fix && (
                  <div className="mt-1 text-rose-800">Fix: {debug.lastDecision.fix}</div>
                )}
              </div>
            ) : (
              <div className="p-3 rounded-xl bg-emerald-50 border border-emerald-200 text-sm text-emerald-900">
                No blocker detected. {debug.lastDecision?.fix || ''}
              </div>
            )}

            {/* Subscribed fields */}
            <div className="text-xs text-slate-600">
              Subscribed fields:{' '}
              {(debug.subscribedFields || []).length === 0
                ? <span className="text-rose-700">none</span>
                : (debug.subscribedFields || []).map(f => (
                  <Badge key={f} className="rounded-full mr-1 border-0 bg-slate-100 text-slate-700">{f}</Badge>
                ))}
              {debug.subscriptionError && (
                <span className="ml-2 text-rose-700">err: {debug.subscriptionError}</span>
              )}
            </div>

            {/* Recent webhook events */}
            <div>
              <div className="text-sm font-semibold mb-1">Recent messaging webhook events ({debug.recentWebhookEvents?.length || 0})</div>
              {(!debug.recentWebhookEvents || debug.recentWebhookEvents.length === 0) ? (
                <div className="text-xs text-slate-500">No webhook events recorded for this IG account in the last 50 deliveries.</div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead><tr className="text-left text-slate-500 border-b border-slate-100">
                      <th className="py-1">Time</th><th>Kind</th><th>Item keys</th>
                      <th>Msg keys</th><th>Sender</th><th>Recip</th>
                      <th>Msg id</th><th>Text</th><th>Read</th><th>Deliv</th>
                      <th>Postback</th><th>React</th><th>Echo</th><th>Preview</th>
                    </tr></thead>
                    <tbody>
                      {debug.recentWebhookEvents.map((ev, i) => (
                        <tr key={i} className="border-b border-slate-50 align-top">
                          <td className="py-1 whitespace-nowrap">{fmtTime(ev.createdAt)}</td>
                          <td><Badge className="rounded-full border-0 bg-slate-100 text-slate-700">{ev.eventKind}</Badge></td>
                          <td className="font-mono text-[10px]">{(ev.messagingItemKeys || []).join(',')}</td>
                          <td className="font-mono text-[10px]">{(ev.messageKeys || []).join(',')}</td>
                          <td>{ev.senderPresent ? (ev.senderIdRedacted || '✓') : '✗'}</td>
                          <td>{ev.recipientPresent ? '✓' : '✗'}</td>
                          <td>{ev.messageIdPresent ? '✓' : '✗'}</td>
                          <td>{ev.messageTextPresent ? '✓' : '✗'}</td>
                          <td>{ev.hasRead ? '✓' : '—'}</td>
                          <td>{ev.hasDelivery ? '✓' : '—'}</td>
                          <td>{ev.hasPostback ? '✓' : '—'}</td>
                          <td>{ev.hasReaction ? '✓' : '—'}</td>
                          <td>{ev.isEcho ? '✓' : '—'}</td>
                          <td className="font-mono">{ev.textPreview}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>

            {/* Recent dm logs */}
            <div>
              <div className="text-sm font-semibold mb-1">Recent DM processor logs ({debug.recentDmLogs?.length || 0})</div>
              {(!debug.recentDmLogs || debug.recentDmLogs.length === 0) ? (
                <div className="text-xs text-slate-500">No dm_logs rows yet. If messaging events are arriving, the processor is not running for this user.</div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead><tr className="text-left text-slate-500 border-b border-slate-100">
                      <th className="py-1">Time</th><th>Kind</th><th>Sender</th><th>Text</th>
                      <th>Rule</th><th>Status</th><th>Skip reason</th><th>Error</th>
                    </tr></thead>
                    <tbody>
                      {debug.recentDmLogs.map((l, i) => (
                        <tr key={i} className="border-b border-slate-50 align-top">
                          <td className="py-1 whitespace-nowrap">{fmtTime(l.createdAt)}</td>
                          <td><Badge className="rounded-full border-0 bg-slate-100 text-slate-700">{l.eventKind || '—'}</Badge></td>
                          <td className="font-mono">{l.senderId}</td>
                          <td className="max-w-xs truncate">{l.incomingText || ''}</td>
                          <td>{l.matchedRuleName || '—'}</td>
                          <td><Badge className={`rounded-full border-0 ${STATUS_COLORS[l.status] || 'bg-slate-100 text-slate-700'}`}>{l.status}</Badge></td>
                          <td>{l.skipReason || ''}</td>
                          <td className="text-rose-700 max-w-xs truncate">{l.error || ''}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )}
      </Card>

      {/* Create rule form */}
      <Card className="mt-6 p-6 rounded-2xl border-slate-100">
        <h3 className="font-display font-bold text-lg">Create DM rule</h3>
        <div className="mt-4 grid md:grid-cols-2 gap-4">
          <div className="space-y-2">
            <Label>Rule name</Label>
            <Input className="h-11 rounded-xl" placeholder="e.g. Welcome reply"
              value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} />
          </div>
          <div className="space-y-2">
            <Label>Keyword</Label>
            <Input className="h-11 rounded-xl" placeholder="e.g. hello"
              value={form.keyword} onChange={e => setForm({ ...form, keyword: e.target.value })} />
          </div>
          <div className="space-y-2">
            <Label>Match mode</Label>
            <select className="h-11 w-full rounded-xl border border-slate-200 px-3 bg-white"
              value={form.matchMode} onChange={e => setForm({ ...form, matchMode: e.target.value })}>
              {MATCH_MODES.map(m => <option key={m.value} value={m.value}>{m.label}</option>)}
            </select>
          </div>
          <div className="space-y-2">
            <Label>Active</Label>
            <div className="h-11 flex items-center">
              <Switch checked={form.isActive} onCheckedChange={v => setForm({ ...form, isActive: v })} />
              <span className="ml-2 text-sm text-slate-600">{form.isActive ? 'Active' : 'Inactive'}</span>
            </div>
          </div>
          <div className="md:col-span-2 space-y-2">
            <Label>Reply message</Label>
            <textarea rows={3} className="w-full rounded-xl border border-slate-200 p-3"
              placeholder="Hi, thanks for your message!"
              value={form.replyText} onChange={e => setForm({ ...form, replyText: e.target.value })} />
          </div>
        </div>
        <div className="mt-4 flex justify-end">
          <Button onClick={saveRule} disabled={saving} className="rounded-xl bg-slate-900 text-white">
            {saving ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : null}
            Save rule
          </Button>
        </div>
      </Card>

      {/* Rules table */}
      <Card className="mt-6 p-6 rounded-2xl border-slate-100">
        <h3 className="font-display font-bold text-lg">DM rules ({rules.length})</h3>
        {rules.length === 0 ? (
          <div className="mt-4 text-sm text-slate-500">No rules yet.</div>
        ) : (
          <div className="mt-4 overflow-x-auto">
            <table className="w-full text-sm">
              <thead><tr className="text-left text-slate-500 border-b border-slate-100">
                <th className="py-2">Name</th><th>Keyword</th><th>Mode</th><th>Reply</th><th>Active</th><th></th>
              </tr></thead>
              <tbody>
                {rules.map(r => (
                  <tr key={r.id} className="border-b border-slate-50">
                    <td className="py-2">{r.name}</td>
                    <td className="font-mono">{r.keyword}</td>
                    <td>{r.matchMode}</td>
                    <td className="max-w-xs truncate">{r.replyText}</td>
                    <td><Switch checked={r.isActive} onCheckedChange={() => toggleActive(r)} /></td>
                    <td className="text-right">
                      <Button onClick={() => deleteRule(r)} variant="ghost" size="sm" className="text-rose-600">
                        <Trash2 className="w-4 h-4" />
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {/* Test rule box */}
      <Card className="mt-6 p-6 rounded-2xl border-slate-100">
        <h3 className="font-display font-bold text-lg">Test rule matching</h3>
        <p className="text-sm text-slate-500">Check which active rule would match a given message — does not send anything.</p>
        <div className="mt-4 flex gap-2">
          <Input className="h-11 rounded-xl flex-1" placeholder='e.g. "hello there"'
            value={testText} onChange={e => setTestText(e.target.value)} />
          <Button onClick={runTest} className="rounded-xl bg-slate-900 text-white">
            <Play className="w-4 h-4 mr-2" /> Test
          </Button>
        </div>
        {testResult && (
          <div className="mt-3 p-3 rounded-xl bg-slate-50 border border-slate-200 text-sm">
            <div>Input: <code className="text-xs">{testResult.inputText}</code></div>
            <div>Matched rules: <b>{testResult.matchCount}</b></div>
            {testResult.firstMatch && (
              <div className="mt-2 text-xs">
                First match: <b>{testResult.firstMatch.name}</b> ({testResult.firstMatch.matchMode}: <code>{testResult.firstMatch.keyword}</code>)
                <div className="text-slate-600">→ would reply: "{testResult.firstMatch.replyText}"</div>
              </div>
            )}
          </div>
        )}
      </Card>

      {/* Logs */}
      <Card className="mt-6 p-6 rounded-2xl border-slate-100">
        <div className="flex items-center justify-between">
          <h3 className="font-display font-bold text-lg">Recent DM events ({logs.length})</h3>
          <Button onClick={loadAll} variant="ghost" size="sm" className="rounded-xl">
            <RefreshCcw className="w-4 h-4 mr-2" /> Refresh
          </Button>
        </div>
        {logs.length === 0 ? (
          <div className="mt-4 text-sm text-slate-500">No DM events yet.</div>
        ) : (
          <div className="mt-4 overflow-x-auto">
            <table className="w-full text-sm">
              <thead><tr className="text-left text-slate-500 border-b border-slate-100">
                <th className="py-2">Time</th><th>Incoming</th><th>Matched rule</th><th>Status</th><th>Error</th>
              </tr></thead>
              <tbody>
                {logs.map(l => {
                  const matched = rules.find(r => r.id === l.matchedRuleId);
                  return (
                    <tr key={l.id} className="border-b border-slate-50 align-top">
                      <td className="py-2 whitespace-nowrap text-xs text-slate-500">{fmtTime(l.created)}</td>
                      <td className="max-w-xs truncate">{l.incomingText}</td>
                      <td>{matched?.name || (l.matchedRuleId ? l.matchedRuleId.slice(0, 8) : '—')}</td>
                      <td>
                        <Badge className={`rounded-full border-0 ${STATUS_COLORS[l.status] || 'bg-slate-100 text-slate-700'}`}>
                          {l.status}
                        </Badge>
                      </td>
                      <td className="text-xs text-rose-700 max-w-xs truncate">{l.error || ''}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
};

export default DmAutomation;

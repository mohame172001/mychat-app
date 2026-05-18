import React, { useState, useEffect, useCallback } from 'react';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Badge } from '../components/ui/badge';
import { Switch } from '../components/ui/switch';
import { Inbox, Loader2, Trash2, RefreshCcw } from 'lucide-react';
import { toast } from 'sonner';
import api from '../lib/api';
import { useTranslation } from '../lib/i18n';

function matchModes(lang) {
  return [
    { value: 'contains', label: lang === 'ar' ? 'يحتوي على' : 'contains' },
    { value: 'exact', label: lang === 'ar' ? 'مطابقة تامة' : 'exact' },
    { value: 'starts_with', label: lang === 'ar' ? 'يبدأ بـ' : 'starts with' },
  ];
}

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
  const { t, lang } = useTranslation();
  const MATCH_MODES = matchModes(lang);
  const [diag, setDiag] = useState(null);
  const [diagLoading, setDiagLoading] = useState(false);
  const [rules, setRules] = useState([]);
  const [logs, setLogs] = useState([]);
  const [form, setForm] = useState({ name: '', keyword: '', matchMode: 'contains', replyText: '', isActive: true });
  const [saving, setSaving] = useState(false);

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
      toast.error(e?.response?.data?.detail || (lang === 'ar' ? 'تعذّر تحميل بيانات الرسائل' : 'Failed to load DM data'));
    }
  }, []);

  useEffect(() => { loadAll(); }, [loadAll]);

  const refreshDiag = async () => {
    setDiagLoading(true);
    try {
      const { data } = await api.get('/instagram/dm/diagnostics');
      setDiag(data);
    } catch (e) {
      toast.error(e?.response?.data?.detail || (lang === 'ar' ? 'فشل التشخيص' : 'Diagnostics failed'));
    } finally {
      setDiagLoading(false);
    }
  };

  const saveRule = async () => {
    if (!form.name.trim() || !form.keyword.trim() || !form.replyText.trim()) {
      toast.error(lang === 'ar' ? 'الاسم والكلمة المفتاحية والردّ مطلوبة' : 'Name, keyword and reply are required');
      return;
    }
    setSaving(true);
    try {
      await api.post('/instagram/dm/rules', form);
      toast.success(lang === 'ar' ? 'تمّ إنشاء القاعدة' : 'Rule created');
      setForm({ name: '', keyword: '', matchMode: 'contains', replyText: '', isActive: true });
      await loadAll();
    } catch (e) {
      toast.error(e?.response?.data?.detail || (lang === 'ar' ? 'تعذّر إنشاء القاعدة' : 'Failed to create rule'));
    } finally {
      setSaving(false);
    }
  };

  const toggleActive = async (rule) => {
    try {
      await api.patch(`/instagram/dm/rules/${rule.id}`, { isActive: !rule.isActive });
      await loadAll();
    } catch (e) {
      toast.error(lang === 'ar' ? 'تعذّر التبديل' : 'Failed to toggle');
    }
  };

  const deleteRule = async (rule) => {
    if (!window.confirm(lang === "ar" ? `حذف القاعدة "${rule.name}"؟` : `Delete rule "${rule.name}"?`)) return;
    try {
      await api.delete(`/instagram/dm/rules/${rule.id}`);
      toast.success(lang === 'ar' ? 'تمّ الحذف' : 'Deleted');
      await loadAll();
    } catch (e) {
      toast.error(lang === 'ar' ? 'تعذّر الحذف' : 'Failed to delete');
    }
  };

  return (
    <div className="p-4 sm:p-6 lg:p-8 max-w-6xl mx-auto">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="font-display text-3xl font-extrabold tracking-tight flex items-center gap-2">
            <Inbox className="w-7 h-7" /> {t('dmAutomation.title')}
          </h1>
          <p className="mt-1 text-slate-600">{t('dmAutomation.subtitle')}</p>
        </div>
        <Button onClick={refreshDiag} variant="outline" className="rounded-xl sm:w-auto" disabled={diagLoading}>
          {diagLoading ? <Loader2 className="w-4 h-4 me-2 animate-spin" /> : <RefreshCcw className="w-4 h-4 me-2" />}
          {lang === 'ar' ? 'تحديث الحالة' : 'Refresh status'}
        </Button>
      </div>

      {/* Status pills */}
      {diag && (
        <Card className="mt-6 p-4 rounded-2xl border-slate-100">
          <div className="flex flex-wrap gap-2">
            {[
              [lang === 'ar' ? 'Instagram مربوط' : 'Instagram connected', diag.connected],
              [lang === 'ar' ? 'الـwebhook مُشترَك' : 'Messaging webhook subscribed', diag.messagingWebhookSubscribed],
              [lang === 'ar' ? `قواعد الرسائل النشطة: ${diag.activeDmRules}` : `Active DM rules: ${diag.activeDmRules}`, diag.activeDmRules > 0],
            ].map(([k, v]) => (
              <Badge key={k} className={`rounded-full border-0 ${v ? 'bg-emerald-100 text-emerald-700' : 'bg-rose-100 text-rose-700'}`}>
                {v ? '✓' : '✗'} {k}
              </Badge>
            ))}
            <Badge className="rounded-full border-0 bg-slate-100 text-slate-700">
              {lang === 'ar' ? 'أحداث الرسائل الحديثة: ' : 'Recent messaging events: '}{diag.recentMessagingEvents ?? 0}
            </Badge>
            <Badge className="rounded-full border-0 bg-slate-100 text-slate-700">
              {lang === 'ar' ? 'آخر رسالة: ' : 'Last DM: '}{diag.lastMessageAt ? fmtTime(diag.lastMessageAt) : (lang === 'ar' ? 'لا يوجد' : 'none')}
            </Badge>
            <Badge className={`rounded-full border-0 ${STATUS_COLORS[diag.lastReplyStatus] || 'bg-slate-100 text-slate-700'}`}>
              {lang === 'ar' ? 'آخر ردّ: ' : 'Last reply: '}{diag.lastReplyStatus || (lang === 'ar' ? 'لا يوجد' : 'none')}
            </Badge>
          </div>
          {diag.subscriptionError && (
            <div className="mt-2 text-xs text-rose-700">{lang === 'ar' ? 'خطأ التحقّق من الاشتراك: ' : 'Subscription check error: '}{diag.subscriptionError}</div>
          )}
        </Card>
      )}

      {/* Create rule form */}
      <Card className="mt-6 p-6 rounded-2xl border-slate-100">
        <h3 className="font-display font-bold text-lg">{t('dmAutomation.createTitle')}</h3>
        <div className="mt-4 grid md:grid-cols-2 gap-4">
          <div className="space-y-2">
            <Label>{t('dmAutomation.ruleName')}</Label>
            <Input className="h-11 rounded-xl"
              value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} />
          </div>
          <div className="space-y-2">
            <Label>{t('dmAutomation.keyword')}</Label>
            <Input className="h-11 rounded-xl"
              value={form.keyword} onChange={e => setForm({ ...form, keyword: e.target.value })} />
          </div>
          <div className="space-y-2">
            <Label>{t('dmAutomation.matchMode')}</Label>
            <select className="h-11 w-full rounded-xl border border-slate-200 px-3 bg-white"
              value={form.matchMode} onChange={e => setForm({ ...form, matchMode: e.target.value })}>
              {MATCH_MODES.map(m => <option key={m.value} value={m.value}>{m.label}</option>)}
            </select>
          </div>
          <div className="space-y-2">
            <Label>{t('dmAutomation.active')}</Label>
            <div className="h-11 flex items-center">
              <Switch checked={form.isActive} onCheckedChange={v => setForm({ ...form, isActive: v })} />
              <span className="ms-2 text-sm text-slate-600">
                {form.isActive
                  ? (lang === 'ar' ? 'مُفعّلة' : 'Active')
                  : (lang === 'ar' ? 'مُعطّلة' : 'Inactive')}
              </span>
            </div>
          </div>
          <div className="md:col-span-2 space-y-2">
            <Label>{t('dmAutomation.replyMessage')}</Label>
            <textarea rows={3} className="w-full rounded-xl border border-slate-200 p-3"
              value={form.replyText} onChange={e => setForm({ ...form, replyText: e.target.value })} />
          </div>
        </div>
        <div className="mt-4 flex justify-end">
          <Button onClick={saveRule} disabled={saving} className="rounded-xl bg-slate-900 text-white">
            {saving ? <Loader2 className="w-4 h-4 me-2 animate-spin" /> : null}
            {t('dmAutomation.saveRule')}
          </Button>
        </div>
      </Card>

      {/* Rules table */}
      <Card className="mt-6 p-6 rounded-2xl border-slate-100">
        <h3 className="font-display font-bold text-lg">{t('dmAutomation.rulesCount')} ({rules.length})</h3>
        {rules.length === 0 ? (
          <div className="mt-4 text-sm text-slate-500">{t('dmAutomation.noRules')}</div>
        ) : (
          <div className="mt-4 overflow-x-auto">
            <table className="w-full text-sm">
              <thead><tr className="text-start text-slate-500 border-b border-slate-100">
                <th className="py-2">{lang === 'ar' ? 'الاسم' : 'Name'}</th>
                <th>{t('dmAutomation.keyword')}</th>
                <th>{t('dmAutomation.matchMode')}</th>
                <th>{lang === 'ar' ? 'الرد' : 'Reply'}</th>
                <th>{t('dmAutomation.active')}</th>
                <th></th>
              </tr></thead>
              <tbody>
                {rules.map(r => (
                  <tr key={r.id} className="border-b border-slate-50">
                    <td className="py-2">{r.name}</td>
                    <td className="font-mono">{r.keyword}</td>
                    <td>{r.matchMode}</td>
                    <td className="max-w-xs truncate">{r.replyText}</td>
                    <td><Switch checked={r.isActive} onCheckedChange={() => toggleActive(r)} /></td>
                    <td className="text-end">
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

      {/* Logs */}
      <Card className="mt-6 p-6 rounded-2xl border-slate-100">
        <div className="flex items-center justify-between">
          <h3 className="font-display font-bold text-lg">{t('dmAutomation.recentEventsTitle')} ({logs.length})</h3>
          <Button onClick={loadAll} variant="ghost" size="sm" className="rounded-xl">
            <RefreshCcw className="w-4 h-4 me-2" /> {t('common.refresh')}
          </Button>
        </div>
        {logs.length === 0 ? (
          <div className="mt-4 text-sm text-slate-500">{t('dmAutomation.noEvents')}</div>
        ) : (
          <div className="mt-4 overflow-x-auto">
            <table className="w-full text-sm">
              <thead><tr className="text-start text-slate-500 border-b border-slate-100">
                <th className="py-2">{lang === 'ar' ? 'الوقت' : 'Time'}</th>
                <th>{lang === 'ar' ? 'الرسالة الواردة' : 'Incoming'}</th>
                <th>{lang === 'ar' ? 'القاعدة المُطابِقة' : 'Matched rule'}</th>
                <th>{lang === 'ar' ? 'الحالة' : 'Status'}</th>
                <th>{lang === 'ar' ? 'الخطأ' : 'Error'}</th>
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

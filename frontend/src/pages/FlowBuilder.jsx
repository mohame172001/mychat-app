import React, { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import { ArrowLeft, Pencil, Trash2, Loader2, Instagram } from 'lucide-react';
import { toast } from 'sonner';
import { useTranslation } from '../lib/i18n';
import { automationsApi } from '../api/automationsApi';
import { ROUTES } from '../constants/routes';

const FlowBuilder = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  const { lang } = useTranslation();
  const ar = lang === 'ar';
  const [auto, setAuto] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    (async () => {
      try {
        const { data } = await automationsApi.get(id);
        setAuto(data);
      } catch {
        toast.error(ar ? 'غير موجود' : 'Not found');
        navigate(ROUTES.APP_AUTOMATIONS);
      }
      setLoading(false);
    })();
  }, [id, navigate, ar]);

  const handleDelete = async () => {
    try { await automationsApi.remove(id); toast.success(ar ? 'تم الحذف' : 'Deleted'); navigate(ROUTES.APP_AUTOMATIONS); }
    catch { toast.error(ar ? 'فشل العملية' : 'Failed'); }
  };

  const toggleStatus = async () => {
    const newStatus = auto.status === 'active' ? 'paused' : 'active';
    try {
      const { data } = await automationsApi.update(id, { status: newStatus });
      setAuto(data);
    } catch { toast.error(ar ? 'فشل العملية' : 'Failed'); }
  };

  if (loading) {
    return <div className="p-10 flex justify-center text-slate-500"><Loader2 className="w-5 h-5 animate-spin" /></div>;
  }
  if (!auto) return null;

  const thumb = auto.media_preview?.thumbnail_url;
  const postLabel = auto.latest
    ? (ar ? 'أحدث منشور' : 'Latest post')
    : (auto.media_preview?.caption || auto.media_id || '—');
  const matchLabel = auto.match === 'keyword' && auto.keyword
    ? (ar ? `عندما يحتوي التعليق على "${auto.keyword}"` : `When comment contains "${auto.keyword}"`)
    : (ar ? 'أي تعليق' : 'Any comment');
  const modeLabel = auto.mode === 'reply_only'
    ? (ar ? 'ردّ فقط' : 'Reply only')
    : (ar ? 'ردّ + رسالة' : 'Reply + DM');
  const statusLabel = ar
    ? (auto.status === 'active' ? 'نشطة' : auto.status === 'paused' ? 'متوقّفة' : auto.status === 'draft' ? 'مسودّة' : auto.status)
    : auto.status;

  return (
    <div className="p-4 sm:p-6 lg:p-8 max-w-3xl mx-auto">
      <Button variant="ghost" onClick={() => navigate(ROUTES.APP_AUTOMATIONS)} className="mb-4">
        <ArrowLeft className="w-4 h-4 me-1.5" /> {ar ? 'رجوع' : 'Back'}
      </Button>

      <Card className="p-6 rounded-2xl border-slate-100">
        <div className="flex items-start gap-4 flex-wrap">
          <div className="w-20 h-20 rounded-xl overflow-hidden shrink-0 bg-gradient-to-br from-pink-500 via-fuchsia-500 to-orange-400 flex items-center justify-center">
            {thumb ? <img src={thumb} alt="" className="w-full h-full object-cover" /> : <Instagram className="w-8 h-8 text-white" />}
          </div>
          <div className="flex-1 min-w-[200px]">
            <h1 className="font-display text-2xl font-extrabold">{auto.name}</h1>
            <div className="text-sm text-slate-500 mt-1">{postLabel}</div>
          </div>
          <Badge className={`rounded-full ${auto.status === 'active' ? 'bg-emerald-50 text-emerald-700 border-emerald-100' : 'bg-slate-100 text-slate-600 border-slate-200'}`}>
            {statusLabel}
          </Badge>
        </div>

        <div className="mt-6 grid sm:grid-cols-3 gap-3 text-sm">
          <div className="p-4 rounded-xl bg-slate-50 border border-slate-100">
            <div className="text-xs text-slate-500">{ar ? 'الإجراء' : 'Action'}</div>
            <div className="mt-1 font-semibold">{modeLabel}</div>
          </div>
          <div className="p-4 rounded-xl bg-slate-50 border border-slate-100">
            <div className="text-xs text-slate-500">{ar ? 'المُحفّز' : 'Trigger'}</div>
            <div className="mt-1 font-semibold">{matchLabel}</div>
          </div>
          <div className="p-4 rounded-xl bg-slate-50 border border-slate-100">
            <div className="text-xs text-slate-500">{ar ? 'مرّات التشغيل' : 'Fired'}</div>
            <div className="mt-1 font-semibold">{(auto.sent || 0).toLocaleString()}</div>
          </div>
        </div>

        <div className="mt-6 space-y-3">
          {auto.comment_reply && (
            <div className="p-4 rounded-xl border border-slate-100">
              <div className="text-xs text-slate-500">{ar ? 'الردّ العلني' : 'Public reply'}</div>
              <div className="mt-1">{auto.comment_reply}</div>
            </div>
          )}
          {auto.dm_text && (
            <div className="p-4 rounded-xl border border-slate-100">
              <div className="text-xs text-slate-500">{ar ? 'الرسالة الخاصة' : 'Private DM'}</div>
              <div className="mt-1">{auto.dm_text}</div>
            </div>
          )}
        </div>

        <div className="mt-6 flex gap-2">
          <Button onClick={() => navigate(`${ROUTES.APP_AUTOMATIONS}?edit=${id}`)} className="rounded-xl bg-slate-900 text-white">
            <Pencil className="w-4 h-4 me-1.5" /> {ar ? 'تعديل' : 'Edit'}
          </Button>
          <Button onClick={toggleStatus} variant="outline" className="rounded-xl">
            {auto.status === 'active' ? (ar ? 'إيقاف مؤقت' : 'Pause') : (ar ? 'تفعيل' : 'Activate')}
          </Button>
          <Button onClick={handleDelete} variant="ghost" className="rounded-xl text-red-600 hover:bg-red-50">
            <Trash2 className="w-4 h-4 me-1.5" /> {ar ? 'حذف' : 'Delete'}
          </Button>
        </div>

        <p className="mt-4 text-xs text-slate-500">
          {ar
            ? 'تعديل الأتمتة لا يؤثّر على عدد مرّات التشغيل أو سجلّ النشاط.'
            : "Editing keeps this automation's existing fired count and activity history."}
        </p>
      </Card>
    </div>
  );
};

export default FlowBuilder;

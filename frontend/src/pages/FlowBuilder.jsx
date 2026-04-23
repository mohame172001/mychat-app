import React, { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import { ArrowLeft, Trash2, Loader2, Instagram } from 'lucide-react';
import api from '../lib/api';
import { toast } from 'sonner';

const FlowBuilder = () => {
  const { id } = useParams();
  const navigate = useNavigate();
  const [auto, setAuto] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    (async () => {
      try {
        const { data } = await api.get(`/automations/${id}`);
        setAuto(data);
      } catch {
        toast.error('Not found');
        navigate('/app/automations');
      }
      setLoading(false);
    })();
  }, [id, navigate]);

  const handleDelete = async () => {
    try { await api.delete(`/automations/${id}`); toast.success('Deleted'); navigate('/app/automations'); }
    catch { toast.error('Failed'); }
  };

  const toggleStatus = async () => {
    const newStatus = auto.status === 'active' ? 'paused' : 'active';
    try {
      const { data } = await api.patch(`/automations/${id}`, { status: newStatus });
      setAuto(data);
    } catch { toast.error('Failed'); }
  };

  if (loading) {
    return <div className="p-10 flex justify-center text-slate-500"><Loader2 className="w-5 h-5 animate-spin" /></div>;
  }
  if (!auto) return null;

  const thumb = auto.media_preview?.thumbnail_url;
  const postLabel = auto.latest ? 'Latest post' : (auto.media_preview?.caption || auto.media_id || '—');
  const matchLabel = auto.match === 'keyword' && auto.keyword ? `When comment contains "${auto.keyword}"` : 'Any comment';
  const modeLabel = auto.mode === 'reply_only' ? 'Reply only' : 'Reply + DM';

  return (
    <div className="p-8 max-w-3xl mx-auto">
      <Button variant="ghost" onClick={() => navigate('/app/automations')} className="mb-4">
        <ArrowLeft className="w-4 h-4 mr-1.5" /> Back
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
            {auto.status}
          </Badge>
        </div>

        <div className="mt-6 grid sm:grid-cols-3 gap-3 text-sm">
          <div className="p-4 rounded-xl bg-slate-50 border border-slate-100">
            <div className="text-xs text-slate-500">Action</div>
            <div className="mt-1 font-semibold">{modeLabel}</div>
          </div>
          <div className="p-4 rounded-xl bg-slate-50 border border-slate-100">
            <div className="text-xs text-slate-500">Trigger</div>
            <div className="mt-1 font-semibold">{matchLabel}</div>
          </div>
          <div className="p-4 rounded-xl bg-slate-50 border border-slate-100">
            <div className="text-xs text-slate-500">Fired</div>
            <div className="mt-1 font-semibold">{(auto.sent || 0).toLocaleString()}</div>
          </div>
        </div>

        <div className="mt-6 space-y-3">
          {auto.comment_reply && (
            <div className="p-4 rounded-xl border border-slate-100">
              <div className="text-xs text-slate-500">Public reply</div>
              <div className="mt-1">{auto.comment_reply}</div>
            </div>
          )}
          {auto.dm_text && (
            <div className="p-4 rounded-xl border border-slate-100">
              <div className="text-xs text-slate-500">Private DM</div>
              <div className="mt-1">{auto.dm_text}</div>
            </div>
          )}
        </div>

        <div className="mt-6 flex gap-2">
          <Button onClick={toggleStatus} variant="outline" className="rounded-xl">
            {auto.status === 'active' ? 'Pause' : 'Activate'}
          </Button>
          <Button onClick={handleDelete} variant="ghost" className="rounded-xl text-red-600 hover:bg-red-50">
            <Trash2 className="w-4 h-4 mr-1.5" /> Delete
          </Button>
        </div>

        <p className="mt-4 text-xs text-slate-500">
          To change this rule, delete it and create a new one from the Automations page.
        </p>
      </Card>
    </div>
  );
};

export default FlowBuilder;

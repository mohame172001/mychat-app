import React, { useEffect, useState } from 'react';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Badge } from '../components/ui/badge';
import { Plus, Send, Calendar, Users, FileText, MoreVertical, Loader2 } from 'lucide-react';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter } from '../components/ui/dialog';
import { Label } from '../components/ui/label';
import api from '../lib/api';
import { toast } from 'sonner';

const statusColors = {
  sent: 'bg-emerald-50 text-emerald-700 border-emerald-100',
  scheduled: 'bg-blue-50 text-blue-700 border-blue-100',
  sending: 'bg-blue-50 text-blue-700 border-blue-100',
  draft: 'bg-slate-100 text-slate-600 border-slate-200',
};

const Broadcasting = () => {
  const [list, setList] = useState([]);
  const [open, setOpen] = useState(false);
  const [name, setName] = useState('');
  const [message, setMessage] = useState('');

  useEffect(() => {
    (async () => {
      try {
        const { data } = await api.get('/broadcasts');
        setList(data);
      } catch (err) {
        console.error('[Broadcasting] Failed to load:', err);
        toast.error('Failed to load broadcasts');
      }
    })();
  }, []);

  const handleCreate = async () => {
    if (!name || !message) {
      toast.error('Fill all fields');
      return;
    }
    try {
      const { data } = await api.post('/broadcasts', { name, message });
      setList(prev => [data, ...prev]);
      setOpen(false);
      setName('');
      setMessage('');
      toast.success('Broadcast created. Click Send to deliver it.');
    } catch {
      toast.error('Failed');
    }
  };

  const handleSend = async (bid) => {
    try {
      setList(prev => prev.map(b => b.id === bid ? { ...b, status: 'sending' } : b));
      const { data } = await api.post(`/broadcasts/${bid}/send`);
      toast.success(`Sending to ${data.recipients} contacts...`);
      const poll = setInterval(async () => {
        try {
          const { data: updated } = await api.get('/broadcasts');
          const found = updated.find(b => b.id === bid);
          if (found?.status === 'sent') {
            setList(updated);
            clearInterval(poll);
            toast.success('Broadcast sent');
          }
        } catch {
          clearInterval(poll);
        }
      }, 2000);
    } catch (e) {
      setList(prev => prev.map(b => b.id === bid ? { ...b, status: 'draft' } : b));
      toast.error(e?.response?.data?.detail || 'Failed to send');
    }
  };

  const stats = {
    sent: list.filter(b => b.status === 'sent').length,
    drafts: list.filter(b => b.status === 'draft').length,
    audience: list.reduce((total, b) => total + (Number(b.audience) || 0), 0),
  };

  return (
    <div className="p-4 sm:p-6 lg:p-8 max-w-7xl mx-auto">
      <div className="flex items-end justify-between flex-wrap gap-4">
        <div>
          <h1 className="font-display text-3xl font-extrabold tracking-tight">Broadcasting</h1>
          <p className="mt-1 text-slate-600">Send messages to real subscribed contacts from your account.</p>
        </div>
        <Dialog open={open} onOpenChange={setOpen}>
          <DialogTrigger asChild>
            <Button className="w-full bg-slate-900 hover:bg-slate-800 text-white rounded-xl sm:w-auto">
              <Plus className="w-4 h-4 mr-1.5" /> New Broadcast
            </Button>
          </DialogTrigger>
          <DialogContent className="rounded-2xl">
            <DialogHeader><DialogTitle className="font-display text-2xl">Create Broadcast</DialogTitle></DialogHeader>
            <div className="space-y-4 pt-2">
              <div className="space-y-2">
                <Label>Broadcast name</Label>
                <Input placeholder="Broadcast name" value={name} onChange={e => setName(e.target.value)} className="rounded-xl h-11" />
              </div>
              <div className="space-y-2">
                <Label>Message</Label>
                <textarea
                  value={message}
                  onChange={e => setMessage(e.target.value)}
                  placeholder="Write the message you want to send"
                  className="w-full min-h-[120px] px-3 py-2 rounded-xl border border-slate-200 text-sm focus:outline-none focus:ring-2 focus:ring-slate-900"
                />
              </div>
              <div className="flex items-center gap-2 text-sm text-slate-600">
                <Users className="w-4 h-4" /> Will be sent to your subscribed contacts
              </div>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => setOpen(false)} className="rounded-xl">Cancel</Button>
              <Button onClick={handleCreate} className="bg-slate-900 text-white rounded-xl">Create</Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      <div className="mt-8 grid sm:grid-cols-3 gap-4">
        <Card className="p-5 rounded-2xl border-slate-100">
          <div className="w-10 h-10 rounded-xl bg-blue-50 flex items-center justify-center"><Send className="w-5 h-5 text-blue-600" /></div>
          <div className="mt-4 text-3xl font-extrabold font-display">{stats.sent}</div>
          <div className="text-sm text-slate-500 mt-1">Broadcasts sent</div>
        </Card>
        <Card className="p-5 rounded-2xl border-slate-100">
          <div className="w-10 h-10 rounded-xl bg-emerald-50 flex items-center justify-center"><FileText className="w-5 h-5 text-emerald-600" /></div>
          <div className="mt-4 text-3xl font-extrabold font-display">{stats.drafts}</div>
          <div className="text-sm text-slate-500 mt-1">Draft broadcasts</div>
        </Card>
        <Card className="p-5 rounded-2xl border-slate-100">
          <div className="w-10 h-10 rounded-xl bg-pink-50 flex items-center justify-center"><Users className="w-5 h-5 text-pink-600" /></div>
          <div className="mt-4 text-3xl font-extrabold font-display">{stats.audience.toLocaleString()}</div>
          <div className="text-sm text-slate-500 mt-1">Recorded audience</div>
        </Card>
      </div>

      <Card className="mt-6 rounded-2xl border-slate-100 overflow-hidden">
        <div className="px-5 py-4 border-b border-slate-100"><h3 className="font-display font-bold text-lg">All Broadcasts</h3></div>
        <div className="divide-y divide-slate-50">
          {list.map(b => (
            <div key={b.id} className="grid grid-cols-1 sm:grid-cols-[1fr_auto] md:grid-cols-[1fr_120px_140px_120px_120px_40px] items-start sm:items-center gap-3 sm:gap-4 px-4 sm:px-5 py-4 hover:bg-slate-50 transition-colors">
              <div>
                <div className="font-semibold text-sm">{b.name}</div>
                <div className="text-xs text-slate-500 mt-0.5 flex items-center gap-1.5"><Calendar className="w-3 h-3" /> {b.date || '-'}</div>
              </div>
              <Badge className={`rounded-full capitalize ${statusColors[b.status] || statusColors.draft}`}>{b.status}</Badge>
              <div className="hidden md:block text-sm"><div className="text-xs text-slate-500">Audience</div><div className="font-bold">{(b.audience || 0).toLocaleString()}</div></div>
              <div className="hidden md:block text-sm"><div className="text-xs text-slate-500">Open</div><div className="font-bold">{b.openRate || '-'}</div></div>
              <div className="hidden md:block text-sm"><div className="text-xs text-slate-500">Clicks</div><div className="font-bold">{b.clickRate || '-'}</div></div>
              <div className="flex gap-2 sm:justify-end">
                {b.status === 'draft' && (
                  <Button size="sm" onClick={() => handleSend(b.id)} className="bg-slate-900 text-white rounded-xl text-xs h-8 px-3">
                    <Send className="w-3 h-3 mr-1" /> Send
                  </Button>
                )}
                {b.status === 'sending' && (
                  <Button size="sm" disabled className="rounded-xl text-xs h-8 px-3">
                    <Loader2 className="w-3 h-3 mr-1 animate-spin" /> Sending
                  </Button>
                )}
                <Button variant="ghost" size="icon" className="rounded-lg"><MoreVertical className="w-4 h-4" /></Button>
              </div>
            </div>
          ))}
          {list.length === 0 && <div className="p-12 text-center text-slate-500 text-sm">No broadcasts yet. Create your first one.</div>}
        </div>
      </Card>
    </div>
  );
};

export default Broadcasting;

import React, { useEffect, useState } from 'react';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Badge } from '../components/ui/badge';
import { Checkbox } from '../components/ui/checkbox';
import { Search, Plus, Download, Tag, MoreVertical, MessageSquare } from 'lucide-react';
import api from '../lib/api';
import { toast } from 'sonner';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter } from '../components/ui/dialog';
import { Label } from '../components/ui/label';

const tagColors = {
  Customer: 'bg-emerald-50 text-emerald-700 border-emerald-100',
  VIP: 'bg-purple-50 text-purple-700 border-purple-100',
  Lead: 'bg-blue-50 text-blue-700 border-blue-100',
  Prospect: 'bg-amber-50 text-amber-700 border-amber-100',
  Unsubscribed: 'bg-slate-100 text-slate-600 border-slate-200'
};

const Contacts = () => {
  const [search, setSearch] = useState('');
  const [selected, setSelected] = useState([]);
  const [list, setList] = useState([]);
  const [open, setOpen] = useState(false);
  const [form, setForm] = useState({ name: '', username: '', tags: '' });

  const load = async () => {
    try {
      const { data } = await api.get('/contacts', { params: { search } });
      setList(data);
    } catch (err) {
      console.error('[Contacts] Failed to load:', err);
      toast.error('Failed to load contacts');
    }
  };
  useEffect(() => { const t = setTimeout(load, 250); return () => clearTimeout(t); }, [search]); // eslint-disable-line react-hooks/exhaustive-deps

  const toggleAll = () => setSelected(selected.length === list.length ? [] : list.map(c => c.id));
  const toggleOne = (id) => setSelected(s => s.includes(id) ? s.filter(x => x !== id) : [...s, id]);

  const handleCreate = async () => {
    if (!form.name || !form.username) { toast.error('Name and username required'); return; }
    try {
      const { data } = await api.post('/contacts', {
        name: form.name, username: form.username.startsWith('@') ? form.username : '@' + form.username,
        tags: form.tags ? form.tags.split(',').map(t => t.trim()).filter(Boolean) : [],
      });
      setList(prev => [data, ...prev]);
      setOpen(false); setForm({ name: '', username: '', tags: '' });
      toast.success('Contact added');
    } catch { toast.error('Failed'); }
  };

  return (
    <div className="p-8 max-w-7xl mx-auto">
      <div className="flex items-end justify-between flex-wrap gap-4">
        <div>
          <h1 className="font-display text-3xl font-extrabold tracking-tight">Contacts</h1>
          <p className="mt-1 text-slate-600">{list.length} total contacts • {list.filter(c => c.subscribed).length} subscribed</p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" className="rounded-xl" onClick={() => toast.success('Export ready')}><Download className="w-4 h-4 mr-1.5" /> Export</Button>
          <Dialog open={open} onOpenChange={setOpen}>
            <DialogTrigger asChild>
              <Button className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl"><Plus className="w-4 h-4 mr-1.5" /> Add Contact</Button>
            </DialogTrigger>
            <DialogContent className="rounded-2xl">
              <DialogHeader><DialogTitle className="font-display text-2xl">Add Contact</DialogTitle></DialogHeader>
              <div className="space-y-4 pt-2">
                <div className="space-y-2"><Label>Name</Label><Input value={form.name} onChange={e => setForm({ ...form, name: e.target.value })} className="h-11 rounded-xl" /></div>
                <div className="space-y-2"><Label>Instagram username</Label><Input value={form.username} onChange={e => setForm({ ...form, username: e.target.value })} placeholder="@username" className="h-11 rounded-xl" /></div>
                <div className="space-y-2"><Label>Tags (comma separated)</Label><Input value={form.tags} onChange={e => setForm({ ...form, tags: e.target.value })} placeholder="Customer, VIP" className="h-11 rounded-xl" /></div>
              </div>
              <DialogFooter>
                <Button variant="outline" onClick={() => setOpen(false)} className="rounded-xl">Cancel</Button>
                <Button onClick={handleCreate} className="bg-slate-900 text-white rounded-xl">Add</Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        </div>
      </div>

      <div className="mt-6 flex flex-wrap gap-3 items-center">
        <div className="relative flex-1 min-w-[240px] max-w-sm">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
          <Input placeholder="Search by name or username..." value={search} onChange={e => setSearch(e.target.value)} className="pl-9 h-10 rounded-xl bg-white" />
        </div>
        {selected.length > 0 && (
          <div className="flex items-center gap-2 ml-auto">
            <span className="text-sm text-slate-600">{selected.length} selected</span>
            <Button variant="outline" size="sm" className="rounded-lg" onClick={() => toast.success('Tag added')}><Tag className="w-3.5 h-3.5 mr-1" /> Tag</Button>
            <Button variant="outline" size="sm" className="rounded-lg" onClick={() => toast.success('Broadcast queued')}><MessageSquare className="w-3.5 h-3.5 mr-1" /> Message</Button>
          </div>
        )}
      </div>

      <Card className="mt-6 rounded-2xl border-slate-100 overflow-hidden">
        <div className="grid grid-cols-[40px_1fr_200px_120px_40px] md:grid-cols-[40px_1fr_260px_140px_40px] items-center gap-4 px-5 py-3 border-b border-slate-100 bg-slate-50 text-xs font-semibold uppercase tracking-wider text-slate-500">
          <Checkbox checked={selected.length === list.length && list.length > 0} onCheckedChange={toggleAll} />
          <div>Contact</div><div className="hidden md:block">Tags</div><div>Last Active</div><div />
        </div>
        {list.map(c => (
          <div key={c.id} className="grid grid-cols-[40px_1fr_200px_120px_40px] md:grid-cols-[40px_1fr_260px_140px_40px] items-center gap-4 px-5 py-3 border-b border-slate-50 hover:bg-slate-50 transition-colors last:border-0">
            <Checkbox checked={selected.includes(c.id)} onCheckedChange={() => toggleOne(c.id)} />
            <div className="flex items-center gap-3 min-w-0">
              <img src={c.avatar} alt={c.name} className="w-9 h-9 rounded-full object-cover" />
              <div className="min-w-0">
                <div className="font-semibold text-sm truncate">{c.name}</div>
                <div className="text-xs text-slate-500 truncate">{c.username}</div>
              </div>
              {!c.subscribed && <Badge className="bg-slate-100 text-slate-600 border-0 rounded-full text-[10px]">Unsub</Badge>}
            </div>
            <div className="hidden md:flex gap-1.5 flex-wrap">
              {c.tags.map(t => (<Badge key={t} className={`rounded-full text-[11px] ${tagColors[t] || 'bg-slate-100 text-slate-700'}`}>{t}</Badge>))}
            </div>
            <div className="text-sm text-slate-600">recently</div>
            <Button variant="ghost" size="icon" className="rounded-lg"><MoreVertical className="w-4 h-4" /></Button>
          </div>
        ))}
        {list.length === 0 && (<div className="p-12 text-center text-slate-500 text-sm">No contacts match your search.</div>)}
      </Card>
    </div>
  );
};

export default Contacts;

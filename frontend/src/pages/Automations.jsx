import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import { Input } from '../components/ui/input';
import { Switch } from '../components/ui/switch';
import { Plus, Search, Zap, Copy, Trash2, Edit } from 'lucide-react';
import api from '../lib/api';
import { toast } from 'sonner';

const Automations = () => {
  const [list, setList] = useState([]);
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState('all');
  const [loading, setLoading] = useState(true);

  const refresh = async () => {
    setLoading(true);
    try {
      const { data } = await api.get('/automations');
      setList(data);
    } catch (err) {
      console.error('[Automations] Failed to load:', err);
      toast.error('Failed to load automations');
    }
    setLoading(false);
  };

  useEffect(() => { refresh(); }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const filtered = list.filter(a =>
    (filter === 'all' || a.status === filter) &&
    a.name.toLowerCase().includes(search.toLowerCase())
  );

  const toggleStatus = async (a) => {
    const newStatus = a.status === 'active' ? 'paused' : 'active';
    setList(prev => prev.map(x => x.id === a.id ? { ...x, status: newStatus } : x));
    try { await api.patch(`/automations/${a.id}`, { status: newStatus }); toast.success('Automation updated'); }
    catch { toast.error('Failed to update'); refresh(); }
  };

  const handleDelete = async (id) => {
    setList(prev => prev.filter(a => a.id !== id));
    try { await api.delete(`/automations/${id}`); toast.success('Deleted'); }
    catch { toast.error('Failed'); refresh(); }
  };

  const handleDuplicate = async (id) => {
    try {
      const { data } = await api.post(`/automations/${id}/duplicate`);
      setList(prev => [data, ...prev]);
      toast.success('Automation duplicated');
    } catch { toast.error('Failed'); }
  };

  const handleCreate = async () => {
    try {
      const { data } = await api.post('/automations', { name: 'Untitled Automation', trigger: 'Manual', status: 'draft' });
      setList(prev => [data, ...prev]);
      window.location.href = `/app/automations/${data.id}`;
    } catch { toast.error('Failed to create'); }
  };

  return (
    <div className="p-8 max-w-7xl mx-auto">
      <div className="flex items-end justify-between flex-wrap gap-4">
        <div>
          <h1 className="font-display text-3xl font-extrabold tracking-tight">Automations</h1>
          <p className="mt-1 text-slate-600">Build, manage, and optimize your Instagram flows.</p>
        </div>
        <Button onClick={handleCreate} className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl">
          <Plus className="w-4 h-4 mr-1.5" /> Create Automation
        </Button>
      </div>

      <div className="mt-6 flex flex-wrap gap-3 items-center">
        <div className="relative flex-1 min-w-[240px] max-w-sm">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
          <Input placeholder="Search automations..." value={search} onChange={e => setSearch(e.target.value)} className="pl-9 h-10 rounded-xl bg-white" />
        </div>
        <div className="flex gap-1 bg-white p-1 rounded-xl border border-slate-200">
          {['all', 'active', 'paused', 'draft'].map(f => (
            <button key={f} onClick={() => setFilter(f)} className={`px-4 py-1.5 text-sm font-medium rounded-lg capitalize transition-colors ${filter === f ? 'bg-slate-900 text-white' : 'text-slate-600 hover:bg-slate-100'}`}>{f}</button>
          ))}
        </div>
      </div>

      <div className="mt-6 grid gap-3">
        {filtered.map(a => (
          <Card key={a.id} className="p-5 rounded-2xl border-slate-100 hover:shadow-md transition-shadow">
            <div className="flex items-center gap-4 flex-wrap">
              <div className="w-11 h-11 rounded-xl bg-gradient-to-br from-blue-500 to-cyan-400 flex items-center justify-center shrink-0">
                <Zap className="w-5 h-5 text-white" />
              </div>
              <div className="flex-1 min-w-[200px]">
                <Link to={`/app/automations/${a.id}`} className="font-semibold hover:underline">{a.name}</Link>
                <div className="text-sm text-slate-500 mt-0.5">{a.trigger}</div>
              </div>
              <div className="hidden md:flex gap-6 text-sm">
                <div><div className="text-xs text-slate-500">Sent</div><div className="font-bold">{(a.sent || 0).toLocaleString()}</div></div>
                <div><div className="text-xs text-slate-500">Clicks</div><div className="font-bold">{(a.clicks || 0).toLocaleString()}</div></div>
              </div>
              <Badge className={`rounded-full ${a.status === 'active' ? 'bg-emerald-50 text-emerald-700 border-emerald-100' : a.status === 'paused' ? 'bg-amber-50 text-amber-700 border-amber-100' : 'bg-slate-100 text-slate-600 border-slate-200'}`}>{a.status}</Badge>
              <Switch checked={a.status === 'active'} onCheckedChange={() => toggleStatus(a)} />
              <div className="flex gap-1">
                <Link to={`/app/automations/${a.id}`}><Button variant="ghost" size="icon" className="rounded-lg"><Edit className="w-4 h-4" /></Button></Link>
                <Button onClick={() => handleDuplicate(a.id)} variant="ghost" size="icon" className="rounded-lg"><Copy className="w-4 h-4" /></Button>
                <Button onClick={() => handleDelete(a.id)} variant="ghost" size="icon" className="rounded-lg text-red-500 hover:text-red-600 hover:bg-red-50"><Trash2 className="w-4 h-4" /></Button>
              </div>
            </div>
          </Card>
        ))}
        {!loading && filtered.length === 0 && (
          <Card className="p-12 text-center rounded-2xl border-slate-100">
            <div className="w-14 h-14 mx-auto rounded-2xl bg-slate-100 flex items-center justify-center"><Zap className="w-6 h-6 text-slate-400" /></div>
            <h3 className="mt-4 font-display font-bold text-lg">No automations found</h3>
            <p className="text-sm text-slate-500 mt-1">Create your first automation to get started.</p>
          </Card>
        )}
      </div>
    </div>
  );
};

export default Automations;

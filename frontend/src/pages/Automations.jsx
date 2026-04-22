import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import { Input } from '../components/ui/input';
import { Switch } from '../components/ui/switch';
import { Plus, Search, Zap, Copy, Trash2, Edit, MessageCircle, X, Instagram, Loader2 } from 'lucide-react';
import api from '../lib/api';
import { toast } from 'sonner';

const Automations = () => {
  const [list, setList] = useState([]);
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState('all');
  const [loading, setLoading] = useState(true);
  const [ruleOpen, setRuleOpen] = useState(false);
  const [media, setMedia] = useState([]);
  const [mediaLoading, setMediaLoading] = useState(false);
  const [mediaError, setMediaError] = useState(null);
  const [selectedMedia, setSelectedMedia] = useState('latest'); // 'latest' or media id
  const [commentReply, setCommentReply] = useState('شكرا على تعليقك!');
  const [dmText, setDmText] = useState('شكرا');
  const [saving, setSaving] = useState(false);

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

  const openRuleDialog = async () => {
    setRuleOpen(true);
    setMediaError(null);
    setMediaLoading(true);
    try {
      const { data } = await api.get('/instagram/media');
      setMedia(data.items || []);
    } catch (e) {
      setMediaError(e?.response?.data?.detail || 'Failed to load posts. Connect Instagram first.');
      setMedia([]);
    }
    setMediaLoading(false);
  };

  const submitRule = async () => {
    if (!commentReply.trim() && !dmText.trim()) {
      toast.error('Add a comment reply or DM text');
      return;
    }
    setSaving(true);
    try {
      const body = {
        comment_reply: commentReply.trim(),
        dm_text: dmText.trim(),
      };
      if (selectedMedia === 'latest') body.latest = true;
      else body.media_id = selectedMedia;
      const { data } = await api.post('/automations/quick-comment-rule', body);
      setList(prev => [data, ...prev]);
      toast.success('Comment reply rule created');
      setRuleOpen(false);
    } catch (e) {
      toast.error(e?.response?.data?.detail || 'Failed to create rule');
    }
    setSaving(false);
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
        <div className="flex gap-2">
          <Button onClick={openRuleDialog} variant="outline" className="rounded-xl">
            <MessageCircle className="w-4 h-4 mr-1.5" /> Comment Reply Rule
          </Button>
          <Button onClick={handleCreate} className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl">
            <Plus className="w-4 h-4 mr-1.5" /> Create Automation
          </Button>
        </div>
      </div>

      {ruleOpen && (
        <div className="fixed inset-0 z-50 bg-black/50 flex items-center justify-center p-4" onClick={() => !saving && setRuleOpen(false)}>
          <Card className="w-full max-w-2xl max-h-[90vh] overflow-y-auto rounded-2xl p-6 bg-white" onClick={e => e.stopPropagation()}>
            <div className="flex items-start justify-between">
              <div>
                <h3 className="font-display font-bold text-lg">New Comment Reply Rule</h3>
                <p className="text-sm text-slate-500">Reply to comments on a specific post and DM the commenter.</p>
              </div>
              <Button variant="ghost" size="icon" onClick={() => !saving && setRuleOpen(false)} className="rounded-lg"><X className="w-4 h-4" /></Button>
            </div>

            <div className="mt-6">
              <div className="text-sm font-semibold mb-2">Choose post</div>
              {mediaError && <div className="p-3 rounded-xl bg-red-50 border border-red-100 text-sm text-red-700">{mediaError}</div>}
              {mediaLoading ? (
                <div className="flex items-center gap-2 text-sm text-slate-500 py-6 justify-center"><Loader2 className="w-4 h-4 animate-spin" /> Loading posts...</div>
              ) : (
                <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
                  <button type="button" onClick={() => setSelectedMedia('latest')} className={`p-3 rounded-xl border text-left ${selectedMedia === 'latest' ? 'border-slate-900 bg-slate-50' : 'border-slate-200 hover:border-slate-300'}`}>
                    <div className="w-full aspect-square rounded-lg bg-gradient-to-br from-pink-500 via-fuchsia-500 to-orange-400 flex items-center justify-center">
                      <Instagram className="w-8 h-8 text-white" />
                    </div>
                    <div className="mt-2 font-semibold text-sm">Latest post</div>
                    <div className="text-xs text-slate-500">Always track the newest one</div>
                  </button>
                  {media.map(m => (
                    <button key={m.id} type="button" onClick={() => setSelectedMedia(m.id)} className={`p-2 rounded-xl border text-left ${selectedMedia === m.id ? 'border-slate-900 bg-slate-50' : 'border-slate-200 hover:border-slate-300'}`}>
                      <div className="w-full aspect-square rounded-lg bg-slate-100 overflow-hidden">
                        {(m.thumbnail_url || m.media_url) ? (
                          <img src={m.thumbnail_url || m.media_url} alt="" className="w-full h-full object-cover" />
                        ) : (
                          <div className="w-full h-full flex items-center justify-center text-slate-400"><Instagram className="w-8 h-8" /></div>
                        )}
                      </div>
                      <div className="mt-1 text-xs text-slate-600 line-clamp-2">{m.caption || m.media_type}</div>
                    </button>
                  ))}
                </div>
              )}
            </div>

            <div className="mt-5 space-y-4">
              <div>
                <div className="text-sm font-semibold mb-2">Reply to the comment with</div>
                <Input value={commentReply} onChange={e => setCommentReply(e.target.value)} className="h-11 rounded-xl" placeholder="e.g. Thanks for your comment!" />
              </div>
              <div>
                <div className="text-sm font-semibold mb-2">Send a DM with</div>
                <Input value={dmText} onChange={e => setDmText(e.target.value)} className="h-11 rounded-xl" placeholder="شكرا" />
              </div>
            </div>

            <div className="mt-6 flex justify-end gap-2">
              <Button variant="outline" onClick={() => !saving && setRuleOpen(false)} className="rounded-xl">Cancel</Button>
              <Button onClick={submitRule} disabled={saving} className="bg-slate-900 text-white rounded-xl">
                {saving && <Loader2 className="w-4 h-4 mr-2 animate-spin" />}
                Create rule
              </Button>
            </div>
          </Card>
        </div>
      )}

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

import React, { useEffect, useState } from 'react';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import { Input } from '../components/ui/input';
import { Switch } from '../components/ui/switch';
import { Checkbox } from '../components/ui/checkbox';
import {
  Plus, Search, Zap, Trash2, X, Instagram, Loader2, ArrowLeft, ArrowRight,
  MessageCircle, Send as SendIcon, Filter, Hash,
} from 'lucide-react';
import api from '../lib/api';
import { toast } from 'sonner';

const stepLabels = ['Post', 'Rule', 'Match', 'Message'];

const Automations = () => {
  const [list, setList] = useState([]);
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState('all');
  const [loading, setLoading] = useState(true);

  // Wizard state
  const [wizardOpen, setWizardOpen] = useState(false);
  const [step, setStep] = useState(0);
  const [media, setMedia] = useState([]);
  const [mediaLoading, setMediaLoading] = useState(false);
  const [mediaError, setMediaError] = useState(null);
  const [mediaWarning, setMediaWarning] = useState(null);
  const [selectedMedia, setSelectedMedia] = useState(null); // {id, thumbnail_url, caption, media_type, latest?}
  const [mode, setMode] = useState('reply_and_dm'); // reply_and_dm | reply_only
  const [match, setMatch] = useState('any'); // any | keyword
  const [keyword, setKeyword] = useState('');
  const [commentReply, setCommentReply] = useState('شكرا على تعليقك!');
  const [dmText, setDmText] = useState('شكرا');
  const [processExistingComments, setProcessExistingComments] = useState(false);
  const [saving, setSaving] = useState(false);

  const refresh = async () => {
    setLoading(true);
    try {
      const { data } = await api.get('/automations');
      setList(data);
    } catch {
      toast.error('Failed to load automations');
    }
    setLoading(false);
  };

  useEffect(() => { refresh(); }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const filtered = list.filter(a =>
    (filter === 'all' || a.status === filter) &&
    (a.name || '').toLowerCase().includes(search.toLowerCase())
  );

  const toggleStatus = async (a) => {
    const newStatus = a.status === 'active' ? 'paused' : 'active';
    setList(prev => prev.map(x => x.id === a.id ? { ...x, status: newStatus } : x));
    try { await api.patch(`/automations/${a.id}`, { status: newStatus }); }
    catch { toast.error('Failed to update'); refresh(); }
  };

  const handleDelete = async (id) => {
    setList(prev => prev.filter(a => a.id !== id));
    try { await api.delete(`/automations/${id}`); toast.success('Deleted'); }
    catch { toast.error('Failed'); refresh(); }
  };

  const resetWizard = () => {
    setStep(0);
    setSelectedMedia(null);
    setMode('reply_and_dm');
    setMatch('any');
    setKeyword('');
    setProcessExistingComments(false);
    setCommentReply('شكرا على تعليقك!');
    setDmText('شكرا');
  };
  const openWizard = async () => {
    resetWizard();
    setWizardOpen(true);
    setMedia([]);
    setMediaError(null);
    setMediaWarning(null);
    setMediaLoading(true);
    try {
      const { data } = await api.get('/instagram/media');
      const items = data?.media || data?.items || [];
      if (data?.ok === false) {
        setMedia([]);
        const errBody = data?.error?.body;
        setMediaError(typeof errBody === 'string' ? errBody : JSON.stringify(data?.error || data));
      } else {
        setMedia(items);
        if (items.length === 0) {
          setMediaWarning(data?.warning || 'No Instagram media returned. Make sure the connected account has published posts.');
        } else if (data?.warning) {
          setMediaWarning(data.warning);
        }
      }
    } catch (e) {
      setMediaError(e?.response?.data?.detail || e?.message || 'Failed to load posts. Connect Instagram first.');
      setMedia([]);
    }
    setMediaLoading(false);
  };

  const canNext = () => {
    if (step === 0) return !!selectedMedia;
    if (step === 1) return !!mode;
    if (step === 2) return match === 'any' || (match === 'keyword' && keyword.trim().length > 0);
    if (step === 3) {
      if (!commentReply.trim()) return false;
      if (mode === 'reply_and_dm' && !dmText.trim()) return false;
      return true;
    }
    return false;
  };

  const submit = async () => {
    setSaving(true);
    try {
      const body = {
        mode,
        match,
        keyword: match === 'keyword' ? keyword.trim() : '',
        comment_reply: commentReply.trim(),
        dm_text: mode === 'reply_and_dm' ? dmText.trim() : '',
        processExistingComments,
      };
      if (selectedMedia?.latest) body.latest = true;
      else {
        body.media_id = selectedMedia.id;
        body.media_preview = {
          caption: selectedMedia.caption || '',
          thumbnail_url: selectedMedia.thumbnail_url || selectedMedia.media_url || '',
          media_type: selectedMedia.media_type || '',
        };
      }
      const { data } = await api.post('/automations/quick-comment-rule', body);
      setList(prev => [data, ...prev]);
      toast.success('Automation created');
      setWizardOpen(false);
    } catch (e) {
      toast.error(e?.response?.data?.detail || 'Failed to create');
    }
    setSaving(false);
  };

  return (
    <div className="p-8 max-w-6xl mx-auto">
      <div className="flex items-end justify-between flex-wrap gap-4">
        <div>
          <h1 className="font-display text-3xl font-extrabold tracking-tight">Automations</h1>
          <p className="mt-1 text-slate-600">Pick a post, choose a rule, done.</p>
        </div>
        <Button onClick={openWizard} className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl">
          <Plus className="w-4 h-4 mr-1.5" /> Create Automation
        </Button>
      </div>

      {wizardOpen && (
        <div className="fixed inset-0 z-50 bg-black/50 flex items-center justify-center p-4"
             onClick={() => !saving && setWizardOpen(false)}>
          <Card className="w-full max-w-3xl max-h-[92vh] overflow-hidden rounded-2xl bg-white flex flex-col"
                onClick={e => e.stopPropagation()}>
            <div className="flex items-start justify-between p-6 pb-4 border-b border-slate-100">
              <div>
                <h3 className="font-display font-bold text-lg">Create Automation</h3>
                <p className="text-sm text-slate-500">Step {step + 1} of {stepLabels.length} — {stepLabels[step]}</p>
              </div>
              <Button variant="ghost" size="icon" onClick={() => !saving && setWizardOpen(false)} className="rounded-lg">
                <X className="w-4 h-4" />
              </Button>
            </div>

            {/* Progress */}
            <div className="px-6 pt-4">
              <div className="flex items-center gap-2">
                {stepLabels.map((l, i) => (
                  <div key={l} className="flex-1 h-1.5 rounded-full overflow-hidden bg-slate-100">
                    <div className={`h-full transition-all ${i <= step ? 'bg-slate-900' : 'bg-transparent'}`}
                         style={{ width: i <= step ? '100%' : '0%' }} />
                  </div>
                ))}
              </div>
            </div>

            <div className="p-6 overflow-y-auto flex-1">
              {step === 0 && (
                <div>
                  <h4 className="font-semibold">Choose the post</h4>
                  <p className="text-sm text-slate-500 mt-1">Pick one of your Instagram posts, or let the automation always track the newest one.</p>
                  {mediaError && <div className="mt-3 p-3 rounded-xl bg-red-50 border border-red-100 text-sm text-red-700 break-all">Graph API error: {mediaError}</div>}
                  {!mediaError && mediaWarning && <div className="mt-3 p-3 rounded-xl bg-amber-50 border border-amber-100 text-sm text-amber-800">{mediaWarning}</div>}
                  {mediaLoading ? (
                    <div className="flex items-center gap-2 text-sm text-slate-500 py-10 justify-center">
                      <Loader2 className="w-4 h-4 animate-spin" /> Loading posts...
                    </div>
                  ) : (
                    <div className="mt-4 grid grid-cols-2 sm:grid-cols-3 gap-3">
                      <button type="button"
                              onClick={() => setSelectedMedia({ id: 'latest', latest: true, caption: 'Always the latest post' })}
                              className={`p-3 rounded-xl border text-left transition ${selectedMedia?.latest ? 'border-slate-900 bg-slate-50 ring-2 ring-slate-900/10' : 'border-slate-200 hover:border-slate-300'}`}>
                        <div className="w-full aspect-square rounded-lg bg-gradient-to-br from-pink-500 via-fuchsia-500 to-orange-400 flex items-center justify-center">
                          <Instagram className="w-8 h-8 text-white" />
                        </div>
                        <div className="mt-2 font-semibold text-sm">Latest post</div>
                        <div className="text-xs text-slate-500 line-clamp-1">Always tracks the newest</div>
                      </button>
                      {media.map(m => {
                        const selected = selectedMedia?.id === m.id && !selectedMedia?.latest;
                        const thumb = m.thumbnail_url || m.media_url;
                        return (
                          <button key={m.id} type="button"
                                  onClick={() => setSelectedMedia(m)}
                                  className={`p-2 rounded-xl border text-left transition ${selected ? 'border-slate-900 bg-slate-50 ring-2 ring-slate-900/10' : 'border-slate-200 hover:border-slate-300'}`}>
                            <div className="w-full aspect-square rounded-lg bg-slate-100 overflow-hidden">
                              {thumb ? (
                                <img src={thumb} alt="" className="w-full h-full object-cover" />
                              ) : (
                                <div className="w-full h-full flex items-center justify-center text-slate-400"><Instagram className="w-8 h-8" /></div>
                              )}
                            </div>
                            <div className="mt-1 text-xs text-slate-600 line-clamp-2 min-h-[2.25rem]">
                              {m.caption || m.media_type}
                            </div>
                          </button>
                        );
                      })}
                    </div>
                  )}
                </div>
              )}

              {step === 1 && (
                <div>
                  <h4 className="font-semibold">What should happen?</h4>
                  <p className="text-sm text-slate-500 mt-1">Choose how this automation reacts to comments.</p>
                  <div className="mt-4 grid sm:grid-cols-2 gap-3">
                    {[
                      { v: 'reply_and_dm', title: 'Reply + DM', desc: 'Publicly reply to the comment and send a private DM to the commenter.', Icon: SendIcon },
                      { v: 'reply_only', title: 'Reply only', desc: 'Just publicly reply to the comment.', Icon: MessageCircle },
                    ].map(o => {
                      const active = mode === o.v;
                      return (
                        <button key={o.v} type="button" onClick={() => setMode(o.v)}
                                className={`p-4 rounded-xl border text-left transition ${active ? 'border-slate-900 bg-slate-50 ring-2 ring-slate-900/10' : 'border-slate-200 hover:border-slate-300'}`}>
                          <div className="w-10 h-10 rounded-xl bg-slate-900 text-white flex items-center justify-center">
                            <o.Icon className="w-5 h-5" />
                          </div>
                          <div className="mt-3 font-semibold">{o.title}</div>
                          <div className="text-xs text-slate-500 mt-1">{o.desc}</div>
                        </button>
                      );
                    })}
                  </div>
                </div>
              )}

              {step === 2 && (
                <div>
                  <h4 className="font-semibold">Which comments should trigger it?</h4>
                  <p className="text-sm text-slate-500 mt-1">Run on every comment, or only when a specific word is used.</p>
                  <div className="mt-4 grid sm:grid-cols-2 gap-3">
                    {[
                      { v: 'any', title: 'Any comment', desc: 'Fires for every new comment.', Icon: Filter },
                      { v: 'keyword', title: 'Specific keyword', desc: 'Only when the comment contains your word.', Icon: Hash },
                    ].map(o => {
                      const active = match === o.v;
                      return (
                        <button key={o.v} type="button" onClick={() => setMatch(o.v)}
                                className={`p-4 rounded-xl border text-left transition ${active ? 'border-slate-900 bg-slate-50 ring-2 ring-slate-900/10' : 'border-slate-200 hover:border-slate-300'}`}>
                          <div className="w-10 h-10 rounded-xl bg-slate-900 text-white flex items-center justify-center">
                            <o.Icon className="w-5 h-5" />
                          </div>
                          <div className="mt-3 font-semibold">{o.title}</div>
                          <div className="text-xs text-slate-500 mt-1">{o.desc}</div>
                        </button>
                      );
                    })}
                  </div>
                  {match === 'keyword' && (
                    <div className="mt-5">
                      <div className="text-sm font-semibold mb-2">Keyword</div>
                      <Input value={keyword} onChange={e => setKeyword(e.target.value)}
                             placeholder="e.g. price, LAUNCH, سعر"
                             className="h-11 rounded-xl" />
                      <p className="text-xs text-slate-500 mt-1">Case-insensitive, matched as a substring.</p>
                    </div>
                  )}
                </div>
              )}

              {step === 3 && (
                <div className="space-y-5">
                  <div>
                    <h4 className="font-semibold">Messages</h4>
                    <p className="text-sm text-slate-500 mt-1">Fill in the replies this automation will send.</p>
                  </div>
                  <div>
                    <div className="text-sm font-semibold mb-2">Public reply to the comment</div>
                    <Input value={commentReply} onChange={e => setCommentReply(e.target.value)}
                           className="h-11 rounded-xl" placeholder="e.g. Thanks for your comment!" />
                  </div>
                  {mode === 'reply_and_dm' && (
                    <div>
                      <div className="text-sm font-semibold mb-2">Private DM to the commenter</div>
                      <Input value={dmText} onChange={e => setDmText(e.target.value)}
                             className="h-11 rounded-xl" placeholder="شكرا" />
                    </div>
                  )}

                  <div className="rounded-xl border border-slate-200 p-4">
                    <label className="flex items-start gap-3">
                      <Checkbox
                        checked={processExistingComments}
                        onCheckedChange={v => setProcessExistingComments(Boolean(v))}
                        className="mt-0.5"
                      />
                      <span>
                        <span className="block text-sm font-semibold">Also process existing comments</span>
                        <span className="block text-xs text-slate-500 mt-1">
                          Leave unchecked to only respond to comments created after this rule is active.
                        </span>
                      </span>
                    </label>
                    {processExistingComments && (
                      <div className="mt-3 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                        This will send replies/DMs to comments that already exist on this post.
                      </div>
                    )}
                  </div>

                  {/* Summary */}
                  <div className="mt-6 p-4 rounded-xl bg-slate-50 border border-slate-100 text-sm space-y-1.5">
                    <div><span className="text-slate-500">Post:</span> <span className="font-medium">{selectedMedia?.latest ? 'Latest post' : (selectedMedia?.caption?.slice(0, 40) || selectedMedia?.media_type || selectedMedia?.id)}</span></div>
                    <div><span className="text-slate-500">Action:</span> <span className="font-medium">{mode === 'reply_and_dm' ? 'Reply + DM' : 'Reply only'}</span></div>
                    <div><span className="text-slate-500">Trigger:</span> <span className="font-medium">{match === 'keyword' ? `Keyword "${keyword}"` : 'Any comment'}</span></div>
                  </div>
                </div>
              )}
            </div>

            <div className="p-6 pt-4 border-t border-slate-100 flex items-center justify-between gap-2">
              <Button variant="outline" disabled={step === 0 || saving}
                      onClick={() => setStep(s => Math.max(0, s - 1))} className="rounded-xl">
                <ArrowLeft className="w-4 h-4 mr-1.5" /> Back
              </Button>
              {step < stepLabels.length - 1 ? (
                <Button disabled={!canNext()} onClick={() => setStep(s => s + 1)}
                        className="bg-slate-900 text-white rounded-xl">
                  Next <ArrowRight className="w-4 h-4 ml-1.5" />
                </Button>
              ) : (
                <Button disabled={!canNext() || saving} onClick={submit}
                        className="bg-slate-900 text-white rounded-xl">
                  {saving && <Loader2 className="w-4 h-4 mr-2 animate-spin" />}
                  Create Automation
                </Button>
              )}
            </div>
          </Card>
        </div>
      )}

      <div className="mt-6 flex flex-wrap gap-3 items-center">
        <div className="relative flex-1 min-w-[240px] max-w-sm">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
          <Input placeholder="Search automations..." value={search} onChange={e => setSearch(e.target.value)}
                 className="pl-9 h-10 rounded-xl bg-white" />
        </div>
        <div className="flex gap-1 bg-white p-1 rounded-xl border border-slate-200">
          {['all', 'active', 'paused', 'draft'].map(f => (
            <button key={f} onClick={() => setFilter(f)}
                    className={`px-4 py-1.5 text-sm font-medium rounded-lg capitalize transition-colors ${filter === f ? 'bg-slate-900 text-white' : 'text-slate-600 hover:bg-slate-100'}`}>
              {f}
            </button>
          ))}
        </div>
      </div>

      <div className="mt-6 grid gap-3">
        {filtered.map(a => {
          const thumb = a.media_preview?.thumbnail_url;
          const label = a.latest
            ? 'Latest post'
            : (a.media_preview?.caption?.slice(0, 40) || a.media_id?.slice(0, 10) || '');
          const matchLabel = a.match === 'keyword' && a.keyword ? `keyword "${a.keyword}"` : 'any comment';
          const modeLabel = a.mode === 'reply_only' ? 'Reply only' : 'Reply + DM';
          return (
            <Card key={a.id} className="p-4 rounded-2xl border-slate-100 hover:shadow-md transition-shadow">
              <div className="flex items-center gap-4 flex-wrap">
                <div className="w-14 h-14 rounded-xl overflow-hidden shrink-0 bg-gradient-to-br from-pink-500 via-fuchsia-500 to-orange-400 flex items-center justify-center">
                  {thumb ? <img src={thumb} alt="" className="w-full h-full object-cover" /> : <Zap className="w-6 h-6 text-white" />}
                </div>
                <div className="flex-1 min-w-[200px]">
                  <div className="font-semibold">{a.name}</div>
                  <div className="text-xs text-slate-500 mt-0.5">
                    {label} • {modeLabel} • {matchLabel}
                  </div>
                </div>
                <div className="hidden md:flex gap-6 text-sm">
                  <div><div className="text-xs text-slate-500">Fired</div><div className="font-bold">{(a.sent || 0).toLocaleString()}</div></div>
                </div>
                <Badge className={`rounded-full ${a.status === 'active' ? 'bg-emerald-50 text-emerald-700 border-emerald-100' : a.status === 'paused' ? 'bg-amber-50 text-amber-700 border-amber-100' : 'bg-slate-100 text-slate-600 border-slate-200'}`}>
                  {a.status}
                </Badge>
                <Switch checked={a.status === 'active'} onCheckedChange={() => toggleStatus(a)} />
                <Button onClick={() => handleDelete(a.id)} variant="ghost" size="icon"
                        className="rounded-lg text-red-500 hover:text-red-600 hover:bg-red-50">
                  <Trash2 className="w-4 h-4" />
                </Button>
              </div>
            </Card>
          );
        })}
        {!loading && filtered.length === 0 && (
          <Card className="p-12 text-center rounded-2xl border-slate-100">
            <div className="w-14 h-14 mx-auto rounded-2xl bg-slate-100 flex items-center justify-center">
              <Zap className="w-6 h-6 text-slate-400" />
            </div>
            <h3 className="mt-4 font-display font-bold text-lg">No automations yet</h3>
            <p className="text-sm text-slate-500 mt-1">Click "Create Automation" to build your first rule.</p>
          </Card>
        )}
      </div>
    </div>
  );
};

export default Automations;

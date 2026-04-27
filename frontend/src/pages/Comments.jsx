import React, { useEffect, useState, useCallback, useRef } from 'react';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Badge } from '../components/ui/badge';
import { AtSign, RefreshCw, Send, Wifi, WifiOff, CheckCircle2 } from 'lucide-react';
import api, { API_BASE } from '../lib/api';
import { toast } from 'sonner';

const WS_URL = API_BASE.replace(/^http/, 'ws').replace('/api', '');

const Comments = () => {
  const [comments, setComments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [replyText, setReplyText] = useState({});
  const [sending, setSending] = useState({});
  const [wsReady, setWsReady] = useState(false);
  const wsRef = useRef(null);
  const reconnectTimer = useRef(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const { data } = await api.get('/comments');
      setComments(data);
    } catch (err) {
      console.error('[Comments] load failed', err);
      toast.error('Failed to load comments');
    } finally {
      setLoading(false);
    }
  }, []);

  const connectWs = useCallback(() => {
    const token = localStorage.getItem('mychat_token');
    const user = JSON.parse(localStorage.getItem('mychat_user') || '{}');
    if (!token || !user.id) return;
    const ws = new WebSocket(`${WS_URL}/ws/${user.id}?token=${token}`);
    wsRef.current = ws;
    ws.onopen = () => setWsReady(true);
    ws.onclose = () => {
      setWsReady(false);
      reconnectTimer.current = setTimeout(connectWs, 3000);
    };
    ws.onerror = () => ws.close();
    ws.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data);
        if (data.type === 'comment' && data.comment) {
          setComments(prev => [data.comment, ...prev]);
          toast.info(`New comment from @${data.comment.commenter_username}`);
        }
      } catch (_) {}
    };
  }, []);

  useEffect(() => {
    load();
    connectWs();
    return () => {
      clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [load, connectWs]);

  const handleReply = async (comment) => {
    const text = (replyText[comment.id] || '').trim();
    if (!text) return;
    setSending(prev => ({ ...prev, [comment.id]: true }));
    try {
      await api.post(`/comments/${comment.id}/reply`, { text });
      toast.success('Reply sent to Instagram');
      setReplyText(prev => ({ ...prev, [comment.id]: '' }));
      setComments(prev => prev.map(c => c.id === comment.id
        ? { ...c, replied: true, reply_text: text } : c));
    } catch (err) {
      console.error('[Comments] reply failed', err);
      const msg = err?.response?.data?.detail || 'Reply failed';
      toast.error(typeof msg === 'string' ? msg : JSON.stringify(msg));
    } finally {
      setSending(prev => ({ ...prev, [comment.id]: false }));
    }
  };

  return (
    <div className="p-4 sm:p-6 max-w-4xl mx-auto">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between mb-6">
        <div>
          <h1 className="text-3xl font-bold font-display">Comments</h1>
          <p className="text-slate-500 mt-1">Instagram comments received via webhook</p>
        </div>
        <div className="flex items-center justify-between gap-3 sm:justify-end">
          {wsReady
            ? <span className="flex items-center gap-1 text-xs text-emerald-600"><Wifi className="w-3 h-3" /> Live</span>
            : <span className="flex items-center gap-1 text-xs text-slate-400"><WifiOff className="w-3 h-3" /> Offline</span>}
          <Button variant="outline" size="sm" onClick={load}>
            <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} /> Refresh
          </Button>
        </div>
      </div>

      {loading && comments.length === 0 && (
        <div className="text-center py-20 text-slate-500">Loading…</div>
      )}

      {!loading && comments.length === 0 && (
        <div className="text-center py-20 bg-white rounded-2xl border border-slate-100">
          <AtSign className="w-12 h-12 text-slate-300 mx-auto mb-4" />
          <h3 className="text-lg font-semibold">No comments yet</h3>
          <p className="text-sm text-slate-500 mt-1">
            Comments on your Instagram posts will appear here in real time.
          </p>
          <p className="text-xs text-slate-400 mt-3">
            Make sure the webhook is subscribed in Meta → Instagram → Webhooks.
          </p>
        </div>
      )}

      <div className="space-y-4">
        {comments.map(c => (
          <div key={c.id} className="bg-white rounded-2xl border border-slate-100 p-5">
            <div className="flex items-start justify-between gap-3">
              <div className="flex-1">
                <div className="flex flex-wrap items-center gap-2">
                  <div className="font-semibold">@{c.commenter_username}</div>
                  {c.replied && (
                    <Badge className="bg-emerald-100 text-emerald-700 border-0">
                      <CheckCircle2 className="w-3 h-3 mr-1" /> Replied
                    </Badge>
                  )}
                </div>
                <div className="text-slate-700 mt-1">{c.text}</div>
                <div className="text-xs text-slate-400 mt-1">
                  {c.created && new Date(c.created).toLocaleString()}
                  {c.media_id && <> • on media <span className="font-mono">{c.media_id}</span></>}
                </div>
              </div>
            </div>

            {c.replied && c.reply_text && (
              <div className="mt-3 pl-4 border-l-2 border-emerald-200 text-sm">
                <div className="text-xs text-emerald-700 font-semibold">Your reply</div>
                <div className="text-slate-700">{c.reply_text}</div>
              </div>
            )}

            {!c.replied && (
              <form
                className="mt-3 flex flex-col gap-2 sm:flex-row"
                onSubmit={(e) => { e.preventDefault(); handleReply(c); }}
              >
                <Input
                  placeholder="Reply to this comment…"
                  value={replyText[c.id] || ''}
                  onChange={(e) => setReplyText(prev => ({ ...prev, [c.id]: e.target.value }))}
                  disabled={!!sending[c.id]}
                />
                <Button type="submit" className="sm:w-auto" disabled={!!sending[c.id] || !(replyText[c.id] || '').trim()}>
                  <Send className="w-4 h-4 mr-2" />
                  {sending[c.id] ? 'Sending…' : 'Reply'}
                </Button>
              </form>
            )}
          </div>
        ))}
      </div>
    </div>
  );
};

export default Comments;

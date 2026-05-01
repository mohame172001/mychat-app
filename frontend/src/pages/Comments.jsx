import React, { useEffect, useState, useCallback, useRef, useMemo } from 'react';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Badge } from '../components/ui/badge';
import { AtSign, RefreshCw, Send, Wifi, WifiOff, CheckCircle2, Filter, AlertTriangle, Clock, Repeat, ShieldX } from 'lucide-react';
import api, { API_BASE } from '../lib/api';
import { toast } from 'sonner';

const WS_URL = API_BASE.replace(/^http/, 'ws').replace('/api', '');

// Permanent DM/reply failures: do not show a Retry button; doing so would
// hit the same Graph error or, worse, duplicate a successful public reply.
const PERMANENT_FAILURE_REASONS = new Set([
  'recipient_unavailable',
  'messaging_not_allowed',
  'user_blocked_messages',
  'permission_error',
]);

const STATUS_FILTERS = [
  { id: 'all',              label: 'All' },
  { id: 'success',          label: 'Success' },
  { id: 'partial_success',  label: 'Partial (DM failed)' },
  { id: 'pending',          label: 'Pending / queued' },
  { id: 'retryable_failed', label: 'Retryable failed' },
  { id: 'permanent_failed', label: 'Permanent failed' },
  { id: 'skipped',          label: 'Skipped' },
];

function classifyComment(c) {
  // Returns one of the STATUS_FILTERS ids.
  const action = String(c.action_status || c.actionStatus || '').toLowerCase();
  const reply = String(c.reply_status || '').toLowerCase();
  const dm = String(c.dm_status || '').toLowerCase();
  const dmReason = c.dm_failure_reason;
  const replyReason = c.reply_failure_reason;
  if (action === 'success' || reply === 'success' && dm !== 'failed') return 'success';
  if (action === 'partial_success' || (reply === 'success' && dm === 'failed')) return 'partial_success';
  if (action === 'pending') return 'pending';
  if (action === 'failed') {
    const permanent = (
      PERMANENT_FAILURE_REASONS.has(dmReason)
      || PERMANENT_FAILURE_REASONS.has(replyReason)
    );
    return permanent ? 'permanent_failed' : 'retryable_failed';
  }
  if (action === 'skipped') return 'skipped';
  return 'pending';
}

function statusBadge(c) {
  const cls = classifyComment(c);
  switch (cls) {
    case 'success':
      return { label: 'Replied', cn: 'bg-emerald-100 text-emerald-700', Icon: CheckCircle2 };
    case 'partial_success':
      return { label: 'Reply OK • DM failed', cn: 'bg-amber-100 text-amber-800', Icon: AlertTriangle };
    case 'pending':
      return { label: 'Queued', cn: 'bg-blue-100 text-blue-700', Icon: Clock };
    case 'retryable_failed':
      return { label: 'Retryable failed', cn: 'bg-orange-100 text-orange-700', Icon: Repeat };
    case 'permanent_failed':
      return { label: 'Permanent failed', cn: 'bg-rose-100 text-rose-700', Icon: ShieldX };
    case 'skipped':
      return { label: 'Skipped', cn: 'bg-slate-100 text-slate-600', Icon: Filter };
    default:
      return { label: '—', cn: 'bg-slate-100 text-slate-600', Icon: Filter };
  }
}

function canRetryReply(c) {
  // Only safe when no provider-proven public reply exists AND the reply
  // step is in a state we can re-try (failed transiently or pending).
  if (c.replied === true) return false;
  if (String(c.reply_status || '').toLowerCase() === 'success') return false;
  if (PERMANENT_FAILURE_REASONS.has(c.reply_failure_reason)) return false;
  return true;
}

const Comments = () => {
  const [comments, setComments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [replyText, setReplyText] = useState({});
  const [sending, setSending] = useState({});
  const [wsReady, setWsReady] = useState(false);
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(false);
  const [unrepliedOnly, setUnrepliedOnly] = useState(true); // Default to unreplied
  const [statusFilter, setStatusFilter] = useState('all');

  const wsRef = useRef(null);
  const reconnectTimer = useRef(null);
  const wsAttempts = useRef(0);
  const wsGaveUp = useRef(false);

  const fetchComments = useCallback(async (pageToFetch, isReset) => {
    if (isReset) setLoading(true);
    else setLoadingMore(true);

    try {
      const { data } = await api.get('/comments', {
        params: { page: pageToFetch, limit: 30, unreplied: unrepliedOnly }
      });
      
      // Fallback for older backend cache just in case
      const incomingComments = Array.isArray(data) ? data : data.comments;
      const incomingHasMore = data.has_more ?? false;

      setComments(prev => isReset ? incomingComments : [...prev, ...incomingComments]);
      setHasMore(incomingHasMore);
      setPage(pageToFetch);
    } catch (err) {
      console.error('[Comments] load failed', err);
      toast.error('Failed to load comments');
    } finally {
      setLoading(false);
      setLoadingMore(false);
    }
  }, [unrepliedOnly]);

  const connectWs = useCallback(() => {
    if (wsGaveUp.current) return;
    const token = localStorage.getItem('mychat_token');
    const user = JSON.parse(localStorage.getItem('mychat_user') || '{}');
    if (!token || !user.id) return;
    let ws;
    try {
      ws = new WebSocket(`${WS_URL}/ws/${user.id}?token=${token}`);
    } catch (_e) {
      wsGaveUp.current = true;
      return;
    }
    wsRef.current = ws;
    ws.onopen = () => {
      wsAttempts.current = 0;
      setWsReady(true);
    };
    ws.onclose = () => {
      setWsReady(false);
      wsAttempts.current += 1;
      // Exponential backoff capped at 60s. After 6 failed attempts (~2 min)
      // we stop reconnecting to avoid spamming /ws 404s in backend logs
      // when the route is unreachable behind the current proxy. The user
      // can refresh the page to retry.
      if (wsAttempts.current >= 6) {
        wsGaveUp.current = true;
        return;
      }
      const delayMs = Math.min(60000, 3000 * Math.pow(2, wsAttempts.current - 1));
      reconnectTimer.current = setTimeout(connectWs, delayMs);
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
    fetchComments(1, true);
  }, [fetchComments]);

  // Apply the status filter on the client. Backend pagination still drives
  // what's loaded; this is a UI-only refinement so support can quickly
  // narrow the page to "partial success" / "permanent failed" etc.
  const filteredComments = useMemo(() => {
    if (statusFilter === 'all') return comments;
    return comments.filter(c => classifyComment(c) === statusFilter);
  }, [comments, statusFilter]);

  useEffect(() => {
    connectWs();
    return () => {
      clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [connectWs]);

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
        <div className="flex flex-wrap items-center gap-3 sm:justify-end">
          {wsReady
            ? <span className="flex items-center gap-1 text-xs text-emerald-600"><Wifi className="w-3 h-3" /> Live</span>
            : <span className="flex items-center gap-1 text-xs text-slate-400"><WifiOff className="w-3 h-3" /> Offline</span>}
          
          <Button 
            variant={unrepliedOnly ? "default" : "outline"} 
            size="sm" 
            onClick={() => setUnrepliedOnly(!unrepliedOnly)}
            className="rounded-full"
          >
            <Filter className="w-4 h-4 mr-2" /> 
            {unrepliedOnly ? 'Unreplied Only' : 'All Comments'}
          </Button>

          <Button variant="outline" size="sm" onClick={() => fetchComments(1, true)}>
            <RefreshCw className={`w-4 h-4 mr-2 ${loading ? 'animate-spin' : ''}`} /> Refresh
          </Button>
        </div>
      </div>

      {/* Status filter bar — operations visibility per Phase 1 spec.
          Filters work client-side over the current page, so the data model
          remains the existing /comments endpoint without new params. */}
      <div className="mb-4 flex flex-wrap gap-2">
        {STATUS_FILTERS.map(f => (
          <button
            key={f.id}
            type="button"
            onClick={() => setStatusFilter(f.id)}
            className={
              'rounded-full border px-3 py-1 text-xs font-medium transition '
              + (statusFilter === f.id
                ? 'border-blue-500 bg-blue-50 text-blue-700'
                : 'border-slate-200 bg-white text-slate-600 hover:bg-slate-50')
            }
          >
            {f.label}
          </button>
        ))}
      </div>

      {loading && comments.length === 0 && (
        <div className="text-center py-20 text-slate-500">Loading…</div>
      )}

      {!loading && comments.length === 0 && (
        <div className="text-center py-20 bg-white rounded-2xl border border-slate-100">
          <AtSign className="w-12 h-12 text-slate-300 mx-auto mb-4" />
          <h3 className="text-lg font-semibold">{unrepliedOnly ? 'No unreplied comments' : 'No comments yet'}</h3>
          <p className="text-sm text-slate-500 mt-1">
            {unrepliedOnly ? "You're all caught up! Great job." : "Comments on your Instagram posts will appear here in real time."}
          </p>
          {!unrepliedOnly && (
            <p className="text-xs text-slate-400 mt-3">
              Make sure the webhook is subscribed in Meta → Instagram → Webhooks.
            </p>
          )}
        </div>
      )}

      <div className="space-y-4">
        {filteredComments.map(c => {
          const badge = statusBadge(c);
          const BadgeIcon = badge.Icon;
          return (
          <div key={c.id} className="bg-white rounded-2xl border border-slate-100 p-5">
            <div className="flex items-start justify-between gap-3">
              <div className="flex-1">
                <div className="flex flex-wrap items-center gap-2">
                  <div className="font-semibold">@{c.commenter_username}</div>
                  <Badge className={`${badge.cn} border-0`}>
                    <BadgeIcon className="w-3 h-3 mr-1" /> {badge.label}
                  </Badge>
                  {/* Surface classified failure reasons so support staff
                      can see WHY a DM failed without opening the diagnostics
                      endpoint. Never leak raw Graph error bodies. */}
                  {c.dm_failure_reason && (
                    <span className="text-xs text-amber-700">
                      DM: <span className="font-mono">{c.dm_failure_reason}</span>
                    </span>
                  )}
                  {c.reply_failure_reason && (
                    <span className="text-xs text-rose-700">
                      Reply: <span className="font-mono">{c.reply_failure_reason}</span>
                    </span>
                  )}
                </div>
                <div className="text-slate-700 mt-1">{c.text}</div>
                <div className="text-xs text-slate-400 mt-1">
                  {c.created && new Date(c.created).toLocaleString()}
                  {c.media_id && <> • on media <span className="font-mono">{c.media_id}</span></>}
                  {typeof c.attempts === 'number' && c.attempts > 1 && (
                    <> • {c.attempts} attempts</>
                  )}
                  {c.next_retry_at && (
                    <> • next retry {new Date(c.next_retry_at).toLocaleString()}</>
                  )}
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
                {/* Safe Retry Reply: only shown when there is no provider-
                    proven reply yet AND the failure isn't permanent. */}
                {canRetryReply(c) && (replyText[c.id] || '').trim() === '' && (
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => {
                      setReplyText(prev => ({
                        ...prev,
                        [c.id]: prev[c.id] || c.reply_text || '',
                      }));
                      toast.info('Type a reply, then send. Retry will not duplicate a successful reply.');
                    }}
                    className="sm:w-auto"
                  >
                    <Repeat className="w-4 h-4 mr-2" /> Retry reply
                  </Button>
                )}
              </form>
            )}
          </div>
          );
        })}
      </div>

      {hasMore && (
        <div className="mt-8 text-center">
          <Button 
            variant="outline" 
            onClick={() => fetchComments(page + 1, false)}
            disabled={loadingMore}
            className="rounded-full px-8"
          >
            {loadingMore ? 'Loading...' : 'Load More'}
          </Button>
        </div>
      )}
    </div>
  );
};

export default Comments;


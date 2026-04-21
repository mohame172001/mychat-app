import React, { useEffect, useState, useRef, useCallback } from 'react';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Badge } from '../components/ui/badge';
import { Search, Send, Paperclip, Smile, Instagram, MoreVertical, Tag, Wifi, WifiOff } from 'lucide-react';
import api, { API_BASE } from '../lib/api';

const WS_URL = API_BASE.replace(/^http/, 'ws').replace('/api', '');

const LiveChat = () => {
  const [convos, setConvos] = useState([]);
  const [activeId, setActiveId] = useState(null);
  const [text, setText] = useState('');
  const [wsReady, setWsReady] = useState(false);
  const scrollRef = useRef(null);
  const wsRef = useRef(null);
  const reconnectTimer = useRef(null);

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
        if (data.type === 'message' || data.type === 'incoming') {
          setConvos(prev => prev.map(c =>
            c.id === data.conv_id
              ? { ...c, messages: [...(c.messages || []), data.message],
                  lastMessage: data.message.text, time: 'now',
                  unread: data.type === 'incoming' ? (c.unread || 0) + 1 : 0 }
              : c
          ));
        }
      } catch (_) {}
    };
  }, []);

  useEffect(() => {
    (async () => {
      try {
        const { data } = await api.get('/conversations');
        setConvos(data);
        if (data.length > 0) setActiveId(data[0].id);
      } catch (err) {
        console.error('[LiveChat] Failed to load conversations:', err);
      }
    })();
    connectWs();
    return () => {
      clearTimeout(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [connectWs]);

  const active = convos.find(c => c.id === activeId);

  useEffect(() => {
    if (scrollRef.current) scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
  }, [activeId, convos]);

  const handleSend = async (e) => {
    e.preventDefault();
    if (!text.trim() || !active) return;
    const body = text.trim();
    setText('');

    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: 'message', conv_id: active.id, text: body }));
    } else {
      // fallback to REST
      try {
        const { data } = await api.post(`/conversations/${active.id}/messages`, { text: body });
        setConvos(prev => prev.map(c => c.id === active.id
          ? { ...c, messages: data.messages, lastMessage: data.messages[data.messages.length - 1]?.text || c.lastMessage, time: 'now', unread: 0 }
          : c));
      } catch (err) {
        console.error('[LiveChat] Failed to send message:', err);
      }
    }
  };

  return (
    <div className="h-[calc(100vh-4rem)] flex bg-slate-50">
      <aside className="w-80 border-r border-slate-200 bg-white flex flex-col">
        <div className="p-4 border-b border-slate-100">
          <div className="flex items-center justify-between">
            <h2 className="font-display text-lg font-bold">Inbox</h2>
            {wsReady
              ? <span className="flex items-center gap-1 text-xs text-emerald-600"><Wifi className="w-3 h-3" /> Live</span>
              : <span className="flex items-center gap-1 text-xs text-slate-400"><WifiOff className="w-3 h-3" /> Offline</span>}
          </div>
          <div className="relative mt-3">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
            <Input placeholder="Search..." className="pl-9 h-10 rounded-xl bg-slate-50 border-slate-100" />
          </div>
        </div>
        <div className="flex-1 overflow-y-auto">
          {convos.map(c => (
            <button key={c.id} onClick={() => { setActiveId(c.id); setConvos(prev => prev.map(x => x.id === c.id ? { ...x, unread: 0 } : x)); }}
              className={`w-full flex items-start gap-3 p-4 border-b border-slate-50 hover:bg-slate-50 transition-colors text-left ${activeId === c.id ? 'bg-slate-50' : ''}`}>
              <div className="relative">
                <img src={c.contact.avatar} alt={c.contact.name} className="w-11 h-11 rounded-full object-cover" />
                <Instagram className="absolute -bottom-0.5 -right-0.5 w-4 h-4 text-pink-500 bg-white rounded-full p-0.5" />
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center justify-between">
                  <div className="font-semibold text-sm truncate">{c.contact.name}</div>
                  <div className="text-xs text-slate-500 shrink-0 ml-1">{c.time}</div>
                </div>
                <div className="flex items-center justify-between mt-0.5">
                  <div className="text-sm text-slate-600 truncate">{c.lastMessage}</div>
                  {c.unread > 0 && <Badge className="bg-pink-500 text-white border-0 rounded-full ml-2 shrink-0">{c.unread}</Badge>}
                </div>
              </div>
            </button>
          ))}
          {convos.length === 0 && <div className="p-6 text-center text-sm text-slate-500">No conversations</div>}
        </div>
      </aside>

      <main className="flex-1 flex flex-col">
        {active ? (
          <>
            <div className="h-16 px-6 bg-white border-b border-slate-200 flex items-center justify-between shrink-0">
              <div className="flex items-center gap-3">
                <img src={active.contact.avatar} alt={active.contact.name} className="w-10 h-10 rounded-full object-cover" />
                <div>
                  <div className="font-semibold text-sm">{active.contact.name}</div>
                  <div className="text-xs text-slate-500">{active.contact.username} • {wsReady ? 'Live' : 'Connecting...'}</div>
                </div>
              </div>
              <Button variant="ghost" size="icon" className="rounded-lg"><MoreVertical className="w-4 h-4" /></Button>
            </div>
            <div ref={scrollRef} className="flex-1 overflow-y-auto p-6 space-y-3">
              {active.messages.map(m => (
                <div key={m.id} className={`flex ${m.from === 'me' ? 'justify-end' : 'justify-start'}`}>
                  <div className={`max-w-md px-4 py-2.5 rounded-2xl text-sm ${m.from === 'me' ? 'bg-gradient-to-br from-blue-500 to-cyan-400 text-white rounded-br-md' : 'bg-white text-slate-900 border border-slate-100 rounded-bl-md'}`}>
                    {m.text}
                    <div className={`text-[10px] mt-1 ${m.from === 'me' ? 'text-white/70' : 'text-slate-400'}`}>{m.time}</div>
                  </div>
                </div>
              ))}
            </div>
            <form onSubmit={handleSend} className="p-4 border-t border-slate-200 bg-white">
              <div className="flex items-center gap-2 bg-slate-50 rounded-2xl px-3">
                <Button type="button" variant="ghost" size="icon" className="rounded-lg"><Paperclip className="w-4 h-4 text-slate-500" /></Button>
                <Input value={text} onChange={e => setText(e.target.value)} placeholder="Type a message..." className="border-0 bg-transparent focus-visible:ring-0 h-12 flex-1" />
                <Button type="button" variant="ghost" size="icon" className="rounded-lg"><Smile className="w-4 h-4 text-slate-500" /></Button>
                <Button type="submit" size="icon" className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl"><Send className="w-4 h-4" /></Button>
              </div>
            </form>
          </>
        ) : (
          <div className="flex-1 flex items-center justify-center text-slate-500">Select a conversation</div>
        )}
      </main>

      <aside className="hidden xl:flex w-72 bg-white border-l border-slate-200 p-5 flex-col">
        {active && (
          <>
            <div className="text-center">
              <img src={active.contact.avatar} alt={active.contact.name} className="w-20 h-20 rounded-full object-cover mx-auto" />
              <h3 className="mt-3 font-display font-bold">{active.contact.name}</h3>
              <div className="text-sm text-slate-500">{active.contact.username}</div>
            </div>
            <div className="mt-6 space-y-3">
              <Button variant="outline" className="w-full justify-start rounded-xl"><Tag className="w-4 h-4 mr-2" /> Add tag</Button>
              <Button variant="outline" className="w-full justify-start rounded-xl"><Instagram className="w-4 h-4 mr-2" /> View on Instagram</Button>
            </div>
          </>
        )}
      </aside>
    </div>
  );
};

export default LiveChat;

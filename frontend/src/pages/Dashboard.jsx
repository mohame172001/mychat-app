import React, { useEffect, useState } from 'react';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import {
  Users, Zap, Send, TrendingUp, ArrowUpRight, Instagram, Plus
} from 'lucide-react';
import { Link } from 'react-router-dom';
import api from '../lib/api';
import { useAuth } from '../context/AuthContext';

const Dashboard = () => {
  const { user } = useAuth();
  const [stats, setStats] = useState(null);
  const [autos, setAutos] = useState([]);
  const [convos, setConvos] = useState([]);

  useEffect(() => {
    (async () => {
      try {
        const [s, a, c] = await Promise.all([
          api.get('/dashboard/stats'),
          api.get('/automations'),
          api.get('/conversations'),
        ]);
        setStats(s.data); setAutos(a.data); setConvos(c.data);
      } catch (err) {
        console.error('[Dashboard] Failed to load data:', err);
      }
    })();
  }, []);

  const chart = stats?.weekly_chart || [];
  const maxVal = chart.length ? Math.max(...chart.map(d => d.messages)) : 1;

  const statsCards = [
    { label: 'Total Contacts', value: stats?.total_contacts ?? '—', icon: Users, change: '+18.2%' },
    { label: 'Active Automations', value: stats?.active_automations ?? '—', icon: Zap, change: '+3' },
    { label: 'Messages Sent', value: (stats?.messages_sent ?? 0).toLocaleString(), icon: Send, change: '+24.5%' },
    { label: 'Conversion Rate', value: `${stats?.conversion_rate ?? 0}%`, icon: TrendingUp, change: '+2.1%' },
  ];

  return (
    <div className="p-8 max-w-7xl mx-auto">
      <div className="flex items-end justify-between flex-wrap gap-4">
        <div>
          <h1 className="font-display text-3xl font-extrabold tracking-tight">Good morning, {user?.name} 👋</h1>
          <p className="mt-1 text-slate-600">Here’s what’s happening with your Instagram automations today.</p>
        </div>
        <Link to="/app/automations">
          <Button className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl">
            <Plus className="w-4 h-4 mr-1.5" /> New Automation
          </Button>
        </Link>
      </div>

      <div className="mt-8 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {statsCards.map((s) => {
          const Icon = s.icon;
          return (
            <Card key={s.label} className="p-5 rounded-2xl border-slate-100 hover:shadow-md transition-shadow">
              <div className="flex items-start justify-between">
                <div className="w-10 h-10 rounded-xl bg-slate-100 flex items-center justify-center">
                  <Icon className="w-5 h-5 text-slate-700" />
                </div>
                <Badge className="bg-emerald-50 text-emerald-700 border-emerald-100 rounded-full">
                  <ArrowUpRight className="w-3 h-3 mr-0.5" />{s.change}
                </Badge>
              </div>
              <div className="mt-4 text-3xl font-extrabold font-display">{s.value}</div>
              <div className="text-sm text-slate-500 mt-1">{s.label}</div>
            </Card>
          );
        })}
      </div>

      <div className="mt-6 grid lg:grid-cols-3 gap-4">
        <Card className="lg:col-span-2 p-6 rounded-2xl border-slate-100">
          <div className="flex items-center justify-between">
            <div>
              <h3 className="font-display font-bold text-lg">Weekly Performance</h3>
              <p className="text-sm text-slate-500">Messages sent vs conversions</p>
            </div>
            <div className="flex gap-3 text-xs">
              <div className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full bg-blue-500" />Messages</div>
              <div className="flex items-center gap-1.5"><span className="w-2.5 h-2.5 rounded-full bg-pink-500" />Conversions</div>
            </div>
          </div>
          <div className="mt-6 flex items-end justify-between gap-3 h-56">
            {chart.map((d) => (
              <div key={d.day} className="flex-1 flex flex-col items-center gap-2">
                <div className="w-full flex items-end gap-1 h-48">
                  <div className="flex-1 rounded-t-lg bg-gradient-to-t from-blue-500 to-cyan-400" style={{ height: `${(d.messages / maxVal) * 100}%` }} />
                  <div className="flex-1 rounded-t-lg bg-gradient-to-t from-pink-500 to-orange-400" style={{ height: `${(d.conversions / maxVal) * 100}%` }} />
                </div>
                <div className="text-xs text-slate-500 font-medium">{d.day}</div>
              </div>
            ))}
          </div>
        </Card>

        <Card className="p-6 rounded-2xl border-slate-100 bg-gradient-to-br from-pink-500 via-fuchsia-500 to-orange-400 text-white relative overflow-hidden">
          <Instagram className="w-8 h-8" />
          <h3 className="mt-4 font-display font-bold text-xl">{user?.instagramConnected ? 'Instagram Connected' : 'Connect Instagram'}</h3>
          <p className="mt-1 text-sm text-white/90">{user?.instagramHandle || '@your_handle'} is {user?.instagramConnected ? 'linked and actively responding' : 'not connected yet'}.</p>
          <div className="mt-6 space-y-3">
            <div className="flex justify-between text-sm"><span className="text-white/80">Response rate</span><span className="font-bold">98.2%</span></div>
            <div className="flex justify-between text-sm"><span className="text-white/80">Avg response</span><span className="font-bold">&lt; 1s</span></div>
            <div className="flex justify-between text-sm"><span className="text-white/80">Uptime</span><span className="font-bold">100%</span></div>
          </div>
          <Link to="/app/settings"><Button className="mt-6 w-full bg-white text-slate-900 hover:bg-slate-100 rounded-xl">Manage Connection</Button></Link>
        </Card>
      </div>

      <div className="mt-6 grid lg:grid-cols-2 gap-4">
        <Card className="p-6 rounded-2xl border-slate-100">
          <div className="flex items-center justify-between">
            <h3 className="font-display font-bold text-lg">Top Automations</h3>
            <Link to="/app/automations" className="text-sm font-medium text-slate-600 hover:text-slate-900">View all</Link>
          </div>
          <div className="mt-4 space-y-3">
            {autos.slice(0, 4).map(a => (
              <div key={a.id} className="flex items-center gap-3 p-3 rounded-xl hover:bg-slate-50 transition-colors">
                <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 to-cyan-400 flex items-center justify-center"><Zap className="w-5 h-5 text-white" /></div>
                <div className="flex-1 min-w-0">
                  <div className="font-semibold text-sm truncate">{a.name}</div>
                  <div className="text-xs text-slate-500">{a.trigger} • {(a.sent || 0).toLocaleString()} sent</div>
                </div>
                <Badge className={`rounded-full ${a.status === 'active' ? 'bg-emerald-50 text-emerald-700 border-emerald-100' : a.status === 'paused' ? 'bg-amber-50 text-amber-700 border-amber-100' : 'bg-slate-100 text-slate-600 border-slate-200'}`}>
                  {a.status}
                </Badge>
              </div>
            ))}
            {autos.length === 0 && <div className="text-sm text-slate-500 text-center py-6">No automations yet</div>}
          </div>
        </Card>

        <Card className="p-6 rounded-2xl border-slate-100">
          <div className="flex items-center justify-between">
            <h3 className="font-display font-bold text-lg">Recent Conversations</h3>
            <Link to="/app/live-chat" className="text-sm font-medium text-slate-600 hover:text-slate-900">View all</Link>
          </div>
          <div className="mt-4 space-y-3">
            {convos.slice(0, 4).map(c => (
              <div key={c.id} className="flex items-center gap-3 p-3 rounded-xl hover:bg-slate-50 transition-colors">
                <img src={c.contact.avatar} alt={c.contact.name} className="w-10 h-10 rounded-full object-cover" />
                <div className="flex-1 min-w-0">
                  <div className="flex items-center justify-between">
                    <div className="font-semibold text-sm">{c.contact.name}</div>
                    <div className="text-xs text-slate-500">{c.time}</div>
                  </div>
                  <div className="text-sm text-slate-600 truncate">{c.lastMessage}</div>
                </div>
                {c.unread > 0 && <Badge className="bg-pink-500 text-white rounded-full border-0">{c.unread}</Badge>}
              </div>
            ))}
            {convos.length === 0 && <div className="text-sm text-slate-500 text-center py-6">No conversations yet</div>}
          </div>
        </Card>
      </div>
    </div>
  );
};

export default Dashboard;

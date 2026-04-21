import React, { useState, useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Badge } from '../components/ui/badge';
import { Switch } from '../components/ui/switch';
import { Instagram, Key, Bell, CreditCard, User, Shield, Check, AlertCircle, Loader2 } from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import { toast } from 'sonner';
import api from '../lib/api';

const tabs = [
  { id: 'profile', label: 'Profile', icon: User },
  { id: 'instagram', label: 'Instagram', icon: Instagram },
  { id: 'notifications', label: 'Notifications', icon: Bell },
  { id: 'billing', label: 'Billing', icon: CreditCard },
  { id: 'security', label: 'Security', icon: Shield }
];

const Settings = () => {
  const { user, refreshUser } = useAuth();
  const location = useLocation();
  const [tab, setTab] = useState('profile');
  const [notif, setNotif] = useState({ email: true, push: true, weekly: false });
  const [igConnecting, setIgConnecting] = useState(false);

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const igStatus = params.get('ig');
    if (igStatus === 'connected') {
      setTab('instagram');
      refreshUser().then(() => toast.success('Instagram connected successfully!'));
      window.history.replaceState({}, '', location.pathname);
    } else if (igStatus === 'error') {
      setTab('instagram');
      const reason = params.get('reason') || 'unknown';
      toast.error(`Instagram connection failed: ${reason}`);
      window.history.replaceState({}, '', location.pathname);
    }
  }, [location.search]); // eslint-disable-line

  return (
    <div className="p-8 max-w-6xl mx-auto">
      <h1 className="font-display text-3xl font-extrabold tracking-tight">Settings</h1>
      <p className="mt-1 text-slate-600">Manage your account, Instagram connection and preferences.</p>

      <div className="mt-8 grid md:grid-cols-[240px_1fr] gap-6">
        <aside className="space-y-1">
          {tabs.map(t => {
            const Icon = t.icon;
            return (
              <button key={t.id} onClick={() => setTab(t.id)} className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-colors ${tab === t.id ? 'bg-slate-900 text-white' : 'text-slate-600 hover:bg-slate-100'}`}>
                <Icon className="w-4 h-4" /> {t.label}
              </button>
            );
          })}
        </aside>

        <div>
          {tab === 'profile' && (
            <Card className="p-6 rounded-2xl border-slate-100">
              <h3 className="font-display font-bold text-lg">Profile</h3>
              <p className="text-sm text-slate-500">Update your personal information.</p>
              <div className="mt-6 flex items-center gap-4">
                <img src={user?.avatar} alt="avatar" className="w-16 h-16 rounded-full object-cover" />
                <Button variant="outline" className="rounded-xl">Change photo</Button>
              </div>
              <div className="mt-6 grid sm:grid-cols-2 gap-4">
                <div className="space-y-2"><Label>Full name</Label><Input defaultValue={user?.name} className="h-11 rounded-xl" /></div>
                <div className="space-y-2"><Label>Username</Label><Input defaultValue={user?.username} className="h-11 rounded-xl" /></div>
                <div className="space-y-2 sm:col-span-2"><Label>Email</Label><Input defaultValue={user?.email} className="h-11 rounded-xl" /></div>
              </div>
              <div className="mt-6 flex justify-end">
                <Button onClick={() => toast.success('Profile updated')} className="bg-slate-900 text-white rounded-xl">Save changes</Button>
              </div>
            </Card>
          )}

          {tab === 'instagram' && (
            <Card className="p-6 rounded-2xl border-slate-100">
              <h3 className="font-display font-bold text-lg">Instagram Account</h3>
              <p className="text-sm text-slate-500">Connect your Instagram Business account to enable automations.</p>

              {user?.instagramConnected ? (
                <>
                  <div className="mt-6 p-5 rounded-2xl bg-gradient-to-br from-pink-50 via-purple-50 to-orange-50 border border-pink-100">
                    <div className="flex items-center gap-4">
                      <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-pink-500 via-fuchsia-500 to-orange-400 flex items-center justify-center">
                        <Instagram className="w-6 h-6 text-white" />
                      </div>
                      <div className="flex-1">
                        <div className="font-semibold">{user.instagramHandle}</div>
                        <div className="text-sm text-slate-600">Business account{user.instagramFollowers ? ` • ${user.instagramFollowers.toLocaleString()} followers` : ''}</div>
                      </div>
                      <Badge className="bg-emerald-100 text-emerald-700 border-0 rounded-full">
                        <Check className="w-3 h-3 mr-1" /> Connected
                      </Badge>
                    </div>
                  </div>
                  <div className="mt-6 flex justify-between">
                    <Button variant="outline" className="rounded-xl text-red-600 border-red-200 hover:bg-red-50" onClick={async () => {
                      try { await api.post('/instagram/disconnect'); await refreshUser(); toast.success('Disconnected'); }
                      catch { toast.error('Failed to disconnect'); }
                    }}>Disconnect</Button>
                    <Button onClick={async () => {
                      setIgConnecting(true);
                      try { const { data } = await api.get('/instagram/auth-url'); window.location.href = data.url; }
                      catch (e) { toast.error(e?.response?.data?.detail || 'Failed'); setIgConnecting(false); }
                    }} variant="outline" className="rounded-xl" disabled={igConnecting}>
                      {igConnecting ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : null}
                      Refresh Token
                    </Button>
                  </div>
                </>
              ) : (
                <>
                  <div className="mt-6 p-5 rounded-2xl bg-slate-50 border border-slate-200">
                    <div className="flex items-center gap-4">
                      <div className="w-12 h-12 rounded-xl bg-slate-200 flex items-center justify-center">
                        <Instagram className="w-6 h-6 text-slate-400" />
                      </div>
                      <div className="flex-1">
                        <div className="font-semibold text-slate-500">No account connected</div>
                        <div className="text-sm text-slate-400">Connect an Instagram Business or Creator account</div>
                      </div>
                      <Badge className="bg-slate-100 text-slate-500 border-0 rounded-full">
                        <AlertCircle className="w-3 h-3 mr-1" /> Not connected
                      </Badge>
                    </div>
                  </div>
                  <div className="mt-4 p-4 rounded-xl bg-amber-50 border border-amber-100 text-sm text-amber-700">
                    <strong>Requirements:</strong> You need an Instagram Business or Creator account linked to a Facebook Page.
                    Set <code className="bg-amber-100 px-1 rounded">META_APP_ID</code> and <code className="bg-amber-100 px-1 rounded">META_APP_SECRET</code> in your backend <code className="bg-amber-100 px-1 rounded">.env</code> file first.
                  </div>
                  <div className="mt-6 flex justify-end">
                    <Button onClick={async () => {
                      setIgConnecting(true);
                      try { const { data } = await api.get('/instagram/auth-url'); window.location.href = data.url; }
                      catch (e) { toast.error(e?.response?.data?.detail || 'Failed — check META_APP_ID/SECRET in .env'); setIgConnecting(false); }
                    }} className="bg-slate-900 text-white rounded-xl" disabled={igConnecting}>
                      {igConnecting ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Instagram className="w-4 h-4 mr-2" />}
                      Connect Instagram
                    </Button>
                  </div>
                </>
              )}
            </Card>
          )}

          {tab === 'notifications' && (
            <Card className="p-6 rounded-2xl border-slate-100">
              <h3 className="font-display font-bold text-lg">Notifications</h3>
              <p className="text-sm text-slate-500">Choose how you want to be notified.</p>
              <div className="mt-6 space-y-4">
                {[
                  { id: 'email', label: 'Email notifications', desc: 'Get email alerts for new messages and activity.' },
                  { id: 'push', label: 'Push notifications', desc: 'Receive browser push notifications in real-time.' },
                  { id: 'weekly', label: 'Weekly summary', desc: 'A weekly digest of your automation performance.' }
                ].map(n => (
                  <div key={n.id} className="flex items-center justify-between p-4 rounded-xl border border-slate-100">
                    <div>
                      <div className="font-semibold text-sm">{n.label}</div>
                      <div className="text-xs text-slate-500 mt-0.5">{n.desc}</div>
                    </div>
                    <Switch checked={notif[n.id]} onCheckedChange={(v) => setNotif({ ...notif, [n.id]: v })} />
                  </div>
                ))}
              </div>
            </Card>
          )}

          {tab === 'billing' && (
            <Card className="p-6 rounded-2xl border-slate-100">
              <h3 className="font-display font-bold text-lg">Billing</h3>
              <div className="mt-4 p-5 rounded-2xl bg-slate-900 text-white">
                <div className="flex items-center justify-between">
                  <div>
                    <div className="text-sm opacity-80">Current plan</div>
                    <div className="mt-1 font-display text-2xl font-bold">Pro Plan</div>
                  </div>
                  <Badge className="bg-white/10 text-white border-0 rounded-full">$15/mo</Badge>
                </div>
                <div className="mt-4 text-sm opacity-80">Next billing date: December 24, 2025</div>
              </div>
              <div className="mt-6 flex gap-3">
                <Button className="bg-slate-900 text-white rounded-xl">Upgrade plan</Button>
                <Button variant="outline" className="rounded-xl">View invoices</Button>
              </div>
            </Card>
          )}

          {tab === 'security' && (
            <Card className="p-6 rounded-2xl border-slate-100">
              <h3 className="font-display font-bold text-lg">Security</h3>
              <div className="mt-6 space-y-4">
                <div className="space-y-2"><Label>Current password</Label><Input type="password" className="h-11 rounded-xl" /></div>
                <div className="space-y-2"><Label>New password</Label><Input type="password" className="h-11 rounded-xl" /></div>
                <div className="space-y-2"><Label>Confirm new password</Label><Input type="password" className="h-11 rounded-xl" /></div>
              </div>
              <Button onClick={() => toast.success('Password updated')} className="mt-6 bg-slate-900 text-white rounded-xl">Update password</Button>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
};

export default Settings;

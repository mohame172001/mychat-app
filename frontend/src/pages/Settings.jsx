import React, { useState, useEffect } from 'react';
import { useLocation, Link } from 'react-router-dom';
import { Card } from '../components/ui/card';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { PasswordInput } from '../components/ui/password-input';
import { Label } from '../components/ui/label';
import { Badge } from '../components/ui/badge';
import { Switch } from '../components/ui/switch';
import { Instagram, Key, Bell, CreditCard, User, Shield, Check, AlertCircle, Loader2 } from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import { toast } from 'sonner';
import api from '../lib/api';
import { startInstagramConnect } from '../lib/instagramConnect';
import { instagramErrorMessage, instagramConnectExceptionMessage } from '../lib/instagramErrors';

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
  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [changingPassword, setChangingPassword] = useState(false);
  // Phase 2.18U: Profile-editable state. We hold a draft of name +
  // username locally so the user can type without round-tripping
  // every keystroke, then submit on Save.
  const [profileDraft, setProfileDraft] = useState({ name: '', username: '' });
  const [savingProfile, setSavingProfile] = useState(false);

  useEffect(() => {
    if (user) {
      setProfileDraft({
        name: user.name || '',
        username: user.username || '',
      });
    }
    // We intentionally watch only the two fields we care about; pulling
    // in `user` would re-init the draft on unrelated refreshUser() calls
    // and overwrite an in-progress edit.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user?.name, user?.username]);

  const profileDirty = Boolean(
    user
    && (profileDraft.name.trim() !== (user.name || '').trim()
        || profileDraft.username.trim() !== (user.username || '').trim())
  );

  const saveProfile = async () => {
    if (!profileDirty) return;
    setSavingProfile(true);
    try {
      const payload = {};
      const nextName = profileDraft.name.trim();
      const nextUsername = profileDraft.username.trim();
      if (nextName !== (user?.name || '').trim()) payload.name = nextName;
      if (nextUsername !== (user?.username || '').trim()) payload.username = nextUsername;
      await api.patch('/auth/me', payload);
      toast.success('Profile updated');
      if (typeof refreshUser === 'function') {
        await refreshUser();
      }
    } catch (err) {
      const detail = err?.response?.data?.detail;
      const map = {
        username_already_taken: 'That username is already taken — pick another.',
        username_invalid_characters: 'Username can only contain letters, numbers, underscores, dots, and hyphens.',
        username_length_out_of_range: 'Username must be 3 to 32 characters.',
        name_length_out_of_range: 'Name must be 1 to 80 characters.',
      };
      toast.error(map[detail] || (typeof detail === 'string' ? detail : 'Profile update failed'));
    } finally {
      setSavingProfile(false);
    }
  };

  const hasKnownInstagramIdentity = Boolean(user?.instagramHandle || user?.activeInstagramAccountId || user?.activeInstagramIgUserId);
  const instagramReconnectMode = (user?.instagramConnected || user?.instagramConnectionValid === false || hasKnownInstagramIdentity)
    ? 'reconnect'
    : 'connect';

  useEffect(() => {
    const params = new URLSearchParams(location.search);
    const requestedTab = params.get('tab');
    if (tabs.some(t => t.id === requestedTab)) {
      setTab(requestedTab);
    }
    const igStatus = params.get('ig');
    if (igStatus === 'connected') {
      setTab('instagram');
      refreshUser().then(() => toast.success('Instagram connected successfully!'));
      window.history.replaceState({}, '', location.pathname);
    } else if (igStatus === 'error') {
      setTab('instagram');
      const reason = params.get('reason') || 'unknown';
      toast.error(instagramErrorMessage(reason));
      window.history.replaceState({}, '', location.pathname);
    }
  }, [location.search]); // eslint-disable-line

  return (
    <div className="p-4 sm:p-6 lg:p-8 max-w-6xl mx-auto">
      <h1 className="font-display text-3xl font-extrabold tracking-tight">Settings</h1>
      <p className="mt-1 text-slate-600">Manage your account, Instagram connection and preferences.</p>

      <div className="mt-8 grid md:grid-cols-[240px_1fr] gap-6">
        <aside className="mobile-nav-scroll -mx-1 flex gap-2 overflow-x-auto px-1 pb-1 md:mx-0 md:block md:space-y-1 md:overflow-visible md:px-0 md:pb-0">
          {tabs.map(t => {
            const Icon = t.icon;
            return (
              <button key={t.id} onClick={() => setTab(t.id)} className={`shrink-0 md:w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-colors ${tab === t.id ? 'bg-slate-900 text-white' : 'bg-white text-slate-600 hover:bg-slate-100 md:bg-transparent'}`}>
                <Icon className="w-4 h-4" /> {t.label}
              </button>
            );
          })}
        </aside>

        <div>
          {tab === 'profile' && (
            <Card className="p-6 rounded-2xl border-slate-100" data-testid="settings-profile">
              <h3 className="font-display font-bold text-lg">Profile</h3>
              <p className="text-sm text-slate-500">Your account profile. Name and username are editable.</p>
              <div className="mt-6 flex items-center gap-4">
                <img src={user?.avatar} alt="avatar" className="w-16 h-16 rounded-full object-cover" />
              </div>
              <div className="mt-6 grid sm:grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="profile-name">Full name</Label>
                  <Input
                    id="profile-name"
                    value={profileDraft.name}
                    onChange={(e) => setProfileDraft((p) => ({ ...p, name: e.target.value }))}
                    maxLength={80}
                    placeholder="Your full name"
                    className="h-11 rounded-xl"
                    disabled={savingProfile}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="profile-username">Username</Label>
                  <Input
                    id="profile-username"
                    value={profileDraft.username}
                    onChange={(e) => setProfileDraft((p) => ({ ...p, username: e.target.value }))}
                    maxLength={32}
                    placeholder="username"
                    className="h-11 rounded-xl"
                    disabled={savingProfile}
                  />
                </div>
                <div className="space-y-2 sm:col-span-2">
                  <Label htmlFor="profile-email">Email</Label>
                  <Input
                    id="profile-email"
                    value={user?.email || ''}
                    readOnly
                    className="h-11 rounded-xl bg-slate-50"
                  />
                  <p className="text-xs text-slate-500">
                    Email changes require a verification flow that is not enabled yet.
                    Contact support if you need it changed.
                  </p>
                </div>
              </div>
              <div className="mt-6 flex items-center justify-end gap-3">
                {profileDirty && !savingProfile && (
                  <button
                    type="button"
                    onClick={() => setProfileDraft({
                      name: user?.name || '',
                      username: user?.username || '',
                    })}
                    className="text-sm text-slate-500 hover:text-slate-700"
                    data-testid="profile-cancel"
                  >
                    Cancel
                  </button>
                )}
                <Button
                  onClick={saveProfile}
                  disabled={!profileDirty || savingProfile}
                  className="rounded-xl bg-slate-900 hover:bg-slate-800 text-white"
                  data-testid="profile-save"
                >
                  {savingProfile ? (<><Loader2 className="w-4 h-4 mr-2 animate-spin" /> Saving…</>) : 'Save changes'}
                </Button>
              </div>
            </Card>
          )}

          {tab === 'instagram' && (
            <Card className="p-6 rounded-2xl border-slate-100">
              <h3 className="font-display font-bold text-lg">Instagram Account</h3>
              <p className="text-sm text-slate-500">Connect your Instagram Business account to enable automations.</p>

              {user?.instagramConnected && user?.instagramConnectionValid ? (
                <>
                  <div className="mt-6 p-5 rounded-2xl bg-gradient-to-br from-pink-50 via-purple-50 to-orange-50 border border-pink-100">
                    <div className="flex flex-col gap-4 sm:flex-row sm:items-center">
                      <div className="w-12 h-12 rounded-xl bg-gradient-to-br from-pink-500 via-fuchsia-500 to-orange-400 flex items-center justify-center">
                        <Instagram className="w-6 h-6 text-white" />
                      </div>
                      <div className="flex-1">
                        <div className="font-semibold">{user.instagramHandle}</div>
                        <div className="text-sm text-slate-600">Business account{user.instagramFollowers ? ` • ${user.instagramFollowers.toLocaleString()} followers` : ''}</div>
                      </div>
                      <Badge className="w-fit bg-emerald-100 text-emerald-700 border-0 rounded-full">
                        <Check className="w-3 h-3 mr-1" /> Connected
                      </Badge>
                    </div>
                  </div>
                  <div className="mt-6 flex justify-between flex-wrap gap-2">
                    <Button variant="outline" className="rounded-xl text-red-600 border-red-200 hover:bg-red-50" onClick={async () => {
                      try { await api.post('/instagram/disconnect'); await refreshUser(); toast.success('Disconnected'); }
                      catch { toast.error('Failed to disconnect'); }
                    }}>Disconnect</Button>
                    <div className="flex gap-2">
                      <Button onClick={async () => {
                        setIgConnecting(true);
                        try { await startInstagramConnect({ mode: 'reconnect', returnTo: '/app/settings?tab=instagram' }); }
                        catch (e) { toast.error(instagramConnectExceptionMessage(e)); setIgConnecting(false); }
                      }} variant="outline" className="rounded-xl" disabled={igConnecting}>
                        {igConnecting ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : null}
                        Refresh Token
                      </Button>
                    </div>
                  </div>
                </>
              ) : (
                <>
                  <div className="mt-6 p-5 rounded-2xl bg-slate-50 border border-slate-200">
                    <div className="flex flex-col gap-4 sm:flex-row sm:items-center">
                      <div className="w-12 h-12 rounded-xl bg-slate-200 flex items-center justify-center">
                        <Instagram className="w-6 h-6 text-slate-400" />
                      </div>
                      <div className="flex-1">
                        <div className="font-semibold text-slate-500">No account connected</div>
                        <div className="text-sm text-slate-400">
                          {user?.instagramConnectionValid === false ? 'Reconnect Instagram to verify the access token' : 'Connect an Instagram Business or Creator account'}
                        </div>
                      </div>
                      <Badge className="w-fit bg-slate-100 text-slate-500 border-0 rounded-full">
                        <AlertCircle className="w-3 h-3 mr-1" /> Not connected
                      </Badge>
                    </div>
                  </div>
                  <div className="mt-4 p-4 rounded-xl bg-amber-50 border border-amber-100 text-sm text-amber-700">
                    <strong>Requirements:</strong> You need an Instagram Business or Creator account. The app verifies Graph <code className="bg-amber-100 px-1 rounded">/me</code> before showing the account as connected.
                  </div>
                  <div className="mt-6 flex justify-end">
                    <Button onClick={async () => {
                      setIgConnecting(true);
                      try { await startInstagramConnect({ mode: instagramReconnectMode, returnTo: '/app/settings?tab=instagram' }); }
                      catch (e) { toast.error(instagramConnectExceptionMessage(e, 'Failed - check IG_APP_ID/IG_APP_SECRET in .env')); setIgConnecting(false); }
                    }} className="w-full bg-slate-900 text-white rounded-xl sm:w-auto" disabled={igConnecting}>
                      {igConnecting ? <Loader2 className="w-4 h-4 mr-2 animate-spin" /> : <Instagram className="w-4 h-4 mr-2" />}
                      {instagramReconnectMode === 'reconnect' ? 'Reconnect Instagram' : 'Connect Instagram'}
                    </Button>
                  </div>

                </>
              )}
            </Card>
          )}

          {tab === 'notifications' && (
            <Card className="p-6 rounded-2xl border-slate-100" data-testid="settings-notifications">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <h3 className="font-display font-bold text-lg">Notifications</h3>
                  <p className="text-sm text-slate-500">Choose how you want to be notified.</p>
                </div>
                <Badge className="bg-amber-50 text-amber-700 border-amber-100 rounded-full">
                  Coming soon
                </Badge>
              </div>
              {/* Phase 2.18S: the toggles below are placeholder UI —
                  there is no /api/notifications/preferences endpoint
                  yet. Disabling them and adding the explicit banner
                  is more honest than letting users flip switches that
                  silently revert on refresh. */}
              <div className="mt-4 p-4 rounded-xl bg-amber-50 border border-amber-100 text-sm text-amber-700">
                Notification preferences will be saved once we ship the
                preferences endpoint. For now you receive critical
                account emails (password reset, plan changes) by
                default — nothing else is sent.
              </div>
              <div className="mt-6 space-y-4 opacity-60 pointer-events-none">
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
                    <Switch checked={false} disabled aria-disabled="true" />
                  </div>
                ))}
              </div>
            </Card>
          )}

          {tab === 'billing' && (
            <Card className="p-6 rounded-2xl border-slate-100" data-testid="settings-billing-summary">
              <h3 className="font-display font-bold text-lg">Billing</h3>
              <p className="mt-2 text-sm text-slate-500">
                Billing is not enabled yet. Plan upgrades with payment will be
                available later — during beta, contact support to change your plan.
              </p>
              <div className="mt-4 p-5 rounded-2xl bg-slate-50 border border-slate-100">
                <div className="flex items-center justify-between">
                  <div>
                    <div className="text-xs uppercase tracking-wide font-semibold text-slate-500">
                      Status
                    </div>
                    <div className="mt-1 font-display text-base font-semibold text-slate-800">
                      Beta — billing disabled
                    </div>
                  </div>
                  <Badge className="bg-amber-100 text-amber-800 border-0 rounded-full">
                    No payment required
                  </Badge>
                </div>
              </div>
              <div className="mt-6">
                <Button asChild className="bg-slate-900 text-white rounded-xl">
                  <Link to="/app/billing">View usage &amp; plans</Link>
                </Button>
              </div>
            </Card>
          )}

          {tab === 'security' && (
            <Card className="p-6 rounded-2xl border-slate-100">
              <h3 className="font-display font-bold text-lg">Security</h3>
              <form
                onSubmit={async (e) => {
                  e.preventDefault();
                  if (!currentPassword || !newPassword || !confirmPassword) {
                    toast.error('Please fill in all password fields');
                    return;
                  }
                  if (newPassword.length < 8) {
                    toast.error('New password must be at least 8 characters');
                    return;
                  }
                  if (newPassword !== confirmPassword) {
                    toast.error('New passwords do not match');
                    return;
                  }
                  setChangingPassword(true);
                  try {
                    await api.post('/auth/password', {
                      current_password: currentPassword,
                      new_password: newPassword,
                    });
                    toast.success('Password changed successfully. Use the new password next time you sign in.');
                    setCurrentPassword('');
                    setNewPassword('');
                    setConfirmPassword('');
                  } catch (err) {
                    const detail = err?.response?.data?.detail || 'Failed to change password';
                    toast.error(typeof detail === 'string' ? detail : 'Failed to change password');
                  } finally {
                    setChangingPassword(false);
                  }
                }}
                className="mt-6 space-y-4"
              >
                <div className="space-y-2">
                  <Label htmlFor="current-password">Current password</Label>
                  <PasswordInput
                    id="current-password"
                    autoComplete="current-password"
                    value={currentPassword}
                    onChange={e => setCurrentPassword(e.target.value)}
                    inputClassName="h-11 rounded-xl"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="new-password">New password</Label>
                  <PasswordInput
                    id="new-password"
                    autoComplete="new-password"
                    value={newPassword}
                    onChange={e => setNewPassword(e.target.value)}
                    inputClassName="h-11 rounded-xl"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="confirm-password">Confirm new password</Label>
                  <PasswordInput
                    id="confirm-password"
                    autoComplete="new-password"
                    value={confirmPassword}
                    onChange={e => setConfirmPassword(e.target.value)}
                    inputClassName="h-11 rounded-xl"
                  />
                </div>
                <Button
                  type="submit"
                  disabled={changingPassword}
                  className="bg-slate-900 text-white rounded-xl"
                >
                  {changingPassword ? 'Updating...' : 'Update password'}
                </Button>
              </form>
            </Card>
          )}
        </div>
      </div>
    </div>
  );
};

export default Settings;

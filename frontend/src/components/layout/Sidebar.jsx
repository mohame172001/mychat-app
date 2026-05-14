import React, { useEffect, useState } from 'react';
import { NavLink, Link, useNavigate } from 'react-router-dom';
import {
  LayoutDashboard, Zap, Settings,
  MessageCircle, HelpCircle, LogOut, Inbox, ChevronDown, Check, Instagram,
  CreditCard, ShieldCheck,
} from 'lucide-react';
import { useAuth } from '../../context/AuthContext';
import { Button } from '../ui/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '../ui/dropdown-menu';
import api from '../../lib/api';
import { toast } from 'sonner';
import { startInstagramConnect } from '../../lib/instagramConnect';
import { useIsAdmin } from '../../lib/useIsAdmin';
import { cachedApiGetSWR, getCachedApiData, invalidateApiCache } from '../../lib/apiCache';
import { preloadRoute } from '../../lib/routePreloader';
import { scheduleCoreAppWarmup } from '../../lib/appWarmup';

export const navItems = [
  { to: '/app', end: true, icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/app/automations', icon: Zap, label: 'Automations' },
  { to: '/app/dm-automation', icon: Inbox, label: 'DM Automation' },
  { to: '/app/billing', icon: CreditCard, label: 'Billing' },
  { to: '/app/settings', icon: Settings, label: 'Settings' }
];

const Sidebar = () => {
  const { logout, user, refreshUser } = useAuth();
  const navigate = useNavigate();
  const accountsCacheKey = `instagram-accounts:${user?.id || 'anon'}`;
  const [instagramAccounts, setInstagramAccounts] = useState(() => (
    getCachedApiData(accountsCacheKey, { maxStaleMs: 10 * 60 * 1000 })?.accounts || []
  ));
  const [switchingAccount, setSwitchingAccount] = useState(false);
  const { isAdmin } = useIsAdmin();

  useEffect(() => {
    let alive = true;
    const loadAccounts = async () => {
      try {
        const result = await cachedApiGetSWR(
          accountsCacheKey,
          () => api.get('/instagram/accounts'),
          {
            ttlMs: 180 * 1000,
            maxStaleMs: 10 * 60 * 1000,
            persist: true,
            onUpdate: (data) => {
              if (alive) setInstagramAccounts(data?.accounts || []);
            },
          }
        );
        const data = result.data;
        if (alive) setInstagramAccounts(data?.accounts || []);
      } catch {
        if (alive) setInstagramAccounts([]);
      }
    };
    if (user?.instagramConnected) {
      loadAccounts();
    }
    return () => {
      alive = false;
    };
  }, [accountsCacheKey, user?.instagramConnected, user?.instagramHandle]);

  const currentAccount = instagramAccounts.find(account => account.active || account.isCurrent) || instagramAccounts[0];
  // Phase 2.18M: do NOT fall back to user.instagramHandle when the
  // authoritative instagram_accounts query returned nothing. The
  // previous fallback was creating a phantom "@username" trigger
  // label even after the user had been disconnected — the Settings
  // page correctly said "Not connected" but this button still
  // showed the old handle, which made the state look inconsistent
  // and confused the user. We now only show an IG handle when a
  // real account is in the list; otherwise the button reads
  // "No Instagram account" and the dropdown shows the connect path.
  const currentAccountName = currentAccount?.username
    ? `@${currentAccount.username}`
    : null;
  const currentAccountAvatar = currentAccount?.profilePictureUrl
    || (currentAccount ? user?.instagramProfilePictureUrl : null)
    || user?.avatar;

  const switchInstagramAccount = async (account) => {
    if (!account?.id || account.isCurrent || switchingAccount) return;
    setSwitchingAccount(true);
    // Capture the previous active account so we can roll back the
    // optimistic UI flip on failure.
    const previousActive = instagramAccounts.find((a) => a.active || a.isCurrent) || null;
    // Phase 2.18H UX: optimistically flip the active flag in local
    // state the moment the user picks an account. The dropdown trigger
    // (which renders @username + avatar from the same state) now
    // updates INSTANTLY. The /activate POST + /auth/me refresh still
    // run in the background and reconcile authoritative state — but
    // the user no longer sees the previous account lingering until a
    // manual refresh.
    setInstagramAccounts((prev) => prev.map((a) => ({
      ...a,
      active: a.id === account.id,
      isCurrent: a.id === account.id,
    })));
    try {
      const { data } = await api.post(`/instagram/accounts/${account.id}/activate`);
      // The activate endpoint already returns the authoritative new
      // active account. Use it directly so we don't need to wait on a
      // second /instagram/accounts round-trip.
      if (data?.account) {
        setInstagramAccounts((prev) => prev.map((a) => (
          a.id === data.account.id
            ? { ...a, ...data.account, active: true, isCurrent: true }
            : { ...a, active: false, isCurrent: false }
        )));
      }
      invalidateApiCache('instagram-accounts');
      invalidateApiCache('dashboard-summary');
      invalidateApiCache('automations-summary');
      invalidateApiCache('instagram-media');
      // Kick off the user refresh + core warmup in parallel — neither
      // blocks the UI update; the optimistic state above is already
      // visible to the user.
      const refreshPromise = refreshUser?.();
      refreshPromise?.then((u) => scheduleCoreAppWarmup(u || user));
      toast.success(`Switched to @${account.username || account.instagramAccountId}`);
      navigate(`/app?igAccount=${encodeURIComponent(account.id)}`);
    } catch (e) {
      // Roll back the optimistic flip on failure so the dropdown
      // matches the server's view again.
      setInstagramAccounts((prev) => prev.map((a) => ({
        ...a,
        active: a.id === previousActive?.id,
        isCurrent: a.id === previousActive?.id,
      })));
      toast.error(e?.response?.data?.detail || 'Failed to switch Instagram account');
    }
    setSwitchingAccount(false);
  };

  const connectAnotherInstagramAccount = async (event) => {
    event?.preventDefault?.();
    event?.stopPropagation?.();
    setSwitchingAccount(true);
    try {
      await startInstagramConnect({ mode: 'add_account', returnTo: '/app' });
    } catch (e) {
      toast.error(e?.response?.data?.detail || e?.message || 'Failed to start Instagram connection');
      setSwitchingAccount(false);
    }
  };

  return (
    <aside className="hidden md:flex w-64 flex-col bg-white border-r border-slate-200">
      <Link to="/app" className="h-16 px-6 flex items-center gap-2 border-b border-slate-100">
        <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
          <MessageCircle className="w-5 h-5 text-white" strokeWidth={2.5} />
        </div>
        <span className="text-xl font-bold font-display">mychat</span>
      </Link>
      <nav className="flex-1 p-4 space-y-1">
        {navItems.map(({ to, end, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            end={end}
            onMouseEnter={() => preloadRoute(label)}
            onFocus={() => preloadRoute(label)}
            className={({ isActive }) => `flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-colors ${isActive ? 'bg-slate-900 text-white' : 'text-slate-600 hover:bg-slate-100'}`}
          >
            <Icon className="w-4 h-4" />
            {label}
          </NavLink>
        ))}
        {isAdmin && (
          <NavLink
            to="/app/admin"
            data-testid="sidebar-admin-link"
            onMouseEnter={() => preloadRoute('Admin')}
            onFocus={() => preloadRoute('Admin')}
            className={({ isActive }) => `flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-colors border-t border-slate-100 mt-2 pt-3 ${isActive ? 'bg-slate-900 text-white' : 'text-blue-700 hover:bg-blue-50'}`}
          >
            <ShieldCheck className="w-4 h-4" />
            Admin
          </NavLink>
        )}
      </nav>
      <div className="p-4 border-t border-slate-100 space-y-2">
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="flex w-full items-center gap-3 rounded-xl bg-slate-50 px-3 py-2 text-left transition hover:bg-slate-100">
              <img
                src={currentAccountAvatar}
                alt={currentAccountName || 'Instagram account'}
                className="w-8 h-8 rounded-full object-cover"
              />
              <div className="flex-1 min-w-0">
                <div className="text-sm font-semibold truncate">
                  {currentAccountName || 'No Instagram account'}
                </div>
              </div>
              <ChevronDown className="h-4 w-4 shrink-0 text-slate-400" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" side="top" className="w-64">
            <DropdownMenuLabel>Instagram accounts</DropdownMenuLabel>
            <DropdownMenuSeparator />
            {instagramAccounts.length === 0 && (
              <DropdownMenuItem disabled>
                <Instagram className="h-4 w-4" /> No account connected
              </DropdownMenuItem>
            )}
            {instagramAccounts.map(account => (
              <DropdownMenuItem
                key={account.id}
                onClick={() => switchInstagramAccount(account)}
                disabled={switchingAccount || account.active || account.isCurrent}
                className="cursor-pointer"
              >
                {account.profilePictureUrl ? (
                  <img
                    src={account.profilePictureUrl}
                    alt={account.username || 'Instagram account'}
                    className="h-5 w-5 rounded-full object-cover"
                  />
                ) : (
                  <Instagram className="h-4 w-4" />
                )}
                <span className="min-w-0 flex-1 truncate">
                  @{account.username || account.instagramAccountId}
                </span>
                {(account.active || account.isCurrent) && <Check className="h-4 w-4 text-emerald-600" />}
              </DropdownMenuItem>
            ))}
            <DropdownMenuSeparator />
            <DropdownMenuItem
              onClick={connectAnotherInstagramAccount}
              disabled={switchingAccount}
              className="cursor-pointer"
            >
              <Instagram className="h-4 w-4" /> Connect another account
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
        <Button variant="ghost" className="w-full justify-start text-slate-600" size="sm">
          <HelpCircle className="w-4 h-4 mr-2" /> Help & Support
        </Button>
        <Button onClick={logout} variant="ghost" className="w-full justify-start text-slate-600" size="sm">
          <LogOut className="w-4 h-4 mr-2" /> Log out
        </Button>
      </div>
    </aside>
  );
};

export default Sidebar;

import React, { useEffect, useState } from 'react';
import { NavLink, Link } from 'react-router-dom';
import {
  LayoutDashboard, Zap, Send, Settings,
  MessageCircle, HelpCircle, LogOut, AtSign, Inbox, ChevronDown, Check, Instagram, Activity,
  CreditCard,
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

export const navItems = [
  { to: '/app', end: true, icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/app/automations', icon: Zap, label: 'Automations' },
  { to: '/app/comments', icon: AtSign, label: 'Comments' },
  { to: '/app/dm-automation', icon: Inbox, label: 'DM Automation' },
  { to: '/app/broadcasting', icon: Send, label: 'Broadcasting' },
  { to: '/app/system-health', icon: Activity, label: 'System Health' },
  { to: '/app/billing', icon: CreditCard, label: 'Billing' },
  { to: '/app/settings', icon: Settings, label: 'Settings' }
];

const Sidebar = () => {
  const { logout, user, refreshUser } = useAuth();
  const [instagramAccounts, setInstagramAccounts] = useState([]);
  const [switchingAccount, setSwitchingAccount] = useState(false);

  useEffect(() => {
    let alive = true;
    const loadAccounts = async () => {
      try {
        const { data } = await api.get('/instagram/accounts');
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
  }, [user?.instagramConnected, user?.instagramHandle]);

  const currentAccount = instagramAccounts.find(account => account.active || account.isCurrent) || instagramAccounts[0];
  const currentAccountName = currentAccount?.username
    ? `@${currentAccount.username}`
    : user?.instagramHandle
      ? `@${String(user.instagramHandle).replace('@', '')}`
      : user?.name;
  const currentAccountAvatar = currentAccount?.profilePictureUrl || user?.instagramProfilePictureUrl || user?.avatar;

  const switchInstagramAccount = async (account) => {
    if (!account?.id || account.isCurrent || switchingAccount) return;
    setSwitchingAccount(true);
    try {
      await api.post(`/instagram/accounts/${account.id}/activate`);
      await refreshUser?.();
      toast.success(`Switched to @${account.username || account.instagramAccountId}`);
      window.location.assign(`/app?igAccount=${encodeURIComponent(account.id)}`);
    } catch (e) {
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
            className={({ isActive }) => `flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-colors ${isActive ? 'bg-slate-900 text-white' : 'text-slate-600 hover:bg-slate-100'}`}
          >
            <Icon className="w-4 h-4" />
            {label}
          </NavLink>
        ))}
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

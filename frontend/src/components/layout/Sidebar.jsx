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
import { toast } from 'sonner';
import { startInstagramConnect } from '../../lib/instagramConnect';
import { instagramApi } from '../../api/instagramApi';
import { ROUTES } from '../../constants/routes';
import { useIsAdmin } from '../../lib/useIsAdmin';
import { buildSupportMailtoHref, handleContactClick } from '../../lib/contactSupport';
import { useTranslation } from '../../lib/i18n';
import { cachedApiGetSWR, getCachedApiData, invalidateApiCache } from '../../lib/apiCache';
import { preloadRoute } from '../../lib/routePreloader';
import { scheduleCoreAppWarmup } from '../../lib/appWarmup';

// Labels are i18n keys (under `nav.*`) — Sidebar passes them through
// useTranslation when rendering. Old call sites that read `label`
// directly still work because the key falls back to the English
// string via the i18n module's missing-key fallback path.
export const navItems = [
  { to: ROUTES.APP_DASHBOARD, end: true, icon: LayoutDashboard, label: 'Dashboard', i18nKey: 'nav.dashboard' },
  { to: ROUTES.APP_AUTOMATIONS, icon: Zap, label: 'Automations', i18nKey: 'nav.automations' },
  { to: ROUTES.APP_DM_AUTOMATION, icon: Inbox, label: 'DM Automation', i18nKey: 'nav.dmAutomation' },
  { to: ROUTES.APP_BILLING, icon: CreditCard, label: 'Billing', i18nKey: 'nav.billing' },
  { to: ROUTES.APP_SETTINGS, icon: Settings, label: 'Settings', i18nKey: 'nav.settings' }
];

const hiddenRefreshStates = new Set([
  'disconnected',
  'auto_cleanup_users_disconnected',
  'auto_cleanup_single_account_plan',
  'force_disconnected_by_admin',
  'replaced_by_reconnect',
]);

export const isDisplayableInstagramAccount = (account) => Boolean(
  account
    && account.id
    && account.connectionValid === true
    && account.isActive !== false
    && !hiddenRefreshStates.has(account.refreshStatus)
    && (account.username || account.instagramAccountId || account.igUserId)
);

export const filterDisplayableInstagramAccounts = (payload) => (
  (payload?.accounts || []).filter(isDisplayableInstagramAccount)
);

const Sidebar = () => {
  const { logout, user, refreshUser } = useAuth();
  const { t, lang } = useTranslation();
  const ar = lang === 'ar';
  const navigate = useNavigate();
  const accountsCacheKey = `instagram-accounts:${user?.id || 'anon'}`;
  const [instagramAccounts, setInstagramAccounts] = useState(() => (
    user?.instagramConnected
      ? filterDisplayableInstagramAccounts(getCachedApiData(accountsCacheKey, { maxStaleMs: 10 * 60 * 1000 }))
      : []
  ));
  const [switchingAccount, setSwitchingAccount] = useState(false);
  const { isAdmin } = useIsAdmin();

  useEffect(() => {
    let alive = true;
    const loadAccounts = async () => {
      try {
        const result = await cachedApiGetSWR(
          accountsCacheKey,
          () => instagramApi.listAccounts(),
          {
            ttlMs: 180 * 1000,
            maxStaleMs: 10 * 60 * 1000,
            persist: true,
            onUpdate: (data) => {
              if (alive) setInstagramAccounts(filterDisplayableInstagramAccounts(data));
            },
          }
        );
        const data = result.data;
        if (alive) setInstagramAccounts(filterDisplayableInstagramAccounts(data));
      } catch {
        if (alive) setInstagramAccounts([]);
      }
    };
    if (!user?.instagramConnected) {
      setInstagramAccounts([]);
      invalidateApiCache(accountsCacheKey);
      return () => {
        alive = false;
      };
    }
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
    || (currentAccount ? user?.instagramProfilePictureUrl : null);
  const connectMode = instagramAccounts.length > 0
    ? 'add_account'
    : ((user?.instagramConnectionValid === false || user?.instagramHandle) ? 'reconnect' : 'connect');
  const connectLabel = instagramAccounts.length > 0
    ? (ar ? 'ربط حساب آخر' : 'Connect another account')
    : (connectMode === 'reconnect'
        ? (ar ? 'إعادة ربط Instagram' : 'Reconnect Instagram')
        : (ar ? 'ربط Instagram' : 'Connect Instagram'));

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
      const { data } = await instagramApi.activateAccount(account.id);
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
      toast.success(ar
        ? `تم التبديل إلى @${account.username || account.instagramAccountId}`
        : `Switched to @${account.username || account.instagramAccountId}`);
      navigate(`${ROUTES.APP}?igAccount=${encodeURIComponent(account.id)}`);
    } catch (e) {
      // Roll back the optimistic flip on failure so the dropdown
      // matches the server's view again.
      setInstagramAccounts((prev) => prev.map((a) => ({
        ...a,
        active: a.id === previousActive?.id,
        isCurrent: a.id === previousActive?.id,
      })));
      toast.error(e?.response?.data?.detail || (ar ? 'تعذّر تبديل حساب Instagram' : 'Failed to switch Instagram account'));
    }
    setSwitchingAccount(false);
  };

  const connectAnotherInstagramAccount = async (event) => {
    event?.preventDefault?.();
    event?.stopPropagation?.();
    setSwitchingAccount(true);
    try {
      // Phase 2.19 UX fix: route the OAuth return through Settings →
      // Instagram tab so its useEffect can pick up ?ig=error&reason=…
      // and surface a translated toast. Returning to /app (Dashboard)
      // dropped the error silently because Dashboard has no handler
      // for those params.
      await startInstagramConnect({ mode: connectMode, returnTo: '/app/settings?tab=instagram' });
    } catch (e) {
      toast.error(e?.response?.data?.detail || e?.message || (ar ? 'تعذّر بدء ربط Instagram' : 'Failed to start Instagram connection'));
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
      <nav className="flex-1 p-4 space-y-1" aria-label={ar ? 'القائمة الرئيسية' : 'Main navigation'}>
        {navItems.map(({ to, end, icon: Icon, label, i18nKey }) => (
          <NavLink
            key={to}
            to={to}
            end={end}
            onMouseEnter={() => preloadRoute(label)}
            onFocus={() => preloadRoute(label)}
            className={({ isActive }) => `flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-colors ${isActive ? 'bg-slate-900 text-white' : 'text-slate-600 hover:bg-slate-100'}`}
          >
            <Icon className="w-4 h-4" />
            {i18nKey ? t(i18nKey, label) : label}
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
            {t('nav.admin')}
          </NavLink>
        )}
      </nav>
      <div className="p-4 border-t border-slate-100 space-y-2">
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="flex w-full items-center gap-3 rounded-xl bg-slate-50 px-3 py-2 text-start transition hover:bg-slate-100">
              {currentAccountAvatar ? (
                <img
                  src={currentAccountAvatar}
                  alt={currentAccountName || (ar ? 'حساب Instagram' : 'Instagram account')}
                  loading="lazy"
                  referrerPolicy="strict-origin-when-cross-origin"
                  className="w-8 h-8 rounded-full object-cover"
                />
              ) : (
                <div className="flex h-8 w-8 items-center justify-center rounded-full bg-slate-100 text-slate-500">
                  <Instagram className="h-4 w-4" />
                </div>
              )}
              <div className="flex-1 min-w-0">
                <div className="text-sm font-semibold truncate">
                  {currentAccountName || (ar ? 'لا يوجد حساب Instagram' : 'No Instagram account')}
                </div>
              </div>
              <ChevronDown className="h-4 w-4 shrink-0 text-slate-400" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" side="top" className="w-64">
            <DropdownMenuLabel>{ar ? 'حسابات Instagram' : 'Instagram accounts'}</DropdownMenuLabel>
            <DropdownMenuSeparator />
            {instagramAccounts.length === 0 && (
              <DropdownMenuItem disabled>
                <Instagram className="h-4 w-4" /> {ar ? 'لا يوجد حساب مربوط' : 'No account connected'}
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
                    alt={account.username || (ar ? 'حساب Instagram' : 'Instagram account')}
                    loading="lazy"
                    referrerPolicy="strict-origin-when-cross-origin"
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
              <Instagram className="h-4 w-4" /> {connectLabel}
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
        <Button
          asChild
          variant="ghost"
          className="w-full justify-start text-slate-600"
          size="sm"
        >
          <a
            href={buildSupportMailtoHref()}
            onClick={handleContactClick}
            rel="noopener noreferrer"
          >
            <HelpCircle className="w-4 h-4 me-2" /> {t('nav.helpSupport')}
          </a>
        </Button>
        <Button onClick={logout} variant="ghost" className="w-full justify-start text-slate-600" size="sm">
          <LogOut className="w-4 h-4 me-2" /> {t('common.logout')}
        </Button>
      </div>
    </aside>
  );
};

export default Sidebar;

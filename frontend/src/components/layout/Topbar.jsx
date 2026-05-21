import React from 'react';
import { Link } from 'react-router-dom';
import { Plus, Instagram } from 'lucide-react';
import { Button } from '../ui/button';
import { Badge } from '../ui/badge';
import { useAuth } from '../../context/AuthContext';
import { useTranslation } from '../../lib/i18n';
import LangSwitcher from '../LangSwitcher';
import { ROUTES } from '../../constants/routes';

// Phase 2.18S: Topbar cleanup.
// Removed two dead UI elements that confused users in the live tester
// pass:
//   - "Search contacts, automations…" input box. It had no handler and
//     no real search backend; typing did nothing. Promising a search
//     and not delivering one is worse than not promising it at all.
//   - Bell notification button. It rendered a pink dot indicator at
//     all times but had no onClick, no notifications panel, and no
//     notifications API. The dot kept implying there was something
//     new to read.
// Both will come back when there is an actual feature behind them.

const Topbar = () => {
  const { user } = useAuth();
  const { t, lang } = useTranslation();
  const notConnectedLabel = t('topbar.notConnected') || 'Not connected';
  const instagramConnected = Boolean(user?.instagramConnected && user?.instagramConnectionValid);
  const hasKnownInstagramIdentity = Boolean(
    user?.instagramHandle
      || user?.activeInstagramAccountId
      || user?.activeInstagramIgUserId
      || user?.ig_user_id
  );
  const instagramNeedsReconnect = !instagramConnected && Boolean(
    user?.instagramConnected
      || user?.instagramConnectionValid === false
      || hasKnownInstagramIdentity
  );
  const instagramStatus = instagramConnected
    ? {
        label: t('topbar.connected'),
        className: 'bg-emerald-50 text-emerald-700 border-emerald-100',
      }
    : instagramNeedsReconnect
      ? {
          label: lang === 'ar' ? 'أعد الربط' : 'Reconnect',
          className: 'bg-amber-50 text-amber-700 border-amber-100',
        }
      : {
          label: notConnectedLabel,
          className: 'bg-slate-50 text-slate-600 border-slate-200',
        };

  return (
    <header className="hidden md:flex h-16 bg-white border-b border-slate-200 px-6 items-center justify-end topbar-shadow shrink-0">
      <div className="flex items-center gap-3">
        <LangSwitcher />
        <Badge
          className={`${instagramStatus.className} rounded-full hidden md:flex items-center gap-1`}
          data-testid="topbar-instagram-status"
        >
          <Instagram className="w-3 h-3" />
          {instagramStatus.label}
        </Badge>
        <Button className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl h-10" asChild>
          <Link to={ROUTES.APP_AUTOMATIONS}>
            <Plus className="w-4 h-4 me-1.5" /> {t('topbar.newAutomation')}
          </Link>
        </Button>
      </div>
    </header>
  );
};

export default Topbar;

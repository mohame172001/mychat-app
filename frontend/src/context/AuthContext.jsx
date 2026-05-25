import React, { createContext, useContext, useState, useEffect } from 'react';
import api from '../lib/api';
import analytics from '../lib/analytics';
import { clearApiCache, seedApiCacheEntry } from '../lib/apiCache';
import { scheduleCoreAppWarmup } from '../lib/appWarmup';

const AuthContext = createContext(null);

// Phase 2.18I: cache-key constants used by all consumers (Dashboard,
// Automations, Sidebar, AdminConsole) and by the bootstrap-response
// seeder below. Anything that wants to read a snapshot uses the same
// scheme as the place that writes it.
// Phase 2.18Y cold-start fix: extend maxStaleMs windows so the
// localStorage snapshot is still eligible for stale-while-revalidate
// render after a full day away. The TTL controls when a background
// refresh fires; the maxStaleMs controls when we give up on the
// snapshot entirely and force a blocking loading state. Keeping these
// generous means the user always sees their last-known data instantly,
// and the refresh updates in the background.
const DASHBOARD_TTL_MS = 60 * 1000;
const DASHBOARD_MAX_STALE_MS = 24 * 60 * 60 * 1000;
const DEFAULT_DASHBOARD_RANGE = '7d';
const AUTOMATIONS_TTL_MS = 90 * 1000;
const AUTOMATIONS_MAX_STALE_MS = 24 * 60 * 60 * 1000;
const ACCOUNTS_TTL_MS = 180 * 1000;
const ACCOUNTS_MAX_STALE_MS = 24 * 60 * 60 * 1000;
const ADMIN_TTL_MS = 60 * 1000;
const ADMIN_MAX_STALE_MS = 24 * 60 * 60 * 1000;

function activeAccountKey(user) {
  return user?.activeInstagramAccountId || user?.activeInstagramIgUserId || 'active';
}

/**
 * Seed the in-memory + persistent SWR cache from a single
 * /auth/bootstrap response so all four core pages (Dashboard,
 * Automations, Sidebar accounts, Admin overview) can render from a
 * hot cache the moment the user navigates to them — no second
 * round-trip after login or refresh.
 */
function seedFromBootstrap(bootstrap, user) {
  if (!bootstrap || typeof bootstrap !== 'object' || !user?.id) return;
  const accountKey = activeAccountKey(user);
  if (bootstrap.dashboard_summary && !bootstrap.dashboard_summary.error) {
    seedApiCacheEntry(
      `dashboard-summary:${user.id}:${accountKey}:${DEFAULT_DASHBOARD_RANGE}`,
      bootstrap.dashboard_summary,
      { persist: true, ttlMs: DASHBOARD_TTL_MS, maxStaleMs: DASHBOARD_MAX_STALE_MS },
    );
  }
  if (bootstrap.automations_summary && !bootstrap.automations_summary.error) {
    seedApiCacheEntry(
      `automations-summary:${user.id}:${accountKey}`,
      bootstrap.automations_summary,
      { persist: true, ttlMs: AUTOMATIONS_TTL_MS, maxStaleMs: AUTOMATIONS_MAX_STALE_MS },
    );
  }
  if (bootstrap.instagram_accounts && !bootstrap.instagram_accounts.error) {
    seedApiCacheEntry(
      `instagram-accounts:${user.id}`,
      bootstrap.instagram_accounts,
      { persist: true, ttlMs: ACCOUNTS_TTL_MS, maxStaleMs: ACCOUNTS_MAX_STALE_MS },
    );
  }
  if (bootstrap.isAdmin && bootstrap.admin_overview && !bootstrap.admin_overview.error) {
    seedApiCacheEntry(
      `admin:overview:${user.id}`,
      bootstrap.admin_overview,
      { persist: true, ttlMs: ADMIN_TTL_MS, maxStaleMs: ADMIN_MAX_STALE_MS },
    );
  }
}

async function fetchBootstrap(currentUser) {
  try {
    const { data } = await api.get('/auth/bootstrap');
    if (data?.user) {
      try { localStorage.setItem('mychat_user', JSON.stringify(data.user)); } catch (_) {}
      try { analytics.identify({ id: data.user.id }); } catch (_) {}
    }
    seedFromBootstrap(data, data?.user || currentUser);
    return data;
  } catch (err) {
    console.warn('[Auth] /auth/bootstrap failed:', err?.response?.status);
    return null;
  }
}

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const init = async () => {
      const token = localStorage.getItem('mychat_token');
      const stored = localStorage.getItem('mychat_user');
      let restoredUser = null;
      if (token && stored) {
        try {
          const parsed = JSON.parse(stored);
          restoredUser = parsed;
          setUser(parsed);
          setLoading(false);
          analytics.identify({ id: parsed.id });
          // Kick off bootstrap immediately so /dashboard/summary,
          // /automations/summary, /instagram/accounts (and /admin/overview
          // when applicable) all arrive in ONE round-trip. The page can
          // render the cached snapshot synchronously while the fresh
          // bootstrap response replaces it the moment it lands.
          scheduleCoreAppWarmup(parsed);
        } catch (err) {
          console.error('[Auth] Failed to parse stored user:', err);
          localStorage.removeItem('mychat_user');
          clearApiCache();
        }
        const data = await fetchBootstrap(restoredUser);
        if (data?.user) setUser(data.user);
      }
      if (!restoredUser) setLoading(false);
    };
    init();
  }, []);

  const login = async (username, password) => {
    const { data } = await api.post('/auth/login', { username, password });
    localStorage.setItem('mychat_token', data.token);
    localStorage.setItem('mychat_user', JSON.stringify(data.user));
    clearApiCache();
    setUser(data.user);
    // Race the bootstrap in parallel with the React route transition
    // so by the time the dashboard mounts, its snapshot is already
    // in the cache. Don't await — the user sees /app immediately.
    fetchBootstrap(data.user).then((b) => {
      if (b?.user) setUser(b.user);
    });
    scheduleCoreAppWarmup(data.user);
    analytics.identify({ id: data.user.id });
    analytics.capture('login_completed', {});
    return data.user;
  };

  const signup = async (username, email, password) => {
    const { data } = await api.post('/auth/signup', { username, email, password });
    localStorage.setItem('mychat_token', data.token);
    localStorage.setItem('mychat_user', JSON.stringify(data.user));
    clearApiCache();
    setUser(data.user);
    fetchBootstrap(data.user).then((b) => {
      if (b?.user) setUser(b.user);
    });
    scheduleCoreAppWarmup(data.user);
    analytics.identify({ id: data.user.id });
    analytics.capture('signup_completed', {});
    return data.user;
  };

  /**
   * Phase 2.7: Google Sign-In. Sends the Google ID token to the backend,
   * which decides login vs signup vs link based on existing google_sub /
   * email. The credential JWT is forwarded once and never logged.
   */
  const loginWithGoogle = async (credential) => {
    if (!credential) throw new Error('missing_google_credential');
    const { data } = await api.post('/auth/google', { credential });
    localStorage.setItem('mychat_token', data.token);
    localStorage.setItem('mychat_user', JSON.stringify(data.user));
    clearApiCache();
    setUser(data.user);
    fetchBootstrap(data.user).then((b) => {
      if (b?.user) setUser(b.user);
    });
    scheduleCoreAppWarmup(data.user);
    analytics.identify({ id: data.user.id });
    analytics.capture('login_completed', { method: 'google' });
    return data.user;
  };

  const logout = () => {
    localStorage.removeItem('mychat_token');
    localStorage.removeItem('mychat_user');
    clearApiCache();
    setUser(null);
    analytics.reset();
  };

  const refreshUser = async () => {
    const data = await fetchBootstrap(user);
    if (data?.user) {
      setUser(data.user);
      return data.user;
    }
    return null;
  };

  return (
    <AuthContext.Provider value={{
      user, login, signup, logout, loading, refreshUser, loginWithGoogle,
    }}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
};

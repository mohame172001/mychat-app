import React, { createContext, useContext, useState, useEffect } from 'react';
import api from '../lib/api';
import analytics from '../lib/analytics';
import { clearApiCache } from '../lib/apiCache';
import { scheduleCoreAppWarmup } from '../lib/appWarmup';

const AuthContext = createContext(null);

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
          scheduleCoreAppWarmup(parsed);
          // Restore analytics identity for an already-authenticated session.
          analytics.identify({ id: parsed.id });
        } catch (err) {
          console.error('[Auth] Failed to parse stored user:', err);
          localStorage.removeItem('mychat_user');
          clearApiCache();
        }
        try {
          const { data } = await api.get('/auth/me');
          setUser(data);
          localStorage.setItem('mychat_user', JSON.stringify(data));
          analytics.identify({ id: data.id });
          scheduleCoreAppWarmup(data);
        } catch (err) {
          // Non-fatal: token may be expired; api interceptor handles redirect.
          console.warn('[Auth] /auth/me refresh failed:', err?.response?.status);
        }
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
    scheduleCoreAppWarmup(data.user);
    // Phase 2.5 funnel events. analytics no-ops when PostHog is disabled.
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
    try {
      const { data } = await api.get('/auth/me');
      setUser(data);
      localStorage.setItem('mychat_user', JSON.stringify(data));
      scheduleCoreAppWarmup(data);
      return data;
    } catch (err) {
      console.warn('[Auth] refreshUser failed:', err?.response?.status);
    }
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

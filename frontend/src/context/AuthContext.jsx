import React, { createContext, useContext, useState, useEffect } from 'react';
import api from '../lib/api';

const AuthContext = createContext(null);

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const init = async () => {
      const token = localStorage.getItem('mychat_token');
      const stored = localStorage.getItem('mychat_user');
      if (token && stored) {
        try {
          setUser(JSON.parse(stored));
        } catch (err) {
          console.error('[Auth] Failed to parse stored user:', err);
          localStorage.removeItem('mychat_user');
        }
        try {
          const { data } = await api.get('/auth/me');
          setUser(data);
          localStorage.setItem('mychat_user', JSON.stringify(data));
        } catch (err) {
          // Non-fatal: token may be expired; api interceptor handles redirect.
          console.warn('[Auth] /auth/me refresh failed:', err?.response?.status);
        }
      }
      setLoading(false);
    };
    init();
  }, []);

  const login = async (username, password) => {
    const { data } = await api.post('/auth/login', { username, password });
    localStorage.setItem('mychat_token', data.token);
    localStorage.setItem('mychat_user', JSON.stringify(data.user));
    setUser(data.user);
    return data.user;
  };

  const signup = async (username, email, password) => {
    const { data } = await api.post('/auth/signup', { username, email, password });
    localStorage.setItem('mychat_token', data.token);
    localStorage.setItem('mychat_user', JSON.stringify(data.user));
    setUser(data.user);
    return data.user;
  };

  const logout = () => {
    localStorage.removeItem('mychat_token');
    localStorage.removeItem('mychat_user');
    setUser(null);
  };

  const refreshUser = async () => {
    try {
      const { data } = await api.get('/auth/me');
      setUser(data);
      localStorage.setItem('mychat_user', JSON.stringify(data));
      return data;
    } catch (err) {
      console.warn('[Auth] refreshUser failed:', err?.response?.status);
    }
  };

  return (
    <AuthContext.Provider value={{ user, login, signup, logout, loading, refreshUser }}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
};

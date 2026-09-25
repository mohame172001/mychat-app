import { useEffect, useState } from 'react';
import api from './api';
import { useAuth } from '../context/AuthContext';

/**
 * Phase 2.4: small hook that probes /api/admin/me and exposes
 * is_admin without ever raising. Used by Sidebar + DashboardLayout to
 * conditionally render the Admin nav entry.
 *
 * Deduplicate per session for one minute. Never cache network failures
 * or allow a previous session's response to populate the current cache.
 */
let _adminMePromise = null;
let _adminMeData = null;
let _session = null;
let _expiresAt = 0;

export function fetchAdminMe() {
  const session = localStorage.getItem('mychat_token');
  if (_session !== session) {
    clearAdminMeCacheForTests();
    _session = session;
  }
  if (!session) return Promise.resolve({ is_admin: false });
  if (Date.now() >= _expiresAt) _adminMeData = null;
  if (_adminMeData) return Promise.resolve(_adminMeData);
  if (_adminMePromise) return _adminMePromise;
  const pending = api.get('/admin/me')
    .then(({ data }) => {
      const result = data || { is_admin: false };
      if (_session === session && _adminMePromise === pending) {
        _adminMeData = result;
        _expiresAt = Date.now() + 60000;
      }
      return result;
    })
    .catch(() => {
      return { is_admin: false };
    })
    .finally(() => {
      if (_adminMePromise === pending) _adminMePromise = null;
    });
  _adminMePromise = pending;
  return _adminMePromise;
}

export function clearAdminMeCacheForTests() {
  _adminMePromise = null;
  _adminMeData = null;
  _session = null;
  _expiresAt = 0;
}

export function useIsAdmin() {
  const { user } = useAuth();
  const [result, setResult] = useState({ userId: null, isAdmin: false, loaded: false });
  useEffect(() => {
    let alive = true;
    const refresh = () => fetchAdminMe().then((data) => {
      if (!alive) return;
      setResult({ userId: user?.id, isAdmin: Boolean(data?.is_admin), loaded: true });
    });
    refresh();
    window.addEventListener('focus', refresh);
    return () => { alive = false; window.removeEventListener('focus', refresh); };
  }, [user?.id]);
  return result.userId === user?.id ? result : { isAdmin: false, loaded: false };
}

import { useEffect, useState } from 'react';
import api from './api';

/**
 * Phase 2.4: small hook that probes /api/admin/me and exposes
 * is_admin without ever raising. Used by Sidebar + DashboardLayout to
 * conditionally render the Admin nav entry.
 *
 * Phase 2.18Y: module-level dedup. Three components on the admin page
 * (Sidebar, DashboardLayout, AdminConsole) each used to fire their
 * own /admin/me on mount, paying 3× the latency. The cached promise
 * here returns the same in-flight (or resolved) result to every
 * caller until the page reloads.
 */
let _adminMePromise = null;
let _adminMeData = null;
let _generation = 0;
const listeners = new Set();

export function fetchAdminMe() {
  if (_adminMeData) return Promise.resolve(_adminMeData);
  if (_adminMePromise) return _adminMePromise;
  const generation = _generation;
  _adminMePromise = api.get('/admin/me')
    .then(({ data }) => {
      if (generation !== _generation) return { is_admin: false };
      _adminMeData = data || { is_admin: false };
      return _adminMeData;
    })
    .catch(() => {
      if (generation !== _generation) return { is_admin: false };
      _adminMeData = { is_admin: false };
      return _adminMeData;
    })
    .finally(() => {
      if (generation === _generation) _adminMePromise = null;
    });
  return _adminMePromise;
}

export function clearAdminMeCache() {
  _generation += 1;
  _adminMePromise = null;
  _adminMeData = null;
  listeners.forEach((listener) => listener());
}

export const clearAdminMeCacheForTests = clearAdminMeCache;

export function useIsAdmin() {
  const [isAdmin, setIsAdmin] = useState(Boolean(_adminMeData?.is_admin));
  const [loaded, setLoaded] = useState(Boolean(_adminMeData));
  useEffect(() => {
    let alive = true;
    const refresh = () => {
      const generation = _generation;
      setIsAdmin(false);
      setLoaded(false);
      fetchAdminMe().then((data) => {
        if (!alive || generation !== _generation) return;
        setIsAdmin(Boolean(data?.is_admin));
        setLoaded(true);
      });
    };
    listeners.add(refresh);
    refresh();
    return () => { alive = false; listeners.delete(refresh); };
  }, []);
  return { isAdmin, loaded };
}

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

export function fetchAdminMe() {
  if (_adminMeData) return Promise.resolve(_adminMeData);
  if (_adminMePromise) return _adminMePromise;
  _adminMePromise = api.get('/admin/me')
    .then(({ data }) => {
      _adminMeData = data || { is_admin: false };
      return _adminMeData;
    })
    .catch(() => {
      _adminMeData = { is_admin: false };
      return _adminMeData;
    })
    .finally(() => {
      _adminMePromise = null;
    });
  return _adminMePromise;
}

export function clearAdminMeCacheForTests() {
  _adminMePromise = null;
  _adminMeData = null;
}

export function useIsAdmin() {
  const [isAdmin, setIsAdmin] = useState(Boolean(_adminMeData?.is_admin));
  const [loaded, setLoaded] = useState(Boolean(_adminMeData));
  useEffect(() => {
    let alive = true;
    fetchAdminMe().then((data) => {
      if (!alive) return;
      setIsAdmin(Boolean(data?.is_admin));
      setLoaded(true);
    });
    return () => { alive = false; };
  }, []);
  return { isAdmin, loaded };
}

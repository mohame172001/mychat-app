import { useEffect, useState } from 'react';
import api from './api';

/**
 * Phase 2.4: small hook that probes /api/admin/me and exposes
 * is_admin without ever raising. Used by Sidebar + DashboardLayout to
 * conditionally render the Admin nav entry.
 */
export function useIsAdmin() {
  const [isAdmin, setIsAdmin] = useState(false);
  const [loaded, setLoaded] = useState(false);
  useEffect(() => {
    let alive = true;
    api.get('/admin/me')
      .then(({ data }) => {
        if (!alive) return;
        setIsAdmin(Boolean(data?.is_admin));
        setLoaded(true);
      })
      .catch(() => {
        if (!alive) return;
        setIsAdmin(false);
        setLoaded(true);
      });
    return () => { alive = false; };
  }, []);
  return { isAdmin, loaded };
}

import React from 'react';
import { NavLink, Outlet, Link } from 'react-router-dom';
import { LogOut, MessageCircle } from 'lucide-react';
import Sidebar, { navItems } from './Sidebar';
import Topbar from './Topbar';
import { useAuth } from '../../context/AuthContext';
import { Button } from '../ui/button';
import { BUILD_SHA } from '../../buildInfo.generated';

const DashboardLayout = () => {
  const { logout } = useAuth();

  return (
    <div className="h-[100dvh] flex bg-slate-50 overflow-hidden">
      <Sidebar />
      <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        <div className="md:hidden shrink-0 border-b border-slate-200 bg-white">
          <div className="h-14 px-4 flex items-center justify-between">
            <Link to="/app" className="flex items-center gap-2 min-w-0">
              <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
                <MessageCircle className="w-5 h-5 text-white" strokeWidth={2.5} />
              </div>
              <span className="text-lg font-bold font-display truncate">mychat</span>
            </Link>
            <Button onClick={logout} variant="ghost" size="icon" className="rounded-full" aria-label="Log out">
              <LogOut className="w-4 h-4" />
            </Button>
          </div>
          <nav className="mobile-nav-scroll flex gap-2 overflow-x-auto px-3 pb-3">
            {navItems.map(({ to, end, icon: Icon, label }) => (
              <NavLink
                key={to}
                to={to}
                end={end}
                className={({ isActive }) => `shrink-0 inline-flex items-center gap-2 rounded-full px-3 py-2 text-xs font-semibold transition-colors ${
                  isActive ? 'bg-slate-900 text-white' : 'bg-slate-100 text-slate-600'
                }`}
              >
                <Icon className="w-4 h-4" />
                {label}
              </NavLink>
            ))}
          </nav>
        </div>
        <Topbar />
        <main className="flex-1 overflow-y-auto pb-4 md:pb-0">
          <Outlet />
        </main>
        <footer className="border-t border-slate-200 bg-white px-4 py-3 md:px-6 flex flex-col sm:flex-row items-center justify-between gap-2 text-xs text-slate-500 shrink-0">
          <span>© 2026 mychat · Build: {BUILD_SHA}</span>
          <div className="flex gap-4 md:gap-5">
            <Link to="/privacy" className="hover:text-slate-900" target="_blank" rel="noreferrer">Privacy</Link>
            <Link to="/terms" className="hover:text-slate-900" target="_blank" rel="noreferrer">Terms</Link>
            <a href="mailto:mm.mohame172000@gmail.com" className="hover:text-slate-900">Contact</a>
          </div>
        </footer>
      </div>
    </div>
  );
};

export default DashboardLayout;

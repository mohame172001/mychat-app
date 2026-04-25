import React from 'react';
import { Outlet, Link } from 'react-router-dom';
import Sidebar from './Sidebar';
import Topbar from './Topbar';

const DashboardLayout = () => {
  return (
    <div className="h-screen flex bg-slate-50 overflow-hidden">
      <Sidebar />
      <div className="flex-1 flex flex-col overflow-hidden">
        <Topbar />
        <main className="flex-1 overflow-y-auto">
          <Outlet />
        </main>
        <footer className="border-t border-slate-200 bg-white px-6 py-3 flex items-center justify-between text-xs text-slate-500">
          <span>© 2026 mychat</span>
          <div className="flex gap-5">
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

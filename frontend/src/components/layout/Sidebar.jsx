import React from 'react';
import { NavLink, Link } from 'react-router-dom';
import {
  LayoutDashboard, Zap, Send, Settings,
  MessageCircle, HelpCircle, LogOut, AtSign, Inbox
} from 'lucide-react';
import { useAuth } from '../../context/AuthContext';
import { Button } from '../ui/button';

export const navItems = [
  { to: '/app', end: true, icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/app/automations', icon: Zap, label: 'Automations' },
  { to: '/app/comments', icon: AtSign, label: 'Comments' },
  { to: '/app/dm-automation', icon: Inbox, label: 'DM Automation' },
  { to: '/app/broadcasting', icon: Send, label: 'Broadcasting' },
  { to: '/app/settings', icon: Settings, label: 'Settings' }
];

const Sidebar = () => {
  const { logout, user } = useAuth();
  return (
    <aside className="hidden md:flex w-64 flex-col bg-white border-r border-slate-200">
      <Link to="/app" className="h-16 px-6 flex items-center gap-2 border-b border-slate-100">
        <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
          <MessageCircle className="w-5 h-5 text-white" strokeWidth={2.5} />
        </div>
        <span className="text-xl font-bold font-display">mychat</span>
      </Link>
      <nav className="flex-1 p-4 space-y-1">
        {navItems.map(({ to, end, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            end={end}
            className={({ isActive }) => `flex items-center gap-3 px-3 py-2.5 rounded-xl text-sm font-medium transition-colors ${isActive ? 'bg-slate-900 text-white' : 'text-slate-600 hover:bg-slate-100'}`}
          >
            <Icon className="w-4 h-4" />
            {label}
          </NavLink>
        ))}
      </nav>
      <div className="p-4 border-t border-slate-100 space-y-2">
        <div className="flex items-center gap-3 px-3 py-2 rounded-xl bg-slate-50">
          <img src={user?.avatar} alt={user?.name} className="w-8 h-8 rounded-full object-cover" />
          <div className="flex-1 min-w-0">
            <div className="text-sm font-semibold truncate">{user?.name}</div>
            <div className="text-xs text-slate-500 truncate">{user?.instagramHandle}</div>
          </div>
        </div>
        <Button variant="ghost" className="w-full justify-start text-slate-600" size="sm">
          <HelpCircle className="w-4 h-4 mr-2" /> Help & Support
        </Button>
        <Button onClick={logout} variant="ghost" className="w-full justify-start text-slate-600" size="sm">
          <LogOut className="w-4 h-4 mr-2" /> Log out
        </Button>
      </div>
    </aside>
  );
};

export default Sidebar;

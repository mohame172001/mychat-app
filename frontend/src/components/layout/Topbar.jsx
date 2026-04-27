import React from 'react';
import { Link } from 'react-router-dom';
import { Search, Bell, Plus, Instagram } from 'lucide-react';
import { Input } from '../ui/input';
import { Button } from '../ui/button';
import { Badge } from '../ui/badge';

const Topbar = () => {
  return (
    <header className="hidden md:flex h-16 bg-white border-b border-slate-200 px-6 items-center justify-between topbar-shadow shrink-0">
      <div className="relative max-w-md w-full min-w-0">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
        <Input placeholder="Search contacts, automations..." className="pl-9 h-10 rounded-xl bg-slate-50 border-slate-100" />
      </div>
      <div className="flex items-center gap-2">
        <Badge className="bg-emerald-50 text-emerald-700 border-emerald-100 rounded-full hidden md:flex items-center gap-1">
          <Instagram className="w-3 h-3" />
          Connected
        </Badge>
        <Button variant="ghost" size="icon" className="relative rounded-full">
          <Bell className="w-4 h-4" />
          <span className="absolute top-2 right-2 w-2 h-2 rounded-full bg-pink-500" />
        </Button>
        <Button className="bg-slate-900 hover:bg-slate-800 text-white rounded-xl h-10" asChild>
          <Link to="/app/automations">
            <Plus className="w-4 h-4 mr-1.5" /> New Automation
          </Link>
        </Button>
      </div>
    </header>
  );
};

export default Topbar;

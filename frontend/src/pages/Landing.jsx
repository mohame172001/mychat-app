import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import {
  ArrowRight, BarChart3, Bot, ChevronRight, Instagram, Menu,
  MessageCircle, Sparkles, Target, Users, X, Zap,
} from 'lucide-react';

const iconMap = { MessageCircle, Zap, Users, BarChart3, Bot, Target };

const features = [
  {
    icon: 'MessageCircle',
    title: 'Instagram DM Automation',
    description: 'Create replies for comments and direct messages using the Instagram connection on your account.',
    color: 'from-pink-500 to-orange-400',
  },
  {
    icon: 'Zap',
    title: 'Comment Triggers',
    description: 'Trigger a flow when a new comment contains your selected word or phrase.',
    color: 'from-blue-500 to-cyan-400',
  },
  {
    icon: 'Users',
    title: 'Real Contacts',
    description: 'Work with contacts created from your own connected Instagram activity.',
    color: 'from-purple-500 to-pink-400',
  },
  {
    icon: 'BarChart3',
    title: 'Dashboard',
    description: 'Review the activity stored in your account without demo numbers or seeded records.',
    color: 'from-emerald-500 to-teal-400',
  },
  {
    icon: 'Bot',
    title: 'Rule-Based Replies',
    description: 'Define exactly what the automation should send for each keyword rule.',
    color: 'from-indigo-500 to-blue-400',
  },
  {
    icon: 'Target',
    title: 'Account Scoped',
    description: 'Automations run against the Instagram account and post you select.',
    color: 'from-amber-500 to-orange-400',
  },
];

const Landing = () => {
  const [menuOpen, setMenuOpen] = useState(false);

  return (
    <div className="min-h-screen bg-white text-slate-900 overflow-x-hidden">
      <nav className="fixed top-0 inset-x-0 z-50 backdrop-blur-xl bg-white/80 border-b border-slate-100">
        <div className="max-w-7xl mx-auto px-6 h-16 flex items-center justify-between">
          <Link to="/" className="flex items-center gap-2">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
              <MessageCircle className="w-5 h-5 text-white" strokeWidth={2.5} />
            </div>
            <span className="text-xl font-bold font-display tracking-tight">mychat</span>
          </Link>
          <div className="hidden md:flex items-center gap-8">
            <a href="#features" className="text-sm font-medium text-slate-600 hover:text-slate-900 transition-colors">Features</a>
            <a href="#how" className="text-sm font-medium text-slate-600 hover:text-slate-900 transition-colors">How it works</a>
            <Link to="/privacy" className="text-sm font-medium text-slate-600 hover:text-slate-900 transition-colors">Privacy</Link>
            <Link to="/terms" className="text-sm font-medium text-slate-600 hover:text-slate-900 transition-colors">Terms</Link>
          </div>
          <div className="hidden md:flex items-center gap-3">
            <Link to="/login"><Button variant="ghost" className="text-slate-700">Log in</Button></Link>
            <Link to="/signup">
              <Button className="bg-slate-900 hover:bg-slate-800 text-white rounded-full px-5">
                Get Started
              </Button>
            </Link>
          </div>
          <button className="md:hidden" onClick={() => setMenuOpen(!menuOpen)}>
            {menuOpen ? <X className="w-6 h-6" /> : <Menu className="w-6 h-6" />}
          </button>
        </div>
        {menuOpen && (
          <div className="md:hidden border-t border-slate-100 bg-white">
            <div className="px-6 py-4 flex flex-col gap-4">
              <a href="#features" onClick={() => setMenuOpen(false)} className="text-sm font-medium">Features</a>
              <a href="#how" onClick={() => setMenuOpen(false)} className="text-sm font-medium">How it works</a>
              <Link to="/privacy" onClick={() => setMenuOpen(false)} className="text-sm font-medium">Privacy</Link>
              <Link to="/terms" onClick={() => setMenuOpen(false)} className="text-sm font-medium">Terms</Link>
              <Link to="/login"><Button variant="outline" className="w-full">Log in</Button></Link>
              <Link to="/signup"><Button className="w-full bg-slate-900 text-white">Get Started</Button></Link>
            </div>
          </div>
        )}
      </nav>

      <section className="relative pt-32 pb-20 px-6">
        <div className="max-w-6xl mx-auto relative">
          <div className="text-center animate-fade-up">
            <Badge className="bg-blue-50 text-blue-700 hover:bg-blue-50 border-blue-100 rounded-full px-4 py-1.5 mb-6">
              <Sparkles className="w-3.5 h-3.5 mr-1.5" />
              Instagram automation for connected business accounts
            </Badge>
            <h1 className="font-display text-5xl sm:text-6xl md:text-7xl font-extrabold leading-[1.05] tracking-tight">
              Turn Instagram <br />
              conversations into <span className="gradient-text">workflows.</span>
            </h1>
            <p className="mt-6 text-lg md:text-xl text-slate-600 max-w-2xl mx-auto">
              Connect your Instagram account, create comment and DM rules, and manage automations from one dashboard.
            </p>
            <div className="mt-10 flex items-center justify-center">
              <Link to="/signup">
                <Button size="lg" className="bg-slate-900 hover:bg-slate-800 text-white rounded-full px-8 h-14 text-base">
                  Get Started <ArrowRight className="ml-2 w-4 h-4" />
                </Button>
              </Link>
            </div>
          </div>

          <div className="mt-16 relative animate-fade-up" style={{ animationDelay: '0.2s' }}>
            <div className="relative rounded-3xl overflow-hidden shadow-2xl border border-slate-200 bg-white">
              <div className="h-10 bg-slate-50 border-b border-slate-100 flex items-center px-4 gap-1.5">
                <div className="w-3 h-3 rounded-full bg-red-400" />
                <div className="w-3 h-3 rounded-full bg-amber-400" />
                <div className="w-3 h-3 rounded-full bg-green-400" />
              </div>
              <div className="grid md:grid-cols-[240px_1fr] min-h-[400px]">
                <div className="bg-slate-50 border-r border-slate-100 p-4 space-y-1">
                  {['Dashboard', 'Automations', 'Contacts', 'DM Automation', 'Broadcasting'].map((item, i) => (
                    <div key={item} className={`px-3 py-2 rounded-lg text-sm font-medium ${i === 1 ? 'bg-blue-50 text-blue-700' : 'text-slate-600'}`}>{item}</div>
                  ))}
                </div>
                <div className="p-6 flow-grid">
                  <div className="flex gap-6 flex-wrap">
                    <div className="w-56 rounded-2xl bg-gradient-to-br from-pink-500 to-orange-400 text-white p-4 shadow-lg">
                      <div className="text-xs opacity-90 font-medium">TRIGGER</div>
                      <div className="mt-1 font-semibold">New Comment</div>
                      <div className="mt-2 text-xs opacity-90">Your keyword</div>
                    </div>
                    <div className="w-56 rounded-2xl bg-gradient-to-br from-blue-500 to-cyan-400 text-white p-4 shadow-lg">
                      <div className="text-xs opacity-90 font-medium">MESSAGE</div>
                      <div className="mt-1 font-semibold">Opening DM</div>
                      <div className="mt-2 text-xs opacity-90">Write your own message</div>
                    </div>
                    <div className="w-56 rounded-2xl bg-gradient-to-br from-purple-500 to-pink-400 text-white p-4 shadow-lg">
                      <div className="text-xs opacity-90 font-medium">ACTION</div>
                      <div className="mt-1 font-semibold">Reply or Link</div>
                      <div className="mt-2 text-xs opacity-90">Choose what happens next</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section id="features" className="py-24 px-6">
        <div className="max-w-6xl mx-auto">
          <div className="max-w-2xl">
            <Badge className="bg-pink-50 text-pink-700 border-pink-100 rounded-full">Features</Badge>
            <h2 className="mt-4 font-display text-4xl md:text-5xl font-extrabold tracking-tight">
              Tools for your own Instagram data
            </h2>
            <p className="mt-4 text-lg text-slate-600">The app starts empty and fills from your connected account and configured rules.</p>
          </div>
          <div className="mt-14 grid md:grid-cols-2 lg:grid-cols-3 gap-6">
            {features.map((f) => {
              const Icon = iconMap[f.icon];
              return (
                <div key={f.title} className="group relative rounded-2xl p-6 border border-slate-100 hover:border-slate-200 hover:shadow-xl transition-all bg-white">
                  <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${f.color} flex items-center justify-center shadow-lg`}>
                    <Icon className="w-6 h-6 text-white" strokeWidth={2.2} />
                  </div>
                  <h3 className="mt-5 text-xl font-bold font-display">{f.title}</h3>
                  <p className="mt-2 text-slate-600 text-sm leading-relaxed">{f.description}</p>
                  <div className="mt-4 flex items-center text-sm font-semibold text-slate-900 opacity-0 group-hover:opacity-100 transition-opacity">
                    Learn more <ChevronRight className="w-4 h-4 ml-1" />
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      <section id="how" className="py-24 px-6 bg-slate-50">
        <div className="max-w-6xl mx-auto">
          <div className="text-center max-w-2xl mx-auto">
            <Badge className="bg-blue-50 text-blue-700 border-blue-100 rounded-full">How it works</Badge>
            <h2 className="mt-4 font-display text-4xl md:text-5xl font-extrabold tracking-tight">Set up your own account</h2>
          </div>
          <div className="mt-14 grid md:grid-cols-3 gap-6">
            {[
              { step: '01', title: 'Connect Instagram', desc: 'Link a Business or Creator account through the app settings.' },
              { step: '02', title: 'Create a rule', desc: 'Choose a post, keyword, public reply, and DM message.' },
              { step: '03', title: 'Review activity', desc: 'See real contacts, comments, messages, and broadcast records as they are created.' },
            ].map((s) => (
              <div key={s.step} className="rounded-2xl bg-white border border-slate-100 p-8">
                <div className="text-6xl font-display font-extrabold text-slate-100">{s.step}</div>
                <h3 className="mt-4 text-xl font-bold font-display">{s.title}</h3>
                <p className="mt-2 text-slate-600">{s.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="py-24 px-6">
        <div className="max-w-5xl mx-auto relative rounded-3xl overflow-hidden bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 p-12 md:p-16 text-center">
          <div className="relative">
            <h2 className="font-display text-4xl md:text-5xl font-extrabold text-white tracking-tight">Ready to manage Instagram automations?</h2>
            <p className="mt-4 text-lg text-slate-300 max-w-xl mx-auto">Create your account and connect Instagram when you are ready to automate real conversations.</p>
            <Link to="/signup">
              <Button size="lg" className="mt-8 bg-white text-slate-900 hover:bg-slate-100 rounded-full px-8 h-14">
                Get Started <ArrowRight className="ml-2 w-4 h-4" />
              </Button>
            </Link>
          </div>
        </div>
      </section>

      <footer className="border-t border-slate-100 py-12 px-6">
        <div className="max-w-6xl mx-auto flex flex-col md:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
              <MessageCircle className="w-4 h-4 text-white" strokeWidth={2.5} />
            </div>
            <span className="font-bold font-display">mychat</span>
            <span className="text-sm text-slate-500 ml-2">© 2026 All rights reserved.</span>
          </div>
          <div className="flex gap-6 text-sm text-slate-500">
            <Link to="/privacy" className="hover:text-slate-900">Privacy</Link>
            <Link to="/terms" className="hover:text-slate-900">Terms</Link>
            <a href="mailto:mm.mohame172000@gmail.com" className="hover:text-slate-900">Contact</a>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Landing;

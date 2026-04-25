import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import {
  MessageCircle, Zap, Users, BarChart3, Bot, Target, Star, Check,
  ArrowRight, Instagram, Play, Menu, X, Sparkles, ChevronRight
} from 'lucide-react';
import { features, stats, testimonials, pricingPlans } from '../mock/mock';

const iconMap = { MessageCircle, Zap, Users, BarChart3, Bot, Target };

const Landing = () => {
  const [menuOpen, setMenuOpen] = useState(false);

  return (
    <div className="min-h-screen bg-white text-slate-900 overflow-x-hidden">
      {/* Navbar */}
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
            <a href="#pricing" className="text-sm font-medium text-slate-600 hover:text-slate-900 transition-colors">Pricing</a>
            <a href="#testimonials" className="text-sm font-medium text-slate-600 hover:text-slate-900 transition-colors">Customers</a>
          </div>
          <div className="hidden md:flex items-center gap-3">
            <Link to="/login"><Button variant="ghost" className="text-slate-700">Log in</Button></Link>
            <Link to="/signup">
              <Button className="bg-slate-900 hover:bg-slate-800 text-white rounded-full px-5">
                Get Started Free
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
              <a href="#pricing" onClick={() => setMenuOpen(false)} className="text-sm font-medium">Pricing</a>
              <Link to="/login"><Button variant="outline" className="w-full">Log in</Button></Link>
              <Link to="/signup"><Button className="w-full bg-slate-900 text-white">Get Started</Button></Link>
            </div>
          </div>
        )}
      </nav>

      {/* Hero */}
      <section className="relative pt-32 pb-20 px-6">
        <div className="absolute top-20 -left-20 w-96 h-96 bg-blue-200/40 rounded-full blur-3xl" />
        <div className="absolute top-40 -right-20 w-96 h-96 bg-pink-200/40 rounded-full blur-3xl" />
        <div className="max-w-6xl mx-auto relative">
          <div className="text-center animate-fade-up">
            <Badge className="bg-blue-50 text-blue-700 hover:bg-blue-50 border-blue-100 rounded-full px-4 py-1.5 mb-6">
              <Sparkles className="w-3.5 h-3.5 mr-1.5" />
              #1 Instagram Automation Platform
            </Badge>
            <h1 className="font-display text-5xl sm:text-6xl md:text-7xl font-extrabold leading-[1.05] tracking-tight">
              Turn Instagram <br />
              conversations into <span className="gradient-text">revenue.</span>
            </h1>
            <p className="mt-6 text-lg md:text-xl text-slate-600 max-w-2xl mx-auto">
              Automate comments, DMs, and story replies on Instagram. Grow your audience, capture leads, and sell more — all on autopilot.
            </p>
            <div className="mt-10 flex flex-col sm:flex-row items-center justify-center gap-4">
              <Link to="/signup">
                <Button size="lg" className="bg-slate-900 hover:bg-slate-800 text-white rounded-full px-8 h-14 text-base animate-pulse-glow">
                  Start for Free <ArrowRight className="ml-2 w-4 h-4" />
                </Button>
              </Link>
              <Button size="lg" variant="outline" className="rounded-full px-8 h-14 text-base border-slate-200">
                <Play className="mr-2 w-4 h-4" /> Watch Demo
              </Button>
            </div>
            <p className="mt-6 text-sm text-slate-500">Free forever • No credit card required • Set up in 2 minutes</p>
          </div>

          {/* Product preview */}
          <div className="mt-16 relative animate-fade-up" style={{ animationDelay: '0.2s' }}>
            <div className="relative rounded-3xl overflow-hidden shadow-2xl border border-slate-200 bg-white">
              <div className="h-10 bg-slate-50 border-b border-slate-100 flex items-center px-4 gap-1.5">
                <div className="w-3 h-3 rounded-full bg-red-400" />
                <div className="w-3 h-3 rounded-full bg-amber-400" />
                <div className="w-3 h-3 rounded-full bg-green-400" />
              </div>
              <div className="grid md:grid-cols-[240px_1fr] min-h-[400px]">
                <div className="bg-slate-50 border-r border-slate-100 p-4 space-y-1">
                  {['Dashboard', 'Automations', 'Contacts', 'Live Chat', 'Broadcasting'].map((item, i) => (
                    <div key={item} className={`px-3 py-2 rounded-lg text-sm font-medium ${i === 1 ? 'bg-blue-50 text-blue-700' : 'text-slate-600'}`}>{item}</div>
                  ))}
                </div>
                <div className="p-6 flow-grid">
                  <div className="flex gap-6 flex-wrap">
                    <div className="w-56 rounded-2xl bg-gradient-to-br from-pink-500 to-orange-400 text-white p-4 shadow-lg">
                      <div className="text-xs opacity-90 font-medium">TRIGGER</div>
                      <div className="mt-1 font-semibold">New Comment</div>
                      <div className="mt-2 text-xs opacity-90">Keyword: SHOP</div>
                    </div>
                    <div className="w-56 rounded-2xl bg-gradient-to-br from-blue-500 to-cyan-400 text-white p-4 shadow-lg">
                      <div className="text-xs opacity-90 font-medium">MESSAGE</div>
                      <div className="mt-1 font-semibold">Send DM</div>
                      <div className="mt-2 text-xs opacity-90">Hi! Thanks for your interest...</div>
                    </div>
                    <div className="w-56 rounded-2xl bg-gradient-to-br from-purple-500 to-pink-400 text-white p-4 shadow-lg">
                      <div className="text-xs opacity-90 font-medium">ACTION</div>
                      <div className="mt-1 font-semibold">Add Tag: Customer</div>
                      <div className="mt-2 text-xs opacity-90">Auto-segmentation</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
            <div className="absolute -top-6 -right-6 hidden md:flex items-center gap-2 bg-white rounded-2xl shadow-xl border border-slate-100 px-4 py-3 animate-float">
              <Instagram className="w-5 h-5 text-pink-500" />
              <div>
                <div className="text-xs text-slate-500">Just now</div>
                <div className="text-sm font-semibold">+248 new leads</div>
              </div>
            </div>
          </div>
        </div>
      </section>

      {/* Stats */}
      <section className="py-16 px-6 border-y border-slate-100 bg-slate-50">
        <div className="max-w-6xl mx-auto grid grid-cols-2 md:grid-cols-4 gap-8">
          {stats.map((s) => (
            <div key={s.label} className="text-center">
              <div className="font-display text-4xl md:text-5xl font-extrabold gradient-text">{s.value}</div>
              <div className="mt-2 text-sm text-slate-600 font-medium">{s.label}</div>
            </div>
          ))}
        </div>
      </section>

      {/* Features */}
      <section id="features" className="py-24 px-6">
        <div className="max-w-6xl mx-auto">
          <div className="max-w-2xl">
            <Badge className="bg-pink-50 text-pink-700 border-pink-100 rounded-full">Features</Badge>
            <h2 className="mt-4 font-display text-4xl md:text-5xl font-extrabold tracking-tight">
              Everything you need to win on Instagram
            </h2>
            <p className="mt-4 text-lg text-slate-600">Powerful automation tools built specifically for Instagram business accounts.</p>
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

      {/* How it works */}
      <section id="how" className="py-24 px-6 bg-slate-50">
        <div className="max-w-6xl mx-auto">
          <div className="text-center max-w-2xl mx-auto">
            <Badge className="bg-blue-50 text-blue-700 border-blue-100 rounded-full">How it works</Badge>
            <h2 className="mt-4 font-display text-4xl md:text-5xl font-extrabold tracking-tight">Launch in 3 simple steps</h2>
          </div>
          <div className="mt-14 grid md:grid-cols-3 gap-6">
            {[
              { step: '01', title: 'Connect Instagram', desc: 'Link your Business account in one click with secure OAuth.' },
              { step: '02', title: 'Build your flow', desc: 'Drag & drop triggers, messages, and conditions. No code.' },
              { step: '03', title: 'Watch it grow', desc: 'Track performance and scale winning automations.' }
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

      {/* Testimonials */}
      <section id="testimonials" className="py-24 px-6">
        <div className="max-w-6xl mx-auto">
          <div className="text-center max-w-2xl mx-auto">
            <Badge className="bg-amber-50 text-amber-700 border-amber-100 rounded-full">Customers</Badge>
            <h2 className="mt-4 font-display text-4xl md:text-5xl font-extrabold tracking-tight">Loved by 1M+ businesses</h2>
          </div>
          <div className="mt-14 grid md:grid-cols-3 gap-6">
            {testimonials.map((t) => (
              <div key={t.name} className="rounded-2xl border border-slate-100 p-6 bg-white hover:shadow-lg transition-shadow">
                <div className="flex gap-1">
                  {Array.from({ length: t.rating }).map((_, i) => <Star key={`${t.name}-star-${i}`} className="w-4 h-4 fill-amber-400 text-amber-400" />)}
                </div>
                <p className="mt-4 text-slate-700 leading-relaxed">“{t.quote}”</p>
                <div className="mt-6 flex items-center gap-3">
                  <img src={t.avatar} alt={t.name} className="w-10 h-10 rounded-full object-cover" />
                  <div>
                    <div className="font-semibold text-sm">{t.name}</div>
                    <div className="text-xs text-slate-500">{t.role}</div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* Pricing */}
      <section id="pricing" className="py-24 px-6 bg-slate-50">
        <div className="max-w-6xl mx-auto">
          <div className="text-center max-w-2xl mx-auto">
            <Badge className="bg-emerald-50 text-emerald-700 border-emerald-100 rounded-full">Pricing</Badge>
            <h2 className="mt-4 font-display text-4xl md:text-5xl font-extrabold tracking-tight">Simple, transparent pricing</h2>
            <p className="mt-4 text-lg text-slate-600">Start free. Upgrade when you’re ready to scale.</p>
          </div>
          <div className="mt-14 grid md:grid-cols-3 gap-6">
            {pricingPlans.map((p) => (
              <div key={p.name} className={`relative rounded-3xl p-8 border ${p.popular ? 'border-slate-900 bg-slate-900 text-white' : 'border-slate-100 bg-white'}`}>
                {p.popular && (
                  <Badge className="absolute -top-3 left-1/2 -translate-x-1/2 bg-gradient-to-r from-pink-500 to-orange-400 text-white border-0 rounded-full">Most Popular</Badge>
                )}
                <h3 className="font-display text-2xl font-bold">{p.name}</h3>
                <p className={`mt-1 text-sm ${p.popular ? 'text-slate-300' : 'text-slate-500'}`}>{p.description}</p>
                <div className="mt-6 flex items-baseline gap-1">
                  <span className="text-5xl font-extrabold font-display">${p.price}</span>
                  <span className={p.popular ? 'text-slate-400' : 'text-slate-500'}>/month</span>
                </div>
                <Link to="/signup">
                  <Button className={`mt-6 w-full rounded-full h-12 ${p.popular ? 'bg-white text-slate-900 hover:bg-slate-100' : 'bg-slate-900 text-white hover:bg-slate-800'}`}>
                    {p.cta}
                  </Button>
                </Link>
                <ul className="mt-8 space-y-3">
                  {p.features.map((f) => (
                    <li key={f} className="flex items-start gap-2 text-sm">
                      <Check className={`w-4 h-4 mt-0.5 ${p.popular ? 'text-emerald-400' : 'text-emerald-500'}`} />
                      <span>{f}</span>
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* CTA */}
      <section className="py-24 px-6">
        <div className="max-w-5xl mx-auto relative rounded-3xl overflow-hidden bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 p-12 md:p-16 text-center">
          <div className="absolute top-0 right-0 w-80 h-80 bg-pink-500/20 rounded-full blur-3xl" />
          <div className="absolute bottom-0 left-0 w-80 h-80 bg-blue-500/20 rounded-full blur-3xl" />
          <div className="relative">
            <h2 className="font-display text-4xl md:text-5xl font-extrabold text-white tracking-tight">Ready to grow on Instagram?</h2>
            <p className="mt-4 text-lg text-slate-300 max-w-xl mx-auto">Join 1M+ businesses automating their Instagram growth with mychat.</p>
            <Link to="/signup">
              <Button size="lg" className="mt-8 bg-white text-slate-900 hover:bg-slate-100 rounded-full px-8 h-14">
                Start Free Today <ArrowRight className="ml-2 w-4 h-4" />
              </Button>
            </Link>
          </div>
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-slate-100 py-12 px-6">
        <div className="max-w-6xl mx-auto flex flex-col md:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
              <MessageCircle className="w-4 h-4 text-white" strokeWidth={2.5} />
            </div>
            <span className="font-bold font-display">mychat</span>
            <span className="text-sm text-slate-500 ml-2">© 2025 All rights reserved.</span>
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

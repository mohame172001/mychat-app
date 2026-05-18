import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { Button } from '../components/ui/button';
import { Badge } from '../components/ui/badge';
import {
  ArrowRight, BarChart3, Bot, ChevronRight, Instagram, Menu,
  MessageCircle, Sparkles, Target, Users, X, Zap, MousePointerClick,
  Shield, Languages,
} from 'lucide-react';
import { buildSupportMailtoHref, handleContactClick } from '../lib/contactSupport';
import { useTranslation } from '../lib/i18n';
import LangSwitcher from '../components/LangSwitcher';


// Feature key → icon. The actual copy comes from i18n dictionaries.
const FEATURE_ICONS = {
  commentTrigger: { Icon: Zap, color: 'from-blue-500 to-cyan-400' },
  dmAutomation: { Icon: MessageCircle, color: 'from-pink-500 to-orange-400' },
  dashboard: { Icon: BarChart3, color: 'from-emerald-500 to-teal-400' },
  deliveryAware: { Icon: Shield, color: 'from-purple-500 to-pink-400' },
  multiAccount: { Icon: Users, color: 'from-indigo-500 to-blue-400' },
  conversionTracking: { Icon: MousePointerClick, color: 'from-amber-500 to-orange-400' },
};
const FEATURE_KEYS = Object.keys(FEATURE_ICONS);


const Landing = () => {
  const [menuOpen, setMenuOpen] = useState(false);
  const { t } = useTranslation();

  const scrollToSection = (id) => (event) => {
    event?.preventDefault?.();
    setMenuOpen(false);
    if (typeof window === 'undefined' || typeof document === 'undefined') return;
    const el = document.getElementById(id);
    if (!el) return;
    const NAV_OFFSET = 72;
    const top = el.getBoundingClientRect().top + window.pageYOffset - NAV_OFFSET;
    window.scrollTo({ top, behavior: 'smooth' });
    if (window.history?.replaceState) {
      window.history.replaceState(null, '', `#${id}`);
    }
  };

  const previewSidebarItems = ['Dashboard', 'Automations', 'DM Automation', 'Billing', 'Settings'];

  return (
    <div className="min-h-screen bg-white text-slate-900 overflow-x-hidden">
      <nav className="fixed top-0 inset-x-0 z-50 backdrop-blur-xl bg-white/80 border-b border-slate-100">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 h-16 flex items-center justify-between">
          <Link to="/" className="flex items-center gap-2">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
              <MessageCircle className="w-5 h-5 text-white" strokeWidth={2.5} />
            </div>
            <span className="text-xl font-bold font-display tracking-tight">{t('common.brand')}</span>
          </Link>
          <div className="hidden md:flex items-center gap-8">
            <a href="#features" onClick={scrollToSection('features')} className="text-sm font-medium text-slate-600 hover:text-slate-900 transition-colors">{t('landing.nav.features')}</a>
            <a href="#how" onClick={scrollToSection('how')} className="text-sm font-medium text-slate-600 hover:text-slate-900 transition-colors">{t('landing.nav.how')}</a>
            <Link to="/status" className="text-sm font-medium text-slate-600 hover:text-slate-900 transition-colors">{t('landing.nav.status')}</Link>
            <Link to="/privacy" className="text-sm font-medium text-slate-600 hover:text-slate-900 transition-colors">{t('common.privacy')}</Link>
          </div>
          <div className="hidden md:flex items-center gap-3">
            <LangSwitcher />
            <Link to="/login"><Button variant="ghost" className="text-slate-700">{t('common.login')}</Button></Link>
            <Link to="/signup">
              <Button className="bg-slate-900 hover:bg-slate-800 text-white rounded-full px-5">
                {t('common.signup')}
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
              <a href="#features" onClick={scrollToSection('features')} className="text-sm font-medium">{t('landing.nav.features')}</a>
              <a href="#how" onClick={scrollToSection('how')} className="text-sm font-medium">{t('landing.nav.how')}</a>
              <Link to="/status" onClick={() => setMenuOpen(false)} className="text-sm font-medium">{t('landing.nav.status')}</Link>
              <Link to="/privacy" onClick={() => setMenuOpen(false)} className="text-sm font-medium">{t('common.privacy')}</Link>
              <div className="flex items-center justify-between pt-2 border-t border-slate-100">
                <span className="text-xs text-slate-500">{lang === "ar" ? "اللغة" : "Language"}</span>
                <LangSwitcher />
              </div>
              <Link to="/login"><Button variant="outline" className="w-full">{t('common.login')}</Button></Link>
              <Link to="/signup"><Button className="w-full bg-slate-900 text-white">{t('common.signup')}</Button></Link>
            </div>
          </div>
        )}
      </nav>

      <section className="relative pt-28 pb-16 px-4 sm:px-6 md:pt-32 md:pb-20">
        <div className="max-w-6xl mx-auto relative">
          <div className="text-center animate-fade-up">
            <Badge className="bg-blue-50 text-blue-700 hover:bg-blue-50 border-blue-100 rounded-full px-4 py-1.5 mb-6">
              <Sparkles className="w-3.5 h-3.5 me-1.5" />
              {t('landing.hero.badge')}
            </Badge>
            <h1 className="font-display text-4xl sm:text-6xl md:text-7xl font-extrabold leading-[1.05] tracking-tight">
              {t('landing.hero.title1')} <br />
              {t('landing.hero.title2')} <span className="gradient-text">{t('landing.hero.titleEm')}</span>
            </h1>
            <p className="mt-6 text-lg md:text-xl text-slate-600 max-w-2xl mx-auto">
              {t('landing.hero.subtitle')}
            </p>
            <div className="mt-10 flex items-center justify-center gap-3 flex-wrap">
              <Link to="/signup">
                <Button size="lg" className="bg-slate-900 hover:bg-slate-800 text-white rounded-full px-8 h-14 text-base">
                  {t('landing.hero.cta')} <ArrowRight className="ms-2 w-4 h-4" />
                </Button>
              </Link>
              <Link to="/status">
                <Button size="lg" variant="outline" className="rounded-full px-6 h-14 text-base">
                  {t('landing.hero.secondaryCta')}
                </Button>
              </Link>
            </div>
          </div>

          <div className="mt-12 relative animate-fade-up md:mt-16" style={{ animationDelay: '0.2s' }}>
            <div className="relative rounded-3xl overflow-hidden shadow-2xl border border-slate-200 bg-white">
              <div className="h-10 bg-slate-50 border-b border-slate-100 flex items-center px-4 gap-1.5">
                <div className="w-3 h-3 rounded-full bg-red-400" />
                <div className="w-3 h-3 rounded-full bg-amber-400" />
                <div className="w-3 h-3 rounded-full bg-green-400" />
              </div>
              <div className="grid md:grid-cols-[240px_1fr] min-h-[400px]" dir="ltr">
                <div className="bg-slate-50 border-r border-slate-100 p-3 sm:p-4 space-y-1">
                  {previewSidebarItems.map((item, i) => (
                    <div key={item} className={`px-3 py-2 rounded-lg text-sm font-medium ${i === 1 ? 'bg-blue-50 text-blue-700' : 'text-slate-600'}`}>{item}</div>
                  ))}
                </div>
                <div className="p-4 sm:p-6 flow-grid">
                  <div className="flex gap-6 flex-wrap">
                    <div className="w-56 rounded-2xl bg-gradient-to-br from-pink-500 to-orange-400 text-white p-4 shadow-lg">
                      <div className="text-xs opacity-90 font-medium">{t('landing.preview.triggerLabel')}</div>
                      <div className="mt-1 font-semibold">{t('landing.preview.triggerTitle')}</div>
                      <div className="mt-2 text-xs opacity-90">{t('landing.preview.triggerHint')}</div>
                    </div>
                    <div className="w-56 rounded-2xl bg-gradient-to-br from-blue-500 to-cyan-400 text-white p-4 shadow-lg">
                      <div className="text-xs opacity-90 font-medium">{t('landing.preview.messageLabel')}</div>
                      <div className="mt-1 font-semibold">{t('landing.preview.messageTitle')}</div>
                      <div className="mt-2 text-xs opacity-90">{t('landing.preview.messageHint')}</div>
                    </div>
                    <div className="w-56 rounded-2xl bg-gradient-to-br from-purple-500 to-pink-400 text-white p-4 shadow-lg">
                      <div className="text-xs opacity-90 font-medium">{t('landing.preview.actionLabel')}</div>
                      <div className="mt-1 font-semibold">{t('landing.preview.actionTitle')}</div>
                      <div className="mt-2 text-xs opacity-90">{t('landing.preview.actionHint')}</div>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </section>

      <section id="features" className="py-16 px-4 sm:px-6 md:py-24">
        <div className="max-w-6xl mx-auto">
          <div className="max-w-2xl">
            <Badge className="bg-pink-50 text-pink-700 border-pink-100 rounded-full">{t('landing.features.badge')}</Badge>
            <h2 className="mt-4 font-display text-4xl md:text-5xl font-extrabold tracking-tight">
              {t('landing.features.title')}
            </h2>
            <p className="mt-4 text-lg text-slate-600">{t('landing.features.subtitle')}</p>
          </div>
          <div className="mt-14 grid md:grid-cols-2 lg:grid-cols-3 gap-6">
            {FEATURE_KEYS.map((key) => {
              const { Icon, color } = FEATURE_ICONS[key];
              return (
                <div key={key} className="group relative rounded-2xl p-6 border border-slate-100 hover:border-slate-200 hover:shadow-xl transition-all bg-white">
                  <div className={`w-12 h-12 rounded-xl bg-gradient-to-br ${color} flex items-center justify-center shadow-lg`}>
                    <Icon className="w-6 h-6 text-white" strokeWidth={2.2} />
                  </div>
                  <h3 className="mt-5 text-xl font-bold font-display">{t(`landing.features.items.${key}.title`)}</h3>
                  <p className="mt-2 text-slate-600 text-sm leading-relaxed">{t(`landing.features.items.${key}.description`)}</p>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      <section id="how" className="py-16 px-4 sm:px-6 md:py-24 bg-slate-50">
        <div className="max-w-6xl mx-auto">
          <div className="text-center max-w-2xl mx-auto">
            <Badge className="bg-blue-50 text-blue-700 border-blue-100 rounded-full">{t('landing.how.badge')}</Badge>
            <h2 className="mt-4 font-display text-4xl md:text-5xl font-extrabold tracking-tight">{t('landing.how.title')}</h2>
          </div>
          <div className="mt-14 grid md:grid-cols-3 gap-6">
            {t('landing.how.steps').map((s) => (
              <div key={s.num} className="rounded-2xl bg-white border border-slate-100 p-8">
                <div className="text-6xl font-display font-extrabold text-slate-100">{s.num}</div>
                <h3 className="mt-4 text-xl font-bold font-display">{s.title}</h3>
                <p className="mt-2 text-slate-600">{s.desc}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="py-16 px-4 sm:px-6 md:py-24">
        <div className="max-w-5xl mx-auto relative rounded-3xl overflow-hidden bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900 p-6 sm:p-12 md:p-16 text-center">
          <div className="relative">
            <h2 className="font-display text-4xl md:text-5xl font-extrabold text-white tracking-tight">{t('landing.cta.title')}</h2>
            <p className="mt-4 text-lg text-slate-300 max-w-xl mx-auto">{t('landing.cta.body')}</p>
            <Link to="/signup">
              <Button size="lg" className="mt-8 bg-white text-slate-900 hover:bg-slate-100 rounded-full px-8 h-14">
                {t('landing.cta.button')} <ArrowRight className="ms-2 w-4 h-4" />
              </Button>
            </Link>
          </div>
        </div>
      </section>

      <footer className="border-t border-slate-100 py-10 px-4 sm:px-6 md:py-12">
        <div className="max-w-6xl mx-auto flex flex-col md:flex-row items-center justify-between gap-4">
          <div className="flex items-center gap-2">
            <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
              <MessageCircle className="w-4 h-4 text-white" strokeWidth={2.5} />
            </div>
            <span className="font-bold font-display">{t('common.brand')}</span>
            <span className="text-sm text-slate-500 ms-2">{t('common.copyright')}</span>
          </div>
          <div className="flex gap-6 text-sm text-slate-500 items-center flex-wrap">
            <Link to="/status" className="hover:text-slate-900">{t('landing.nav.status')}</Link>
            <Link to="/privacy" className="hover:text-slate-900">{t('common.privacy')}</Link>
            <Link to="/terms" className="hover:text-slate-900">{t('common.terms')}</Link>
            <Link to="/data-deletion" className="hover:text-slate-900">{t('common.dataDeletion')}</Link>
            <a
              href={buildSupportMailtoHref()}
              onClick={handleContactClick}
              className="hover:text-slate-900"
            >
              {t('common.contact')}
            </a>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Landing;

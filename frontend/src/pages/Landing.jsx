import React, { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { ArrowDown, ArrowUpRight, Check, ChevronDown, Instagram, Menu, MessageCircle, ShieldCheck, X } from 'lucide-react';
import { useTranslation } from '../lib/i18n';
import LangSwitcher from '../components/LangSwitcher';
import ScrollStory from '../components/landing/ScrollStory';
import { landingCopy } from '../components/landing/landingCopy';
import './Landing.css';

function useReducedMotion() {
  const [reduced, setReduced] = useState(true);
  useEffect(() => {
    if (!window.matchMedia) return undefined;
    const query = window.matchMedia('(prefers-reduced-motion: reduce)');
    const update = () => setReduced(query.matches);
    update();
    query.addEventListener('change', update);
    return () => query.removeEventListener('change', update);
  }, []);
  return reduced;
}

function Brand() {
  return <Link to="/" className="landing-brand" aria-label="MyChaat" onClick={() => window.scrollTo({ top: 0, behavior: 'auto' })}><span className="landing-brand-icon"><MessageCircle size={21} /></span><span dir="ltr">MyChaat<span className="brand-dot">.</span></span></Link>;
}

export default function Landing() {
  const { lang, t } = useTranslation();
  const copy = landingCopy[lang] || landingCopy.en;
  const [menuOpen, setMenuOpen] = useState(false);
  const pageRef = useRef(null);
  const menuButton = useRef(null);
  const reducedMotion = useReducedMotion();

  useEffect(() => {
    if (reducedMotion || !window.IntersectionObserver) return undefined;
    const nodes = pageRef.current.querySelectorAll('[data-reveal]');
    const observer = new IntersectionObserver(entries => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          entry.target.dataset.reveal = 'visible';
          observer.unobserve(entry.target);
        }
      });
    }, { threshold: 0.08 });
    nodes.forEach(node => { node.dataset.reveal = 'pending'; observer.observe(node); });
    return () => { observer.disconnect(); nodes.forEach(node => { node.dataset.reveal = 'visible'; }); };
  }, [reducedMotion]);

  useEffect(() => {
    if (!menuOpen) return undefined;
    function onKey(event) {
      if (event.key === 'Escape') { setMenuOpen(false); menuButton.current?.focus(); }
    }
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [menuOpen]);

  function goTo(id) {
    return event => {
      event.preventDefault();
      setMenuOpen(false);
      document.getElementById(id)?.scrollIntoView({ behavior: reducedMotion ? 'auto' : 'smooth', block: 'start' });
      window.history.replaceState(null, '', `#${id}`);
    };
  }

  return (
    <div className="landing-page" ref={pageRef} lang={lang} dir={lang === 'ar' ? 'rtl' : 'ltr'}>
      <a className="landing-skip" href="#main">{lang === 'ar' ? 'انتقل إلى المحتوى' : 'Skip to content'}</a>
      <header className="landing-header">
        <nav className="landing-nav landing-shell" aria-label={lang === 'ar' ? 'شريط التنقل' : 'Site navigation'}>
          <Brand />
          <div className="landing-desktop-links"><a href="#features" onClick={goTo('features')}>{copy.nav[0]}</a><a href="#how" onClick={goTo('how')}>{copy.nav[1]}</a><a href="#questions" onClick={goTo('questions')}>{copy.nav[2]}</a></div>
          <div className="landing-nav-actions"><LangSwitcher /><Link className="landing-login" to="/login">{t('common.login')}<ArrowUpRight size={17} /></Link><button ref={menuButton} type="button" className="landing-menu-button" aria-label={menuOpen ? copy.close : copy.menu} aria-expanded={menuOpen} aria-controls="landing-mobile-menu" onClick={() => setMenuOpen(open => !open)}>{menuOpen ? <X /> : <Menu />}</button></div>
        </nav>
        {menuOpen && <nav id="landing-mobile-menu" className="landing-mobile-menu landing-shell" aria-label={copy.menu}><a href="#features" onClick={goTo('features')}>{copy.nav[0]}</a><a href="#how" onClick={goTo('how')}>{copy.nav[1]}</a><a href="#questions" onClick={goTo('questions')}>{copy.nav[2]}</a><Link to="/login" onClick={() => setMenuOpen(false)}>{t('common.login')}</Link><Link to="/signup" onClick={() => setMenuOpen(false)}>{copy.cta}</Link></nav>}
      </header>

      <main id="main">
        <section className="landing-hero landing-shell">
          <div className="hero-copy">
            <p className="landing-eyebrow hero-enter"><span className="eyebrow-dot" />{copy.eyebrow}</p>
            <h1 className="landing-display hero-title hero-enter">{copy.title.map((line, index) => <span key={line} className={index === copy.title.length - 1 ? 'hero-accent' : ''}>{line}</span>)}</h1>
            <p className="hero-description hero-enter">{copy.intro}</p>
            <div className="hero-actions hero-enter"><Link to="/signup" className="landing-button">{copy.cta}<ArrowUpRight size={22} /></Link><a href="#demo" onClick={goTo('demo')} className="landing-text-link">{copy.watch}<ArrowDown size={16} /></a></div>
            <p className="hero-note"><Instagram size={14} />{copy.note}</p>
          </div>
          <div className="hero-float" aria-hidden="true"><span>LET IT FLOW</span><p>{lang === 'ar' ? 'ممكن التفاصيل' : 'Can I get the details?'}</p><ArrowUpRight size={24} /></div>
        </section>
        <ScrollStory copy={copy} lang={lang} reducedMotion={reducedMotion} />
        <div className="landing-principles landing-shell">{copy.principles.map(item => <span key={item}><Check size={16} />{item}</span>)}</div>

        <section id="how" className="landing-how">
          <div className="landing-shell">
            <div className="how-heading" data-reveal="visible"><p className="landing-eyebrow">{copy.howLabel}</p><h2 className="landing-display">{copy.howTitle[0]}<br /><span>{copy.howTitle[1]}</span></h2><span className="how-asterisk" aria-hidden="true">✳</span></div>
            <div className="how-steps">{copy.steps.map((step, index) => <article key={step.title} data-reveal="visible"><span className="step-number" dir="ltr">0{index + 1}</span><h3>{step.title}</h3><p>{step.body}</p></article>)}</div>
          </div>
        </section>

        <section id="features" className="landing-features landing-shell">
          <div className="features-heading" data-reveal="visible"><p className="landing-eyebrow">{copy.featuresLabel}</p><h2 className="landing-display">{copy.featuresTitle[0]}<br /><span>{copy.featuresTitle[1]}</span></h2></div>
          <div className="features-layout">
            <div className="word-poster" data-reveal="visible"><p className="landing-eyebrow">{copy.specimenLabel}</p><div className="word-poster-orbit" aria-hidden="true" /><strong className="landing-display">{copy.specimenWord}<span aria-hidden="true">↗</span></strong><div className="word-poster-tags">{copy.specimenTags.map(tag => <span key={tag}>{tag}</span>)}</div><div className="word-poster-foot">{copy.specimenFoot}<MessageCircle size={25} /></div></div>
            <div className="feature-rows">{copy.features.map((feature, index) => <article key={feature.title} data-reveal="visible"><span className="feature-index" dir="ltr">0{index + 1}</span><div><h3>{feature.title}</h3><p>{feature.body}</p></div><ArrowUpRight size={22} aria-hidden="true" /></article>)}</div>
          </div>
        </section>

        <section className="landing-trust landing-shell" data-reveal="visible"><span className="trust-icon"><ShieldCheck size={30} /></span><div><h2>{copy.trustTitle}</h2><p>{copy.trustBody}</p></div><Link to="/privacy" className="landing-text-link">{copy.trustLink}<ArrowUpRight size={17} /></Link></section>

        <section id="questions" className="landing-faq landing-shell"><div data-reveal="visible"><p className="landing-eyebrow">{copy.faqLabel}</p><h2 className="landing-display">{copy.faqTitle}</h2></div><div className="faq-list" data-reveal="visible">{copy.questions.map(question => <details key={question.title}><summary>{question.title}<ChevronDown size={20} /></summary><p>{question.body}</p></details>)}</div></section>

        <section className="landing-finale"><div className="landing-shell" data-reveal="visible"><p className="landing-eyebrow">{copy.finalLabel}</p><div className="finale-row"><h2 className="landing-display">{copy.finalTitle[0]}<br /><span>{copy.finalTitle[1]}</span></h2><Link to="/signup" className="finale-link"><span className="finale-arrow"><ArrowUpRight strokeWidth={1.2} /></span>{copy.finalCta}</Link></div><p className="finale-note">{copy.finalNote}</p></div></section>
      </main>

      <footer className="landing-footer landing-shell"><div className="footer-top"><Brand /><nav className="landing-footer-links" aria-label={lang === 'ar' ? 'روابط المساعدة والسياسات' : 'Help and policies'}><Link to="/privacy">{t('common.privacy')}</Link><Link to="/terms">{t('common.terms')}</Link><Link to="/data-deletion">{t('common.dataDeletion')}</Link><Link to="/support">{t('common.contact')}</Link><Link to="/status">{t('landing.nav.status')}</Link></nav></div><div className="footer-bottom"><span dir="ltr">© {new Date().getFullYear()} MyChaat</span><p>{copy.legal}</p></div></footer>
    </div>
  );
}

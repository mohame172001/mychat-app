import React, { useEffect, useRef, useState } from 'react';
import { ArrowUpRight, Check, Instagram, MessageCircle, Pause, Play, RotateCcw, Send, Sparkles } from 'lucide-react';

export default function ConversationPreview({ copy, reducedMotion }) {
  const [step, setStep] = useState(0);
  const [playing, setPlaying] = useState(false);
  const preview = useRef(null);
  const started = useRef(false);
  useEffect(() => {
    if (reducedMotion) { setPlaying(false); return undefined; }
    if (started.current) return undefined;
    if (!window.IntersectionObserver) {
      started.current = true;
      setPlaying(true);
      return undefined;
    }
    const observer = new IntersectionObserver(entries => {
      if (entries.some(entry => entry.isIntersecting && entry.intersectionRatio >= 0.65)) {
        if (!started.current) { started.current = true; setPlaying(true); }
        observer.disconnect();
      }
    }, { threshold: 0.65 });
    observer.observe(preview.current);
    return () => observer.disconnect();
  }, [reducedMotion]);
  useEffect(() => {
    if (!playing) return undefined;
    if (step === 3) { setPlaying(false); return undefined; }
    const timer = window.setTimeout(() => setStep(value => value + 1), 1100);
    return () => window.clearTimeout(timer);
  }, [playing, step]);
  function toggle() {
    started.current = true;
    if (playing) setPlaying(false);
    else if (reducedMotion) setStep(value => (value + 1) % 4);
    else { if (step === 3) setStep(0); setPlaying(true); }
  }
  return (
    <section ref={preview} className="conversation-preview" aria-label={copy.demo}>
      <div className="preview-meta"><span><i />{copy.edition}</span><span dir="ltr">01—04</span></div>
      <div className="conversation-stage" data-step={step}>
        <svg className="conversation-wire" viewBox="0 0 520 510" fill="none" aria-hidden="true"><path d="M235 135H431Q467 135 467 171V281Q467 318 431 318H133Q94 318 94 356V437H305" /><path className="conversation-signal" d="M235 135H431Q467 135 467 171V281Q467 318 431 318H133Q94 318 94 356V437H305" /></svg>
        <div className="creator-post">
          <div className="creator-post-top"><Instagram size={16} /><span>your.studio</span><span>•••</span></div>
          <div className="creator-poster"><span className="poster-corner">{copy.postLabel}</span>
            <svg className="poster-sculpture" viewBox="0 0 240 250" fill="none" aria-hidden="true"><circle cx="121" cy="123" r="89" stroke="currentColor" strokeWidth="1.4" /><ellipse cx="121" cy="123" rx="38" ry="89" stroke="currentColor" strokeWidth="1.4" transform="rotate(-34 121 123)" /><ellipse cx="121" cy="123" rx="89" ry="31" stroke="currentColor" strokeWidth="1.4" transform="rotate(-34 121 123)" /><path d="M121 7V239M7 123H235M40 40L202 205M40 205L202 40" stroke="currentColor" strokeWidth="1.4" /></svg>
            <strong className="landing-display">{copy.postTitle.map(line => <span key={line}>{line}</span>)}</strong><span className="poster-edition">VOL. 01 / MYCHAAT STUDIO</span>
          </div>
          <div className="creator-post-caption"><MessageCircle size={15} /><span>{copy.postCaption}</span></div>
        </div>
        <div className={`preview-comment preview-event ${step >= 1 ? 'is-visible' : ''}`} aria-hidden={step < 1}><span className="preview-avatar">S</span><div><small>{copy.visitor}</small><p>{copy.comment}</p></div><MessageCircle size={16} /></div>
        <div className={`preview-match preview-event ${step >= 2 ? 'is-visible' : ''}`} aria-hidden={step < 2}><Sparkles size={16} /><span>{copy.match}: <b>{copy.keyword}</b></span><Check size={15} /></div>
        <div className={`preview-message preview-event ${step >= 3 ? 'is-visible' : ''}`} aria-hidden={step < 3}><span className="preview-message-label"><Send size={13} />{copy.messageLabel}</span><p>{copy.message}</p><span className="preview-link">{copy.messageLink}<ArrowUpRight size={16} /></span></div>
        <span className="preview-orbit" aria-hidden="true">✳</span>
      </div>
      <div className="preview-controls"><div className="preview-progress" role="group" aria-label={copy.demo}>{copy.stages.map((label, index) => <button key={label} type="button" aria-label={label} aria-pressed={step === index} className={step >= index ? 'is-complete' : ''} onClick={() => { started.current = true; setPlaying(false); setStep(index); }}><span>{String(index + 1).padStart(2, '0')}</span></button>)}</div><button type="button" className="preview-play" onClick={toggle} aria-label={playing ? copy.pause : step === 3 ? copy.replay : copy.play}>{playing ? <Pause size={16} /> : step === 3 ? <RotateCcw size={16} /> : <Play size={16} />}</button></div>
      <p className="preview-status" aria-live="polite">{copy.stages[step]}</p><p className="preview-disclaimer">{copy.demoNote}</p>
    </section>
  );
}

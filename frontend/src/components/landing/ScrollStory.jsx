import React, { useEffect, useRef, useState } from 'react';
import { MessageCircle, ArrowDown, ArrowUpRight, Check } from 'lucide-react';
import './ScrollStory.css';

export const storyStep = progress => Math.min(3, Math.max(0, Math.floor(progress * 4)));

export default function ScrollStory({ copy, lang, reducedMotion }) {
  const root = useRef(null);
  const [step, setStep] = useState(0);
  const ar = lang === 'ar';
  const titles = ar
    ? [['محتواك في الصورة', 'والبداية عندك'], ['كلمة بسيطة', 'وراها اهتمام'], ['على طريقتك', 'خطوة بخطوة'], ['رسالتك جاهزة', 'كمّل الحكاية']]
    : [['Your content', 'Your starting point'], ['A small comment', 'A real connection'], ['Your words', 'Your workflow'], ['A thoughtful reply', 'A new conversation']];
  const descriptions = ar
    ? ['اختار منشورك وكل أتمتة تبدأ بالمحتوى اللي تختاره', 'الكلمة تتطابق مع شرطك والحكاية تبدأ', 'تعليق يوصل وشرط يتحقق ورد جاهز أنت بتحدد المسار', 'معاينة لرسالتك الإرسال الفعلي حسب صلاحيات الحساب وسياسات Instagram']
    : ['Choose the post where your next conversation starts', 'A follower uses your keyword and the rule matches', 'A comment, a condition, your reply. You choose the steps', 'Preview your message. Actual delivery depends on permissions and Instagram policies'];

  useEffect(() => {
    const element = root.current;
    if (reducedMotion) return undefined;
    let frame = 0;
    const update = () => {
      const progress = Math.max(0, Math.min(1, -element.getBoundingClientRect().top / Math.max(1, element.offsetHeight - window.innerHeight)));
      element.style.setProperty('--story-progress', progress);
      setStep(storyStep(progress));
      frame = 0;
    };
    const schedule = () => { if (!frame) frame = requestAnimationFrame(update); };
    update();
    window.addEventListener('scroll', schedule, { passive: true });
    window.addEventListener('resize', schedule);
    return () => {
      cancelAnimationFrame(frame);
      window.removeEventListener('scroll', schedule);
      window.removeEventListener('resize', schedule);
    };
  }, [reducedMotion]);

  const jump = index => {
    if (reducedMotion) { setStep(index); return; }
    const element = root.current;
    window.scrollTo({ top: element.getBoundingClientRect().top + window.scrollY + (element.offsetHeight - window.innerHeight) * (index / 4 + .08), behavior: 'smooth' });
  };

  return <section id="demo" ref={root} className={`scroll-story ${reducedMotion ? 'story-reduced' : ''}`} aria-label={copy.demo}>
    <div className="story-stage" data-step={step}>
      <div className="story-tabs" aria-label={ar ? 'مراحل المثال' : 'Example steps'}>{copy.stages.map((label, index) => <button key={label} type="button" aria-current={step === index ? 'step' : undefined} onClick={() => jump(index)}>{label}</button>)}</div>
      <div className="story-copy"><span className="landing-eyebrow">0{step + 1} / 04</span><h2 key={step}>{titles[step][0]}<br />{titles[step][1]}</h2><p>{descriptions[step]}</p></div>
      <div className="story-visual" aria-label={copy.demoNote}>
        <div className="story-orbit" aria-hidden="true" />
        <div className="story-post" aria-hidden={step > 1}><div className="story-account"><span className="story-avatar" />creator.demo<MessageCircle size={16} /></div><div className="story-art"><strong>MAKE IT<br />FLOW</strong></div><p>{copy.postCaption}</p></div>
        <div className="story-comment" aria-hidden={step !== 1}><small>{copy.visitor}</small><p>{copy.comment}</p><span>{copy.keyword}<Check size={14} /></span></div>
        <div className="story-flow" aria-hidden={step !== 2}>{[copy.stages[1], copy.match, copy.stages[3]].map((label, index) => <React.Fragment key={label}>{index > 0 && <span className="story-connector" aria-hidden="true"><ArrowDown size={16} /></span>}<div className="story-node"><b>0{index + 1}</b>{label}</div></React.Fragment>)}</div>
        <div className="story-phone" aria-hidden={step !== 3}><div className="story-account"><span className="story-avatar" />creator.demo<ArrowUpRight size={16} /></div><div className="story-bubble">{copy.message}<span>{copy.messageLink}<ArrowUpRight size={14} /></span></div><small>{copy.demoNote}</small></div>
      </div>
      <div className="story-foot"><span>{copy.demoNote}</span><div className="story-track" aria-hidden="true"><i style={reducedMotion ? { width: `${(step + 1) * 25}%` } : undefined} /></div><span dir="ltr">0{step + 1} / 04</span></div>
    </div>
  </section>;
}

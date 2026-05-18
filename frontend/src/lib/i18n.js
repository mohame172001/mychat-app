/**
 * Phase 2.18Z — minimal i18n system.
 *
 * No npm dependency (saves ~50KB on the bundle vs react-i18next).
 * Pattern:
 *
 *   1. Add the new English string to `dictionaries.en.<scope>.<key>`.
 *   2. Add the Arabic translation to `dictionaries.ar.<scope>.<key>`.
 *   3. In your component:
 *
 *        import { useTranslation } from '../lib/i18n';
 *        const { t, lang, setLang } = useTranslation();
 *        return <h1>{t('landing.hero.title')}</h1>;
 *
 *   4. Strings missing in `ar` fall back to `en` with a console warning.
 *
 * Language preference persists in `localStorage.mychat_lang`. On
 * Arabic the `<html dir="rtl" lang="ar">` is set automatically so all
 * Tailwind RTL utilities (`rtl:`, `ltr:`) flip correctly.
 *
 * **CONTRIBUTING NOTE for future changes:**
 *   Any new user-facing string MUST be added to BOTH `en` and `ar`
 *   dictionaries below. PRs that introduce English-only strings will
 *   log a console warning at runtime that mentions the missing key.
 */
import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react';

export const SUPPORTED_LANGS = ['en', 'ar'];
const STORAGE_KEY = 'mychat_lang';

// Public dictionaries. Add new strings here. Keep both languages in
// lockstep — see contributing note above.
export const dictionaries = {
  en: {
    common: {
      brand: 'mychat',
      login: 'Log in',
      signup: 'Get Started',
      logout: 'Log out',
      privacy: 'Privacy',
      terms: 'Terms',
      contact: 'Contact',
      dataDeletion: 'Data Deletion',
      learnMore: 'Learn more',
      copyright: '© 2026 All rights reserved.',
    },
    landing: {
      nav: {
        features: 'Features',
        how: 'How it works',
        pricing: 'Pricing',
        status: 'Status',
      },
      hero: {
        badge: 'Instagram automation for connected business accounts',
        title1: 'Turn Instagram',
        title2: 'conversations into',
        titleEm: 'workflows.',
        subtitle:
          'Connect your Instagram business account, build comment-and-DM rules in a visual flow, and manage every automation from one workspace.',
        cta: 'Get Started',
        secondaryCta: 'View status',
      },
      preview: {
        triggerLabel: 'TRIGGER',
        triggerTitle: 'New Comment',
        triggerHint: 'on a keyword you pick',
        messageLabel: 'MESSAGE',
        messageTitle: 'Opening DM',
        messageHint: 'sent automatically',
        actionLabel: 'ACTION',
        actionTitle: 'Reply or Link',
        actionHint: 'choose what happens next',
      },
      features: {
        badge: 'Features',
        title: 'Everything you need to scale Instagram conversations',
        subtitle: 'Built around your own connected account — no shared inbox, no shared data.',
        items: {
          commentTrigger: {
            title: 'Comment triggers',
            description:
              'Reply automatically when a comment mentions a keyword you picked — or react to any new comment on a specific post.',
          },
          dmAutomation: {
            title: 'Smart DM automation',
            description:
              'Send a follow-up DM to commenters with a custom message, an attached link, or a follow-gate that asks them to follow first.',
          },
          dashboard: {
            title: 'Real-time dashboard',
            description:
              'See replies sent, DMs delivered, and link clicks as they happen — pulled directly from your own Instagram activity.',
          },
          deliveryAware: {
            title: 'Delivery-aware system',
            description:
              "Knows when Instagram blocks a DM (24-hour window, user opted out, post you don't own) and quietly routes around it instead of failing loudly.",
          },
          multiAccount: {
            title: 'Multi-account support',
            description:
              'Connect multiple Instagram business accounts and switch between them in one click. Automations stay scoped to the account you select.',
          },
          conversionTracking: {
            title: 'Conversion tracking',
            description:
              'Every link you send in a DM is tracked. See exactly how many users clicked through, on which day, from which automation.',
          },
        },
      },
      how: {
        badge: 'How it works',
        title: 'Up and running in three steps',
        steps: [
          {
            num: '01',
            title: 'Connect Instagram',
            desc: 'Link your business or creator account through the in-app setup — takes 30 seconds.',
          },
          {
            num: '02',
            title: 'Build your first rule',
            desc: 'Pick a post, choose a keyword, write the public reply and the DM message. Activate when ready.',
          },
          {
            num: '03',
            title: 'Watch it work',
            desc: 'New comments trigger your rule in real time. Replies and DMs go out automatically — you only review the analytics.',
          },
        ],
      },
      cta: {
        title: 'Ready to manage Instagram automations?',
        body: 'Create your free account and connect Instagram when you are ready to automate real conversations.',
        button: 'Get Started',
      },
    },
    status: {
      title: 'Service status',
    },
  },
  ar: {
    common: {
      brand: 'mychat',
      login: 'تسجيل الدخول',
      signup: 'ابدأ مجاناً',
      logout: 'تسجيل الخروج',
      privacy: 'الخصوصية',
      terms: 'الشروط',
      contact: 'تواصل',
      dataDeletion: 'حذف البيانات',
      learnMore: 'اعرف المزيد',
      copyright: '© ٢٠٢٦ — جميع الحقوق محفوظة.',
    },
    landing: {
      nav: {
        features: 'المميزات',
        how: 'طريقة العمل',
        pricing: 'الأسعار',
        status: 'حالة الخدمة',
      },
      hero: {
        badge: 'أتمتة Instagram لحسابات الأعمال المربوطة',
        title1: 'حوّل محادثات',
        title2: 'إنستجرام إلى',
        titleEm: 'تدفقات عمل.',
        subtitle:
          'اربط حساب Instagram Business بتاعك، اعمل قواعد للتعليقات والرسائل في flow بصري، وأدر كل الأتمتة من مكان واحد.',
        cta: 'ابدأ مجاناً',
        secondaryCta: 'حالة الخدمة',
      },
      preview: {
        triggerLabel: 'الحدث',
        triggerTitle: 'تعليق جديد',
        triggerHint: 'على كلمة مفتاحية تختارها',
        messageLabel: 'الرسالة',
        messageTitle: 'رسالة افتتاحية',
        messageHint: 'تُرسل تلقائياً',
        actionLabel: 'الإجراء',
        actionTitle: 'رد أو لينك',
        actionHint: 'اختر ما يحدث بعدها',
      },
      features: {
        badge: 'المميزات',
        title: 'كل ما تحتاجه لتوسيع نطاق المحادثات على Instagram',
        subtitle: 'مبني حول حسابك أنت — مفيش inbox مشترك، مفيش بيانات مشتركة.',
        items: {
          commentTrigger: {
            title: 'محفّزات على التعليقات',
            description:
              'رد تلقائياً لما تعليق يذكر كلمة مفتاحية اخترتها، أو تفاعل مع أي تعليق جديد على بوست محدد.',
          },
          dmAutomation: {
            title: 'أتمتة ذكية للرسائل',
            description:
              'ابعت DM متابعة للمعلّقين برسالة مخصصة، رابط، أو "follow-gate" يطلب منهم يتابعوك أولاً.',
          },
          dashboard: {
            title: 'لوحة تحكم لحظية',
            description:
              'اعرف الردود المُرسلة، الـ DMs اللي وصلت، وضغطات الروابط لحظة بلحظة — من نشاط حسابك مباشرة.',
          },
          deliveryAware: {
            title: 'نظام واعي بالتسليم',
            description:
              'بيعرف إن Instagram رفض الـ DM (نافذة ٢٤ ساعة منتهية، المستخدم رافض الرسائل، بوست مش حسابك) ويتجنبها بهدوء بدل ما يطلع فشل.',
          },
          multiAccount: {
            title: 'دعم حسابات متعددة',
            description:
              'اربط أكتر من حساب Instagram Business وبدّل بينهم بضغطة. كل أتمتة مرتبطة بالحساب اللي اخترته.',
          },
          conversionTracking: {
            title: 'تتبع التحويلات',
            description:
              'كل رابط في DM متتبع. شوف كم مستخدم ضغط، في أي يوم، من أي automation بالضبط.',
          },
        },
      },
      how: {
        badge: 'طريقة العمل',
        title: 'جاهز في ٣ خطوات',
        steps: [
          {
            num: '٠١',
            title: 'اربط Instagram',
            desc: 'اربط حساب Business أو Creator من الإعدادات داخل التطبيق — يبقى تمام في ٣٠ ثانية.',
          },
          {
            num: '٠٢',
            title: 'اعمل أول قاعدة',
            desc: 'اختار بوست، حدّد كلمة مفتاحية، اكتب الرد العام ورسالة الـ DM. فعّل لما تجهز.',
          },
          {
            num: '٠٣',
            title: 'شوفها تشتغل',
            desc: 'التعليقات الجديدة تشغّل القاعدة لحظياً. الردود والـ DMs تخرج تلقائياً — أنت بس تتفرج على التحليلات.',
          },
        ],
      },
      cta: {
        title: 'جاهز تدير أتمتة Instagram؟',
        body: 'اعمل حسابك المجاني واربط Instagram لما تكون جاهز لأتمتة محادثات حقيقية.',
        button: 'ابدأ مجاناً',
      },
    },
    status: {
      title: 'حالة الخدمة',
    },
  },
};


function resolveKey(obj, path) {
  return path.split('.').reduce(
    (cur, segment) => (cur && typeof cur === 'object' ? cur[segment] : undefined),
    obj,
  );
}


function readSavedLang() {
  try {
    const v = localStorage.getItem(STORAGE_KEY);
    if (SUPPORTED_LANGS.includes(v)) return v;
  } catch (_) { /* ignore */ }
  // Browser preference fallback.
  try {
    const browser = (navigator.language || 'en').slice(0, 2).toLowerCase();
    if (SUPPORTED_LANGS.includes(browser)) return browser;
  } catch (_) { /* ignore */ }
  return 'en';
}


function applyHtmlLangAttributes(lang) {
  if (typeof document === 'undefined') return;
  document.documentElement.lang = lang;
  document.documentElement.dir = lang === 'ar' ? 'rtl' : 'ltr';
}


const I18nContext = createContext(null);


export function I18nProvider({ children }) {
  const [lang, setLangState] = useState(readSavedLang);

  useEffect(() => {
    applyHtmlLangAttributes(lang);
    try { localStorage.setItem(STORAGE_KEY, lang); } catch (_) { /* ignore */ }
  }, [lang]);

  const setLang = useCallback((next) => {
    if (!SUPPORTED_LANGS.includes(next)) return;
    setLangState(next);
  }, []);

  const t = useCallback(
    (key, fallback) => {
      const value = resolveKey(dictionaries[lang], key);
      if (value !== undefined) return value;
      // Fall back to English with a warning so missing translations
      // are caught at dev time.
      const enValue = resolveKey(dictionaries.en, key);
      if (enValue !== undefined) {
        if (lang !== 'en' && typeof console !== 'undefined') {
          console.warn(`[i18n] missing ${lang} translation for "${key}"`);
        }
        return enValue;
      }
      if (typeof console !== 'undefined') {
        console.warn(`[i18n] unknown key "${key}"`);
      }
      return fallback !== undefined ? fallback : key;
    },
    [lang],
  );

  const value = useMemo(() => ({ lang, setLang, t, isRtl: lang === 'ar' }), [lang, setLang, t]);
  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}


export function useTranslation() {
  const ctx = useContext(I18nContext);
  if (!ctx) {
    // Safe fallback so components don't crash when rendered outside
    // the provider (e.g. error boundaries).
    return {
      lang: 'en',
      setLang: () => {},
      isRtl: false,
      t: (key, fallback) => fallback !== undefined ? fallback : key,
    };
  }
  return ctx;
}

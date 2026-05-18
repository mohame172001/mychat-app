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
      save: 'Save',
      saveChanges: 'Save changes',
      cancel: 'Cancel',
      delete: 'Delete',
      edit: 'Edit',
      back: 'Back',
      refresh: 'Refresh',
      retry: 'Retry',
      loading: 'Loading…',
      yes: 'Yes',
      no: 'No',
      connect: 'Connect',
      disconnect: 'Disconnect',
      connected: 'Connected',
      notConnected: 'Not connected',
      newAutomation: 'New Automation',
      createAutomation: 'Create Automation',
      goLive: 'Go Live',
      pause: 'Pause',
      resume: 'Resume',
      activate: 'Activate',
      deactivate: 'Deactivate',
      copyEmail: 'Email copied',
    },
    nav: {
      dashboard: 'Dashboard',
      automations: 'Automations',
      dmAutomation: 'DM Automation',
      billing: 'Billing',
      settings: 'Settings',
      admin: 'Admin',
      helpSupport: 'Help & Support',
    },
    topbar: {
      newAutomation: 'New Automation',
      connected: 'Connected',
      notConnected: 'Not connected',
    },
    dashboard: {
      greeting: 'Good morning, {name}',
      subtitle: 'Here is what is happening with your Instagram automations today.',
      cards: {
        totalContacts: 'Total Contacts',
        activeAutomations: 'Active Automations',
        messagesSent: 'Messages Sent',
        conversionRate: 'Conversion Rate',
      },
      weeklyTitle: 'Weekly Performance',
      weeklySubtitle: 'Messages sent vs conversions',
      legendMessages: 'Messages',
      legendConversions: 'Conversions',
      topAutomations: 'Top Automations',
      viewAll: 'View all',
      noAutomations: 'No automations yet',
      onboarding: {
        titleConnected: "You're set — create your first automation",
        titleNew: 'Welcome to MyChat 👋',
        bodyConnected:
          'Now build a comment automation: when someone comments on your post, MyChat will reply publicly and send them a DM.',
        bodyNew:
          'To start automating Instagram comments + DMs, connect your business account first. It takes 30 seconds.',
        step1: 'Connect your Instagram business account',
        step2: 'Create a comment automation rule',
        step3: 'Comments roll in — MyChat replies + DMs them automatically',
        connectCta: 'Connect Instagram',
        createCta: 'Create automation',
      },
      error: {
        timeout: 'Dashboard request timed out. Please try again.',
        sessionExpired: 'Session expired. Please log in again.',
        serverError: 'Dashboard server error. Please try again later.',
        connectInstagram: 'Connect or reconnect Instagram to load dashboard data.',
        network: 'Network error. Please check your connection.',
        generic: 'Dashboard data could not be loaded.',
        refreshFailedCache: "Couldn't refresh. Showing the latest available data.",
      },
    },
    auth: {
      login: {
        title: 'Welcome back',
        subtitle: 'Log in to manage your Instagram automations.',
        emailOrUsername: 'Email or username',
        password: 'Password',
        submit: 'Log in',
        forgot: 'Forgot password?',
        noAccount: "Don't have an account?",
        signupLink: 'Sign up',
        invalidCreds: 'Invalid username or password',
      },
      signup: {
        title: 'Create your MyChat account',
        subtitle: 'Connect Instagram once you finish — takes 30 seconds.',
        email: 'Email',
        username: 'Username',
        password: 'Password',
        passwordHint: 'At least 8 characters.',
        submit: 'Create account',
        haveAccount: 'Already have an account?',
        loginLink: 'Log in',
        usernameTaken: 'Username already taken',
        emailRegistered: 'Email already registered',
      },
      forgot: {
        title: 'Reset your password',
        subtitle: "Enter your email and we'll send you a reset link.",
        email: 'Email',
        submit: 'Send reset link',
        sent: "If that email is registered, a reset link is on the way.",
        back: 'Back to login',
      },
      reset: {
        title: 'Set a new password',
        newPassword: 'New password',
        confirmPassword: 'Confirm password',
        submit: 'Update password',
        mismatch: 'Passwords do not match',
        success: 'Password updated. You can log in now.',
      },
    },
    automations: {
      pageTitle: 'Automations',
      pageSubtitle: 'Build Instagram comment automations for new comments only.',
      tabs: { all: 'All', active: 'Active', paused: 'Paused', draft: 'Draft' },
      searchPlaceholder: 'Search automations…',
      empty: 'No automations yet. Create your first one.',
      status: { active: 'active', paused: 'paused', draft: 'draft' },
      fired: 'Fired',
      sent: 'sent',
      activeSince: 'Active since',
      createdNotice: 'Automation is live',
      updatedNotice: 'Automation updated. Stats preserved.',
      createFailed: 'Failed to create automation',
      builder: {
        editTitle: 'Edit automation',
        createTitle: 'Automations',
        editDesc: 'Update this Instagram comment automation while keeping its stats.',
        createDesc: 'Create an Instagram comment automation inside your workspace.',
        saveChanges: 'Save Changes',
        goLive: 'Go Live',
      },
    },
    settings: {
      title: 'Settings',
      subtitle: 'Manage your account, Instagram connection and preferences.',
      tabs: {
        profile: 'Profile',
        instagram: 'Instagram',
        notifications: 'Notifications',
        billing: 'Billing',
        security: 'Security',
      },
      profile: {
        heading: 'Profile',
        description: 'Your account profile. Name and username are editable.',
        fullName: 'Full name',
        username: 'Username',
        email: 'Email',
        emailHint: 'Email changes require a verification flow that is not enabled yet. Contact support if you need it changed.',
        saved: 'Profile updated',
      },
      security: {
        heading: 'Security',
        currentPassword: 'Current password',
        newPassword: 'New password',
        confirmNewPassword: 'Confirm new password',
        updateButton: 'Update password',
        updated: 'Password updated.',
        wrongCurrent: 'Current password is incorrect.',
      },
      notifications: {
        heading: 'Notification preferences',
        email: 'Email notifications',
        push: 'Push notifications',
        weekly: 'Weekly summary',
        saved: 'Notification preferences saved.',
      },
    },
    billing: {
      title: 'Billing',
      subtitle: 'Your current plan, this month\'s usage, and remaining quota.',
      currentPlan: 'Current plan',
      thisMonth: 'This month',
      plans: 'Plans',
      billingNotEnabled: 'Billing is not enabled yet.',
      contactSupport: 'Your current plan limits are enforced to keep the beta stable. Contact support during beta to change your plan. Plan upgrades with payment will be available later.',
      perMonth: 'per month',
      currentPlanLabel: 'Current plan',
    },
    dmAutomation: {
      title: 'Instagram DM Automation',
      subtitle: 'Auto-reply to direct messages based on keyword rules. Independent of comments.',
      createTitle: 'Create DM rule',
      ruleName: 'Rule name',
      keyword: 'Keyword',
      matchMode: 'Match mode',
      replyMessage: 'Reply message',
      active: 'Active',
      saveRule: 'Save rule',
      rulesCount: 'DM rules',
      noRules: 'No rules yet.',
      recentEventsTitle: 'Recent DM events',
      noEvents: 'No DM events yet.',
    },
    status: {
      title: 'Service status',
      operational: 'Operational',
      partialOutage: 'Partial outage',
      majorOutage: 'Major outage',
      unknown: 'Unknown',
      allOperational: 'All systems operational',
      allOperationalBody: 'Every MyChat subsystem is responding normally.',
      partialTitle: 'Partial service disruption',
      partialBody: 'Some MyChat subsystems are degraded. Automations may run slower than usual.',
      majorTitle: 'Major outage',
      majorBody: 'One or more critical subsystems are unavailable. We are investigating.',
      unknownTitle: 'Status unavailable',
      unknownBody: 'We could not determine the service status right now.',
      reach: 'For incidents not yet reflected here, email',
      support: 'support',
      checkedAt: 'checked at',
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
    // Egyptian-Arabic, polished business voice. NOT literal —
    // sentences are rewritten so they sound natural to a Cairo SaaS
    // user, while staying professional (no slang, no joking, no
    // overly formal MSA). Brand names (Instagram, Business, DM)
    // intentionally stay in Latin script — that's how Egyptian
    // marketing pages actually write them.
    common: {
      brand: 'mychat',
      login: 'تسجيل الدخول',
      signup: 'ابدأ مجاناً',
      logout: 'تسجيل الخروج',
      privacy: 'الخصوصية',
      terms: 'الشروط',
      contact: 'كلّمنا',
      dataDeletion: 'حذف البيانات',
      learnMore: 'اعرف أكتر',
      copyright: '© ٢٠٢٦ — كل الحقوق محفوظة.',
      save: 'حفظ',
      saveChanges: 'احفظ التعديلات',
      cancel: 'إلغاء',
      delete: 'حذف',
      edit: 'تعديل',
      back: 'رجوع',
      refresh: 'تحديث',
      retry: 'حاول تاني',
      loading: 'جارٍ التحميل…',
      yes: 'تمام',
      no: 'لأ',
      connect: 'اربط',
      disconnect: 'فصل الربط',
      connected: 'متصل',
      notConnected: 'مش متصل',
      newAutomation: 'Automation جديدة',
      createAutomation: 'اعمل Automation',
      goLive: 'فعّلها',
      pause: 'إيقاف مؤقت',
      resume: 'تشغيل',
      activate: 'فعّل',
      deactivate: 'وقّف',
      copyEmail: 'الإيميل اتنسخ',
    },
    nav: {
      dashboard: 'الرئيسية',
      automations: 'Automations',
      dmAutomation: 'أتمتة الرسائل',
      billing: 'الفواتير',
      settings: 'الإعدادات',
      admin: 'الإدارة',
      helpSupport: 'مساعدة ودعم',
    },
    topbar: {
      newAutomation: 'Automation جديدة',
      connected: 'متصل',
      notConnected: 'مش متصل',
    },
    dashboard: {
      greeting: 'يوم سعيد، {name}',
      subtitle: 'إيه اللي حصل النهاردة في الـ automations بتاعتك على Instagram.',
      cards: {
        totalContacts: 'إجمالي الـ Contacts',
        activeAutomations: 'Automations شغّالة',
        messagesSent: 'الرسائل اللي اتبعتت',
        conversionRate: 'نسبة التحويل',
      },
      weeklyTitle: 'أداء الأسبوع',
      weeklySubtitle: 'الرسائل اللي اتبعتت مقابل التحويلات',
      legendMessages: 'رسائل',
      legendConversions: 'تحويلات',
      topAutomations: 'أهم الـ Automations',
      viewAll: 'شوف الكل',
      noAutomations: 'مفيش automations لسه',
      onboarding: {
        titleConnected: 'تمام — يلا نعمل أول Automation',
        titleNew: 'مرحبا بيك في MyChat 👋',
        bodyConnected:
          'دلوقتي ابني automation للتعليقات: لما حد يعلّق على بوستك، MyChat هيرد عليه عام ويبعتله DM.',
        bodyNew:
          'عشان تبدأ تأتمت تعليقات + DMs على Instagram، اربط حسابك Business الأول. ٣٠ ثانية وخلاص.',
        step1: 'اربط حساب Instagram Business بتاعك',
        step2: 'اعمل أول قاعدة للتعليقات',
        step3: 'التعليقات بتيجي — MyChat بيرد ويبعت DMs لوحده',
        connectCta: 'اربط Instagram',
        createCta: 'اعمل Automation',
      },
      error: {
        timeout: 'الطلب أخد وقت أكتر من المعتاد. حاول تاني.',
        sessionExpired: 'الجلسة انتهت. سجّل دخولك من جديد.',
        serverError: 'فيه مشكلة في السيرفر. جرب بعد شوية.',
        connectInstagram: 'اربط أو أعد ربط Instagram عشان نشغّل الـ dashboard.',
        network: 'مفيش إنترنت. اتأكد من الاتصال.',
        generic: 'تعذّر تحميل بيانات الـ dashboard.',
        refreshFailedCache: 'مقدرتش أحدّث. بنعرض آخر بيانات متاحة.',
      },
    },
    auth: {
      login: {
        title: 'يا أهلاً برجوعك',
        subtitle: 'سجّل دخول عشان تدير الـ automations بتاعتك.',
        emailOrUsername: 'الإيميل أو اسم المستخدم',
        password: 'كلمة السر',
        submit: 'تسجيل الدخول',
        forgot: 'نسيت كلمة السر؟',
        noAccount: 'مش عندك حساب؟',
        signupLink: 'سجّل واحد جديد',
        invalidCreds: 'الإيميل أو كلمة السر مش صح',
      },
      signup: {
        title: 'اعمل حسابك على MyChat',
        subtitle: 'تربط Instagram بعد ما تخلص — هياخد منك ٣٠ ثانية.',
        email: 'الإيميل',
        username: 'اسم المستخدم',
        password: 'كلمة السر',
        passwordHint: 'على الأقل ٨ حروف.',
        submit: 'اعمل حساب',
        haveAccount: 'عندك حساب أصلاً؟',
        loginLink: 'سجّل دخول',
        usernameTaken: 'اسم المستخدم متاخد',
        emailRegistered: 'الإيميل ده مسجّل قبل كده',
      },
      forgot: {
        title: 'إعادة تعيين كلمة السر',
        subtitle: 'دخّل إيميلك وهنبعتلك رابط تعيين جديد.',
        email: 'الإيميل',
        submit: 'ابعتلي رابط',
        sent: 'لو الإيميل ده مسجّل عندنا، الرابط في الطريق.',
        back: 'رجوع لتسجيل الدخول',
      },
      reset: {
        title: 'اعمل كلمة سر جديدة',
        newPassword: 'كلمة السر الجديدة',
        confirmPassword: 'أكّد كلمة السر',
        submit: 'حدّث كلمة السر',
        mismatch: 'كلمتي السر مش متطابقتين',
        success: 'تم تحديث كلمة السر. سجّل دخول دلوقتي.',
      },
    },
    automations: {
      pageTitle: 'Automations',
      pageSubtitle: 'اعمل automations للتعليقات الجديدة على Instagram.',
      tabs: { all: 'الكل', active: 'شغّالة', paused: 'موقوفة', draft: 'مسودة' },
      searchPlaceholder: 'ابحث في الـ automations…',
      empty: 'مفيش automations لسه. اعمل أول واحدة.',
      status: { active: 'شغّالة', paused: 'موقوفة', draft: 'مسودة' },
      fired: 'اشتغلت',
      sent: 'بعتت',
      activeSince: 'شغّالة من',
      createdNotice: 'الـ Automation شغّالة',
      updatedNotice: 'تم تحديث الـ Automation. الإحصائيات محفوظة.',
      createFailed: 'فشل إنشاء الـ Automation',
      builder: {
        editTitle: 'تعديل Automation',
        createTitle: 'Automations',
        editDesc: 'حدّث الـ automation وخلي الإحصائيات بتاعتها كما هي.',
        createDesc: 'اعمل automation جديدة للتعليقات على Instagram.',
        saveChanges: 'احفظ التعديلات',
        goLive: 'فعّلها',
      },
    },
    settings: {
      title: 'الإعدادات',
      subtitle: 'إدارة حسابك، ربط Instagram، والتفضيلات.',
      tabs: {
        profile: 'البروفايل',
        instagram: 'Instagram',
        notifications: 'الإشعارات',
        billing: 'الفواتير',
        security: 'الأمان',
      },
      profile: {
        heading: 'البروفايل',
        description: 'بيانات حسابك. الاسم واسم المستخدم تقدر تعدّلهم.',
        fullName: 'الاسم بالكامل',
        username: 'اسم المستخدم',
        email: 'الإيميل',
        emailHint: 'تغيير الإيميل بيحتاج خطوة تحقق مش مفعّلة حالياً. كلّم الدعم لو محتاج تغييره.',
        saved: 'تم تحديث البروفايل',
      },
      security: {
        heading: 'الأمان',
        currentPassword: 'كلمة السر الحالية',
        newPassword: 'كلمة السر الجديدة',
        confirmNewPassword: 'أكّد كلمة السر الجديدة',
        updateButton: 'حدّث كلمة السر',
        updated: 'تم تحديث كلمة السر.',
        wrongCurrent: 'كلمة السر الحالية مش صح.',
      },
      notifications: {
        heading: 'تفضيلات الإشعارات',
        email: 'إشعارات بالإيميل',
        push: 'إشعارات فورية',
        weekly: 'ملخص أسبوعي',
        saved: 'تم حفظ التفضيلات.',
      },
    },
    billing: {
      title: 'الفواتير',
      subtitle: 'خطتك الحالية، استهلاك الشهر، والمتبقي.',
      currentPlan: 'الخطة الحالية',
      thisMonth: 'الشهر ده',
      plans: 'الخطط',
      billingNotEnabled: 'الفوترة مش مفعّلة لسه.',
      contactSupport:
        'حدود خطتك الحالية مطبّقة عشان نحافظ على استقرار البيتا. كلّم الدعم لو محتاج تغيير الخطة. الترقية بالدفع هتكون متاحة لاحقاً.',
      perMonth: 'شهرياً',
      currentPlanLabel: 'الخطة الحالية',
    },
    dmAutomation: {
      title: 'أتمتة رسائل Instagram',
      subtitle: 'رد تلقائياً على الرسائل المباشرة حسب كلمات محددة. مستقل عن التعليقات.',
      createTitle: 'إنشاء قاعدة DM',
      ruleName: 'اسم القاعدة',
      keyword: 'الكلمة',
      matchMode: 'نوع المطابقة',
      replyMessage: 'رسالة الرد',
      active: 'مفعّلة',
      saveRule: 'احفظ القاعدة',
      rulesCount: 'قواعد الـ DM',
      noRules: 'مفيش قواعد لسه.',
      recentEventsTitle: 'آخر أحداث DM',
      noEvents: 'مفيش أحداث DM لسه.',
    },
    status: {
      title: 'حالة الخدمة',
      operational: 'شغّال',
      partialOutage: 'تعطّل جزئي',
      majorOutage: 'تعطّل كبير',
      unknown: 'غير معروف',
      allOperational: 'كل الأنظمة شغّالة',
      allOperationalBody: 'كل الأنظمة في MyChat بترد بشكل طبيعي.',
      partialTitle: 'تعطّل جزئي في الخدمة',
      partialBody: 'فيه أنظمة شغّالة بأداء أقل. الـ automations ممكن تكون أبطأ من المعتاد.',
      majorTitle: 'تعطّل كبير',
      majorBody: 'فيه نظام أو أكتر مش متاح. بنحقق في المشكلة دلوقتي.',
      unknownTitle: 'حالة الخدمة مش متاحة',
      unknownBody: 'مقدرناش نحدد حالة الخدمة دلوقتي.',
      reach: 'لو فيه عطل لسه مش ظاهر هنا، ابعتلنا إيميل',
      support: 'الدعم',
      checkedAt: 'آخر فحص',
    },
    landing: {
      nav: {
        features: 'المميزات',
        how: 'إزاي بيشتغل',
        pricing: 'الأسعار',
        status: 'حالة الخدمة',
      },
      hero: {
        badge: 'حل أتمتة لحسابات Instagram Business',
        title1: 'خلّي تعليقات',
        title2: 'Instagram تشتغل',
        titleEm: 'لوحدها.',
        subtitle:
          'اربط حساب Instagram Business بتاعك، اعمل قواعد للتعليقات والـ DMs بـ flow بصري سهل، وادير كل حاجة من مكان واحد.',
        cta: 'ابدأ مجاناً',
        secondaryCta: 'حالة الخدمة',
      },
      preview: {
        triggerLabel: 'الحدث',
        triggerTitle: 'تعليق جديد',
        triggerHint: 'على كلمة بتختارها أنت',
        messageLabel: 'الرسالة',
        messageTitle: 'DM افتتاحي',
        messageHint: 'بيتبعت لوحده',
        actionLabel: 'الإجراء',
        actionTitle: 'رد أو لينك',
        actionHint: 'اختار اللي يحصل بعدها',
      },
      features: {
        badge: 'المميزات',
        title: 'كل أدواتك في مكان واحد عشان تكبّر شغلك على Instagram',
        subtitle: 'كل حاجة مربوطة بحسابك أنت — مفيش inbox مشترك، ومحدش بيشوف بياناتك.',
        items: {
          commentTrigger: {
            title: 'محفّزات على التعليقات',
            description:
              'رد فوراً على أي تعليق فيه كلمة بتختارها أنت — أو خليه يتعامل مع كل تعليق جديد على بوست محدد.',
          },
          dmAutomation: {
            title: 'أتمتة ذكية للـ DMs',
            description:
              'ابعت DM متابعة لكل واحد علّق — رسالة مخصصة، رابط، أو "follow-gate" يطلب منه يتابعك الأول قبل ما يستلم الرسالة.',
          },
          dashboard: {
            title: 'لوحة تحكم لحظية',
            description:
              'شوف الردود اللي اتبعتت، الـ DMs اللي وصلت، والضغطات على اللينكات لحظة بلحظة — من نشاط حسابك مباشرة.',
          },
          deliveryAware: {
            title: 'نظام واعي بالتوصيل',
            description:
              'بيفهم لما Instagram يرفض الـ DM (نافذة الـ ٢٤ ساعة خلصت، المستخدم رافض الرسائل، أو بوست مش حسابك) ويتعامل معاها بهدوء بدل ما تظهر كأنها فشل.',
          },
          multiAccount: {
            title: 'دعم أكتر من حساب',
            description:
              'اربط أكتر من حساب Instagram Business وبدّل بينهم بضغطة. كل automation بتشتغل على الحساب اللي أنت محدده — مفيش خلط.',
          },
          conversionTracking: {
            title: 'تتبع التحويلات',
            description:
              'كل لينك بتبعته في DM متتبَع. شوف كم واحد ضغط، في أنهي يوم، ومن أنهي automation بالظبط.',
          },
        },
      },
      how: {
        badge: 'إزاي بيشتغل',
        title: 'تشتغل في ٣ خطوات',
        steps: [
          {
            num: '٠١',
            title: 'اربط Instagram',
            desc: 'اربط حساب Business أو Creator من إعدادات التطبيق — هياخد منك ٣٠ ثانية وخلاص.',
          },
          {
            num: '٠٢',
            title: 'اعمل أول قاعدة',
            desc: 'اختار البوست، حدّد الكلمة، اكتب الرد العام ورسالة الـ DM. فعّلها لما تكون جاهز.',
          },
          {
            num: '٠٣',
            title: 'سيبها تشتغل',
            desc: 'كل تعليق جديد بيشغّل القاعدة على طول. الردود والـ DMs بتطلع لوحدها — أنت بس تتابع التحليلات.',
          },
        ],
      },
      cta: {
        title: 'جاهز تأتمت تعليقاتك على Instagram؟',
        body: 'اعمل حسابك المجاني، واربط Instagram لما تكون جاهز تشتغل على محادثات حقيقية.',
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
  // NOTE: we intentionally do NOT flip the document direction to RTL
  // when Arabic is active. The product owner prefers the layout stays
  // left-to-right in both languages — only the text content changes.
  // Arabic characters still render right-to-left within their own
  // text runs (handled by the browser); panels/grids/columns stay LTR.
  document.documentElement.dir = 'ltr';
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

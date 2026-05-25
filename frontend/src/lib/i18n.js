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
        subtitles: {
          totalContacts: 'All-time · Active account',
          activeAutomations: 'Across this account',
          messagesSent: 'In selected range',
          conversionRate: 'In selected range',
        },
        secondary: {
          commentsProcessed: 'Comments Processed',
          publicReplies: 'Public Replies',
          openingDms: 'Opening DMs',
          linkClicks: 'Link Clicks',
          connectedAccounts: 'Connected Accounts',
        },
      },
      range: {
        label: 'Range',
        '24h': '24h',
        '7d': '7 days',
        '30d': '30 days',
        all: 'All time',
      },
      weeklyTitle: 'Weekly Performance',
      weeklySubtitle: 'Messages sent vs conversions',
      legendMessages: 'Messages',
      legendConversions: 'Conversions',
      performanceTitles: {
        '24h': 'Performance — Last 24 hours',
        '7d': 'Performance — Last 7 days',
        '30d': 'Performance — Last 30 days',
        all: 'Performance — All time',
      },
      topAutomations: 'Top Automations',
      topAutomationsSubtitle: 'Active first · by sends',
      viewAll: 'View all',
      noAutomations: 'No automations yet',
      moreStats: 'More stats',
      lessStats: 'Less stats',
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
    // Professional Modern Standard Arabic (MSA) suitable for SaaS,
    // modeled after Arabic-first product sites (araby.ai, Notion AR,
    // Slack AR). Voice rules:
    //   - Active verbs, present tense, concise.
    //   - "كلمة المرور" not "كلمة السر" (industry standard).
    //   - "البريد الإلكتروني" not "الإيميل" (formal Arabic).
    //   - "إنشاء حساب" / "تسجيل الدخول" (standard SaaS phrasing).
    //   - "الأتمتة / أتمتات" — accepted in Arabic tech writing.
    //   - "لوحة التحكم" for Dashboard (standard).
    //   - Brand names (Instagram, MyChat) intentionally stay Latin.
    //   - No regional dialect, no slang. Confident, contemporary.
    //   - Sentences are REWRITTEN to read native, not translated word-
    //     for-word from English.
    common: {
      brand: 'mychat',
      login: 'تسجيل الدخول',
      signup: 'ابدأ مجاناً',
      logout: 'تسجيل الخروج',
      privacy: 'سياسة الخصوصية',
      terms: 'الشروط والأحكام',
      contact: 'تواصل معنا',
      dataDeletion: 'حذف البيانات',
      learnMore: 'اعرف المزيد',
      copyright: '© ٢٠٢٦ — جميع الحقوق محفوظة.',
      save: 'حفظ',
      saveChanges: 'حفظ التغييرات',
      cancel: 'إلغاء',
      delete: 'حذف',
      edit: 'تعديل',
      back: 'رجوع',
      refresh: 'تحديث',
      retry: 'إعادة المحاولة',
      loading: 'جارٍ التحميل…',
      yes: 'نعم',
      no: 'لا',
      connect: 'ربط الحساب',
      disconnect: 'إلغاء الربط',
      connected: 'مُتصل',
      notConnected: 'غير مُتصل',
      newAutomation: 'أتمتة جديدة',
      createAutomation: 'إنشاء أتمتة',
      goLive: 'تفعيل',
      pause: 'إيقاف مؤقت',
      resume: 'استئناف',
      activate: 'تفعيل',
      deactivate: 'إيقاف',
      copyEmail: 'تم نسخ البريد الإلكتروني',
    },
    nav: {
      dashboard: 'لوحة التحكم',
      automations: 'الأتمتات',
      dmAutomation: 'أتمتة الرسائل',
      billing: 'الفوترة',
      settings: 'الإعدادات',
      admin: 'الإدارة',
      helpSupport: 'الدعم والمساعدة',
    },
    topbar: {
      newAutomation: 'أتمتة جديدة',
      connected: 'مُتصل',
      notConnected: 'غير مُتصل',
    },
    dashboard: {
      greeting: 'أهلاً، {name}',
      subtitle: 'إليك ملخص نشاط أتمتاتك على Instagram اليوم.',
      cards: {
        totalContacts: 'إجمالي جهات الاتصال',
        activeAutomations: 'الأتمتات النشطة',
        messagesSent: 'الرسائل المُرسلة',
        conversionRate: 'معدّل التحويل',
        subtitles: {
          totalContacts: 'كل الوقت · الحساب النشط',
          activeAutomations: 'على هذا الحساب',
          messagesSent: 'في النطاق المحدّد',
          conversionRate: 'في النطاق المحدّد',
        },
        secondary: {
          commentsProcessed: 'التعليقات المُعالَجة',
          publicReplies: 'الردود العامة',
          openingDms: 'رسائل البدء',
          linkClicks: 'نقرات الروابط',
          connectedAccounts: 'الحسابات المربوطة',
        },
      },
      range: {
        label: 'النطاق',
        '24h': '24 ساعة',
        '7d': '7 أيام',
        '30d': '30 يومًا',
        all: 'كل الوقت',
      },
      weeklyTitle: 'الأداء الأسبوعي',
      weeklySubtitle: 'الرسائل المُرسلة مقابل التحويلات',
      legendMessages: 'الرسائل',
      legendConversions: 'التحويلات',
      performanceTitles: {
        '24h': 'الأداء — آخر 24 ساعة',
        '7d': 'الأداء — آخر 7 أيام',
        '30d': 'الأداء — آخر 30 يومًا',
        all: 'الأداء — كل الوقت',
      },
      topAutomations: 'أبرز الأتمتات',
      topAutomationsSubtitle: 'النشطة أولًا · حسب الإرسال',
      viewAll: 'عرض الكل',
      noAutomations: 'لا توجد أتمتات بعد',
      moreStats: 'مزيد من الإحصاءات',
      lessStats: 'إخفاء الإحصاءات',
      onboarding: {
        titleConnected: 'حسابك جاهز — أنشئ أول أتمتة',
        titleNew: 'أهلاً بك في MyChat 👋',
        bodyConnected:
          'الخطوة التالية: أنشئ أتمتة للتعليقات. عندما يعلّق أحد على منشورك، سيرد MyChat بشكل عام ويُرسل له رسالة خاصة.',
        bodyNew:
          'لبدء أتمتة التعليقات والرسائل على Instagram، اربط حساب الأعمال أولاً. تستغرق العملية ٣٠ ثانية فقط.',
        step1: 'اربط حساب Instagram للأعمال',
        step2: 'أنشئ قاعدة أتمتة للتعليقات',
        step3: 'تأتي التعليقات — MyChat يرد ويُرسل الرسائل تلقائياً',
        connectCta: 'ربط Instagram',
        createCta: 'إنشاء أتمتة',
      },
      error: {
        timeout: 'استغرق الطلب وقتاً أطول من المعتاد. حاول مرة أخرى.',
        sessionExpired: 'انتهت الجلسة. يرجى تسجيل الدخول من جديد.',
        serverError: 'حدثت مشكلة في الخادم. حاول مرة أخرى بعد قليل.',
        connectInstagram: 'اربط حساب Instagram أو أعد ربطه لتحميل لوحة التحكم.',
        network: 'لا يوجد اتصال بالإنترنت. تحقّق من اتصالك.',
        generic: 'تعذّر تحميل بيانات لوحة التحكم.',
        refreshFailedCache: 'تعذّر التحديث. نعرض آخر بيانات متوفّرة.',
      },
    },
    auth: {
      login: {
        title: 'مرحباً بعودتك',
        subtitle: 'سجّل الدخول لإدارة أتمتات Instagram.',
        emailOrUsername: 'البريد الإلكتروني أو اسم المستخدم',
        password: 'كلمة المرور',
        submit: 'تسجيل الدخول',
        forgot: 'هل نسيت كلمة المرور؟',
        noAccount: 'ليس لديك حساب؟',
        signupLink: 'إنشاء حساب',
        invalidCreds: 'البريد الإلكتروني أو كلمة المرور غير صحيحة',
      },
      signup: {
        title: 'أنشئ حسابك في MyChat',
        subtitle: 'يمكنك ربط Instagram بعد الانتهاء — تستغرق العملية ٣٠ ثانية.',
        email: 'البريد الإلكتروني',
        username: 'اسم المستخدم',
        password: 'كلمة المرور',
        passwordHint: 'لا تقلّ عن ٨ أحرف.',
        submit: 'إنشاء الحساب',
        haveAccount: 'لديك حساب بالفعل؟',
        loginLink: 'تسجيل الدخول',
        usernameTaken: 'اسم المستخدم مستخدم بالفعل',
        emailRegistered: 'هذا البريد الإلكتروني مُسجّل مسبقاً',
      },
      forgot: {
        title: 'إعادة تعيين كلمة المرور',
        subtitle: 'أدخل بريدك الإلكتروني وسنُرسل لك رابط إعادة التعيين.',
        email: 'البريد الإلكتروني',
        submit: 'إرسال الرابط',
        sent: 'إذا كان هذا البريد مُسجّلاً لدينا، فالرابط في طريقه إليك.',
        back: 'العودة لتسجيل الدخول',
      },
      reset: {
        title: 'تعيين كلمة مرور جديدة',
        newPassword: 'كلمة المرور الجديدة',
        confirmPassword: 'تأكيد كلمة المرور',
        submit: 'تحديث كلمة المرور',
        mismatch: 'كلمتا المرور غير متطابقتين',
        success: 'تم تحديث كلمة المرور. يمكنك تسجيل الدخول الآن.',
      },
    },
    automations: {
      pageTitle: 'الأتمتات',
      pageSubtitle: 'أنشئ أتمتات للتعليقات الجديدة على Instagram.',
      tabs: { all: 'الكل', active: 'نشطة', paused: 'متوقفة', draft: 'مسودة' },
      searchPlaceholder: 'ابحث في الأتمتات…',
      empty: 'لا توجد أتمتات بعد. أنشئ أول واحدة.',
      status: { active: 'نشطة', paused: 'متوقفة', draft: 'مسودة' },
      fired: 'مرّات التشغيل',
      sent: 'مُرسلة',
      activeSince: 'نشطة منذ',
      createdNotice: 'الأتمتة فعّالة الآن',
      updatedNotice: 'تم تحديث الأتمتة مع الحفاظ على الإحصائيات.',
      createFailed: 'تعذّر إنشاء الأتمتة',
      builder: {
        editTitle: 'تعديل الأتمتة',
        createTitle: 'الأتمتات',
        editDesc: 'حدّث هذه الأتمتة مع الحفاظ على إحصائياتها.',
        createDesc: 'أنشئ أتمتة جديدة للتعليقات على Instagram داخل مساحة عملك.',
        saveChanges: 'حفظ التغييرات',
        goLive: 'تفعيل',
      },
    },
    settings: {
      title: 'الإعدادات',
      subtitle: 'إدارة حسابك، وربط Instagram، والتفضيلات.',
      tabs: {
        profile: 'الملف الشخصي',
        instagram: 'Instagram',
        notifications: 'الإشعارات',
        billing: 'الفوترة',
        security: 'الأمان',
      },
      profile: {
        heading: 'الملف الشخصي',
        description: 'بيانات حسابك. يمكنك تعديل الاسم واسم المستخدم.',
        fullName: 'الاسم الكامل',
        username: 'اسم المستخدم',
        email: 'البريد الإلكتروني',
        emailHint: 'تغيير البريد الإلكتروني يتطلّب خطوة تحقّق غير مفعّلة حالياً. تواصل مع الدعم لتغييره.',
        saved: 'تم تحديث الملف الشخصي',
      },
      security: {
        heading: 'الأمان',
        currentPassword: 'كلمة المرور الحالية',
        newPassword: 'كلمة المرور الجديدة',
        confirmNewPassword: 'تأكيد كلمة المرور الجديدة',
        updateButton: 'تحديث كلمة المرور',
        updated: 'تم تحديث كلمة المرور.',
        wrongCurrent: 'كلمة المرور الحالية غير صحيحة.',
      },
      notifications: {
        heading: 'تفضيلات الإشعارات',
        email: 'إشعارات البريد الإلكتروني',
        push: 'الإشعارات الفورية',
        weekly: 'الملخّص الأسبوعي',
        saved: 'تم حفظ التفضيلات.',
      },
    },
    billing: {
      title: 'الفوترة',
      subtitle: 'خطّتك الحالية، واستهلاك هذا الشهر، والحصة المتبقية.',
      currentPlan: 'الخطة الحالية',
      thisMonth: 'هذا الشهر',
      plans: 'الخطط',
      billingNotEnabled: 'الفوترة غير مُفعّلة بعد.',
      contactSupport:
        'حدود الخطة الحالية مُطبَّقة لضمان استقرار النسخة التجريبية. تواصل مع الدعم خلال هذه المرحلة لتغيير خطّتك. ستتوفّر الترقية بالدفع لاحقاً.',
      perMonth: 'شهرياً',
      currentPlanLabel: 'الخطة الحالية',
    },
    dmAutomation: {
      title: 'أتمتة رسائل Instagram',
      subtitle: 'رد تلقائياً على الرسائل الخاصة وفق قواعد كلمات مفتاحية، باستقلال عن التعليقات.',
      createTitle: 'إنشاء قاعدة رسالة',
      ruleName: 'اسم القاعدة',
      keyword: 'الكلمة المفتاحية',
      matchMode: 'نوع المطابقة',
      replyMessage: 'نص الرد',
      active: 'مُفعّلة',
      saveRule: 'حفظ القاعدة',
      rulesCount: 'قواعد الرسائل',
      noRules: 'لا توجد قواعد بعد.',
      recentEventsTitle: 'آخر الرسائل',
      noEvents: 'لا توجد رسائل بعد.',
    },
    status: {
      title: 'حالة الخدمة',
      operational: 'يعمل بشكل طبيعي',
      partialOutage: 'تعطّل جزئي',
      majorOutage: 'تعطّل كبير',
      unknown: 'الحالة غير معروفة',
      allOperational: 'جميع الأنظمة تعمل',
      allOperationalBody: 'جميع مكوّنات MyChat تستجيب بشكل طبيعي.',
      partialTitle: 'انقطاع جزئي في الخدمة',
      partialBody: 'بعض المكوّنات تعمل بأداء أقل من المعتاد. قد تكون الأتمتات أبطأ.',
      majorTitle: 'انقطاع كبير في الخدمة',
      majorBody: 'مكوّن أو أكثر غير متاح حالياً. نحقّق في المشكلة الآن.',
      unknownTitle: 'الحالة غير متاحة',
      unknownBody: 'تعذّر علينا تحديد حالة الخدمة في الوقت الحالي.',
      reach: 'إذا واجهت مشكلة غير ظاهرة هنا، راسل',
      support: 'الدعم',
      checkedAt: 'آخر فحص',
    },
    landing: {
      nav: {
        features: 'المميزات',
        how: 'كيف يعمل',
        pricing: 'الأسعار',
        status: 'حالة الخدمة',
      },
      hero: {
        badge: 'أتمتة Instagram لحسابات الأعمال',
        title1: 'حوّل تعليقات Instagram',
        title2: 'إلى',
        titleEm: 'محادثات تبيع.',
        subtitle:
          'اربط حساب Instagram للأعمال، صمّم قواعد للتعليقات والرسائل ببناء بصري بسيط، وأدر كل أتمتاتك من مكان واحد.',
        cta: 'ابدأ مجاناً',
        secondaryCta: 'حالة الخدمة',
      },
      preview: {
        triggerLabel: 'الحدث',
        triggerTitle: 'تعليق جديد',
        triggerHint: 'على كلمة مفتاحية تختارها',
        messageLabel: 'الرسالة',
        messageTitle: 'رسالة افتتاحية',
        messageHint: 'تُرسَل تلقائياً',
        actionLabel: 'الإجراء',
        actionTitle: 'رد أو رابط',
        actionHint: 'حدّد ما يحدث بعدها',
      },
      features: {
        badge: 'المميزات',
        title: 'كل ما تحتاجه لتحويل محادثاتك إلى نتائج',
        subtitle: 'كل شيء مربوط بحسابك أنت — لا صناديق وارد مشتركة، ولا بيانات مشتركة.',
        items: {
          commentTrigger: {
            title: 'محفّزات على التعليقات',
            description:
              'رد فوراً على أي تعليق يحتوي كلمة مفتاحية تختارها، أو تفاعل مع كل تعليق جديد على منشور بعينه.',
          },
          dmAutomation: {
            title: 'أتمتة ذكية للرسائل',
            description:
              'أرسل رسالة متابعة لكل من علّق: رسالة مخصّصة، رابط، أو شرط متابعة قبل استلام الرسالة.',
          },
          dashboard: {
            title: 'لوحة تحكّم لحظية',
            description:
              'تابع الردود المُرسلة والرسائل التي وصلت ونقرات الروابط لحظة بلحظة، من نشاط حسابك مباشرة.',
          },
          deliveryAware: {
            title: 'نظام يفهم سياسات Instagram',
            description:
              'يدرك متى يرفض Instagram تسليم الرسالة (انتهاء نافذة ٢٤ ساعة، رفض المستخدم، منشور لا تملكه) ويتعامل معها بهدوء بدلاً من اعتبارها فشلاً.',
          },
          multiAccount: {
            title: 'دعم حسابات متعدّدة',
            description:
              'اربط أكثر من حساب Instagram للأعمال وتنقّل بينها بنقرة واحدة. كل أتمتة تظل ضمن الحساب الذي تختاره.',
          },
          conversionTracking: {
            title: 'تتبّع التحويلات',
            description:
              'كل رابط ترسله عبر رسالة يُتتبَّع. اعرف عدد النقرات بدقّة، في أي يوم، ومن أي أتمتة.',
          },
        },
      },
      how: {
        badge: 'كيف يعمل',
        title: 'ابدأ خلال ٣ خطوات',
        steps: [
          {
            num: '٠١',
            title: 'اربط Instagram',
            desc: 'اربط حساب Business أو Creator من إعدادات التطبيق — ٣٠ ثانية فقط.',
          },
          {
            num: '٠٢',
            title: 'أنشئ أول قاعدة',
            desc: 'اختر المنشور، حدّد الكلمة المفتاحية، اكتب الرد العام ونص الرسالة، ثم فعّل القاعدة.',
          },
          {
            num: '٠٣',
            title: 'دعها تعمل',
            desc: 'كل تعليق جديد يُشغّل القاعدة فوراً. الردود والرسائل تخرج تلقائياً — كل ما عليك مراجعة التحليلات.',
          },
        ],
      },
      cta: {
        title: 'جاهز لأتمتة تعليقاتك على Instagram؟',
        body: 'أنشئ حسابك المجاني، واربط Instagram عندما تكون مستعداً للعمل على محادثات حقيقية.',
        button: 'ابدأ مجاناً',
      },
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
  // Arabic = right-to-left layout (matches professional Arabic SaaS
  // sites like araby.ai). Tailwind's `rtl:` utility prefix flips
  // margin/padding/border directions automatically when this is set.
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

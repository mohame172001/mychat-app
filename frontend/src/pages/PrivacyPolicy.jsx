import React from 'react';
import { Link } from 'react-router-dom';
import { MessageCircle, ArrowLeft } from 'lucide-react';
import { useTranslation } from '../lib/i18n';

const PrivacyPolicy = () => {
  const { lang } = useTranslation();
  const ar = lang === 'ar';

  const SECTIONS = ar ? [
    { h: '١. مقدّمة', body: <p>مايتشات ("نحن") منصّة لأتمتة Instagram تساعد الشركات على الردّ على التعليقات والرسائل الخاصة. توضّح هذه السياسة ما الذي نجمعه، وكيف نستخدمه، والخيارات المتاحة لك. باستخدامك للخدمة، فإنك توافق على هذه السياسة.</p> },
    { h: '٢. البيانات التي نجمعها', body: <>
      <p><strong>بيانات الحساب.</strong> عند التسجيل نجمع اسمك وبريدك الإلكتروني ومزوّد المصادقة، وكلمة مرور مشفّرة عند استخدام تسجيل الدخول بكلمة مرور.</p>
      <p><strong>بيانات Instagram.</strong> عند ربط حساب Instagram التجاري أو الإبداعي عبر تدفّق Meta الرسمي، نستلم ونخزّن:</p>
      <ul className="list-disc ps-6 space-y-1">
        <li>مُعرّف حساب Instagram التجاري واسم المستخدم ورابط صورة الملف الشخصي</li>
        <li>رمز وصول صادر عن Meta، يُحفظ على الخادم ولا يظهر أبداً في المتصفّح أو واجهة الإدارة</li>
        <li>معرّفات منشوراتك المنشورة وبياناتها الأساسية</li>
        <li>التعليقات العلنية على منشوراتك، بما فيها مُعرّف المُعلّق واسمه ونصّ التعليق</li>
        <li>الرسائل الخاصة المُرسلة إلى حسابك عبر واجهة Instagram، بما يشمل مُعرّف المُرسل والمحتوى اللازم لتشغيل الأتمتات المُفعّلة</li>
      </ul>
      <p><strong>إعدادات الأتمتة.</strong> الكلمات المفتاحية وقوالب الردود والمنطق الذي تُنشئه داخل مايتشات.</p>
      <p><strong>بيانات الاستخدام.</strong> عدّادات الاستخدام وأوقات الأحداث ورموز الحالة وسجلّات الخادم المحدودة لأغراض الأمان والاستقرار. نُخفي الرموز ومحتوى الرسائل من سجلّات التشغيل.</p>
    </> },
    { h: '٣. كيف نستخدم بياناتك', body: <>
      <p>نستخدم البيانات حصراً لتشغيل الخدمة، تحديداً:</p>
      <ul className="list-disc ps-6 space-y-1">
        <li>رصد التعليقات والرسائل الجديدة على حسابك بشكل شبه فوري</li>
        <li>مطابقة التعليقات والرسائل الواردة مع قواعد الأتمتة التي أعددتها</li>
        <li>إرسال الردود والرسائل الخاصة نيابةً عنك عبر واجهة Instagram Graph</li>
        <li>عرض إحصائيات الأتمتات داخل لوحة تحكّم مايتشات</li>
        <li>التحقّق من هويتك والحفاظ على أمان حسابك</li>
      </ul>
      <p>لا نستخدم بيانات Instagram لتدريب نماذج تعلّم آلي، ولا لبناء ملفّات إعلانية، ولا لأي غرض غير مرتبط بتشغيل الميزات التي فعّلتها.</p>
    </> },
    { h: '٤. مشاركة البيانات', body: <>
      <p><strong>لا نبيع</strong> بياناتك ولا نؤجّرها ولا نُتاجر بها. نُشاركها فقط مع:</p>
      <ul className="list-disc ps-6 space-y-1">
        <li><strong>شركة Meta Platforms</strong> — لإرسال الردود/الرسائل وجلب بيانات التعليقات عبر واجهة Instagram Graph</li>
        <li><strong>مزوّدي البنية التحتية</strong> (Railway للاستضافة، MongoDB Atlas لقاعدة البيانات) — حصراً لتشغيل الخدمة</li>
        <li><strong>الجهات القانونية</strong> — فقط عند وجود إلزام قانوني</li>
      </ul>
    </> },
    { h: '٥. مدّة الاحتفاظ بالبيانات', body: <p>نحتفظ ببيانات حسابك ما دام نشطاً. أمّا التعليقات والرسائل المُلتقَطة فنحتفظ بها فقط بقدر ما تستلزمه الأتمتات المُفعّلة وعرض الحالة وإعادة المحاولة الآمنة. تُحذف رموز الوصول عند فصل Instagram أو حذف الحساب. تُحذف أو تُجهَّل السجلّات التشغيلية المرتبطة بحسابك عند إتمام طلب حذف موثّق.</p> },
    { h: '٦. حقوقك وخياراتك', body: <>
      <p>يمكنك في أي وقت:</p>
      <ul className="list-disc ps-6 space-y-1">
        <li><strong>فصل Instagram</strong> من صفحة الإعدادات — يُلغي رمز الوصول ويوقف جمع البيانات</li>
        <li><strong>حذف حسابك</strong> عبر <Link to="/data-deletion" className="text-blue-600 hover:underline">صفحة حذف البيانات</Link> أو بإرسال بريد إلكتروني إلينا</li>
        <li><strong>سحب الوصول من Meta</strong> عبر إعدادات Instagram ← التطبيقات والمواقع؛ سنتوقّف فوراً عن استلام بياناتك</li>
        <li><strong>طلب نسخة</strong> من البيانات الشخصية التي نحتفظ بها عنك</li>
      </ul>
    </> },
    { h: '٧. طلبات حذف البيانات', body: <p>لطلب حذف جميع بياناتك وبيانات Instagram المخزّنة لدى مايتشات، استخدم <Link to="/data-deletion" className="text-blue-600 hover:underline">صفحة حذف البيانات</Link> أو راسلنا على <a href="mailto:mm.mohame172000@gmail.com" className="text-blue-600 hover:underline">mm.mohame172000@gmail.com</a> بعنوان "Data Deletion Request" من البريد المرتبط بحسابك. سنؤكّد الاستلام خلال 72 ساعة ونُكمل الحذف خلال 30 يوماً.</p> },
    { h: '٨. الأمان', body: <p>رموز الوصول تُخزَّن في الخادم فقط ولا تُعاد إلى الواجهة. كل الاتصالات تتمّ عبر HTTPS في الإنتاج. كلمات المرور مشفّرة بخوارزميات قياسية (bcrypt). الوصول إلى بيانات الإنتاج مقصور على الموظّفين المخوّلين. ما من وسيلة آمنة بنسبة 100٪، لكنّنا نتّبع أفضل ممارسات الصناعة.</p> },
    { h: '٩. خصوصية الأطفال', body: <p>مايتشات ليست موجّهة لمن هم دون الـ13 من العمر، ولا نجمع بياناتهم عن قصد.</p> },
    { h: '١٠. التعديلات', body: <p>قد نُحدّث هذه السياسة من وقت لآخر. سنُعلن عن التعديلات الجوهرية داخل لوحة التحكّم وعبر البريد. تاريخ "آخر تحديث" في الأعلى يعكس آخر مراجعة.</p> },
    { h: '١١. التواصل', body: <p>للأسئلة حول هذه السياسة أو بياناتك، راسلنا على <a href="mailto:mm.mohame172000@gmail.com" className="text-blue-600 hover:underline">mm.mohame172000@gmail.com</a>.</p> },
  ] : [
    { h: '1. Introduction', body: <p>mychat ("we", "our", "us") is an Instagram automation platform that helps businesses respond to comments and direct messages on Instagram. This Privacy Policy explains what information we collect, how we use it, and the choices you have. By using mychat, you agree to this policy.</p> },
    { h: '2. Information We Collect', body: <>
      <p><strong>Account information.</strong> When you sign up we collect your name, email address, auth provider, and a hashed password if you use email/password login.</p>
      <p><strong>Instagram data.</strong> When you connect your Instagram Business or Creator account via Meta's official Login flow, we receive and store:</p>
      <ul className="list-disc ps-6 space-y-1">
        <li>Your Instagram Business Account ID, username, and profile picture URL</li>
        <li>An access token issued by Meta, stored server-side and never shown in the browser or admin UI</li>
        <li>The IDs and basic metadata of your published media (posts, reels)</li>
        <li>Public comments left on your media, including the commenter's Instagram-scoped ID, username, and the comment text</li>
        <li>Direct messages sent to your account through the Instagram Messaging API, including sender ID and message content needed to run enabled automations</li>
      </ul>
      <p><strong>Automation configuration.</strong> The keywords, reply templates, and flow logic you create inside mychat.</p>
      <p><strong>Usage data.</strong> Usage counters, event timestamps, status codes, and limited server logs for security, reliability, and debugging. We redact tokens and private message bodies from operational logs.</p>
    </> },
    { h: '3. How We Use Your Information', body: <>
      <p>We use the data above strictly to operate the service, specifically to:</p>
      <ul className="list-disc ps-6 space-y-1">
        <li>Detect new comments and DMs on your Instagram account in near-real time</li>
        <li>Match incoming comments and DMs against the automation rules you configured</li>
        <li>Send replies and direct messages on your behalf using the Instagram Graph API</li>
        <li>Display analytics about your automations inside the mychat dashboard</li>
        <li>Authenticate you and keep your account secure</li>
      </ul>
      <p>We do not use Instagram data to train machine-learning models, build advertising profiles, or for any purpose unrelated to operating the automation features you enabled.</p>
    </> },
    { h: '4. Data Sharing', body: <>
      <p>We do <strong>not</strong> sell, rent, or trade your data. We share data only with:</p>
      <ul className="list-disc ps-6 space-y-1">
        <li><strong>Meta Platforms, Inc.</strong> — to send replies/DMs and fetch comment data via the Instagram Graph API</li>
        <li><strong>Infrastructure providers</strong> (Railway for hosting, MongoDB Atlas for database) — strictly to run the service</li>
        <li><strong>Law enforcement</strong> — only when required by valid legal process</li>
      </ul>
    </> },
    { h: '5. Data Retention', body: <p>We retain your account data for as long as your account is active. Comments and DMs ingested for automation are retained only as needed to operate enabled automations, show status, and support safe retries. Access tokens are removed when you disconnect Instagram or delete your account. Operational records tied to your account are deleted or anonymized when a verified account deletion request is completed.</p> },
    { h: '6. Your Rights and Choices', body: <>
      <p>You can at any time:</p>
      <ul className="list-disc ps-6 space-y-1">
        <li><strong>Disconnect Instagram</strong> from the Settings page — this revokes the access token and stops all data ingestion</li>
        <li><strong>Delete your account</strong> by using the <Link to="/data-deletion" className="text-blue-600 hover:underline">Data Deletion page</Link> or emailing us at the address below</li>
        <li><strong>Revoke access from Meta</strong> at any time via Instagram Settings → Apps and Websites; we will stop receiving data immediately</li>
        <li><strong>Request a copy</strong> of the personal data we hold about you</li>
      </ul>
    </> },
    { h: '7. Data Deletion Requests', body: <p>To request deletion of all your personal data and Instagram data stored by mychat, use the <Link to="/data-deletion" className="text-blue-600 hover:underline">Data Deletion page</Link> or send an email to <a href="mailto:mm.mohame172000@gmail.com" className="text-blue-600 hover:underline">mm.mohame172000@gmail.com</a> with the subject "Data Deletion Request" from the email address associated with your account. We will confirm receipt within 72 hours and complete deletion within 30 days.</p> },
    { h: '8. Security', body: <p>Access tokens are stored only on the backend and are not returned to the frontend. All traffic is served over HTTPS in production. Passwords are hashed using industry-standard algorithms (bcrypt). Access to production data is restricted to authorized personnel. No method of transmission or storage is 100% secure, but we follow industry best practices.</p> },
    { h: "9. Children's Privacy", body: <p>mychat is not directed to children under 13, and we do not knowingly collect data from them.</p> },
    { h: '10. Changes to This Policy', body: <p>We may update this Privacy Policy from time to time. Material changes will be announced inside the dashboard and via email. The "Last updated" date at the top reflects the most recent revision.</p> },
    { h: '11. Contact', body: <p>Questions about this policy or your data can be sent to <a href="mailto:mm.mohame172000@gmail.com" className="text-blue-600 hover:underline">mm.mohame172000@gmail.com</a>.</p> },
  ];

  return (
    <div className="min-h-screen bg-white text-slate-900">
      <nav className="fixed top-0 inset-x-0 z-50 backdrop-blur-xl bg-white/80 border-b border-slate-100">
        <div className="max-w-4xl mx-auto px-6 h-16 flex items-center justify-between">
          <Link to="/" className="flex items-center gap-2">
            <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 via-cyan-400 to-pink-400 flex items-center justify-center">
              <MessageCircle className="w-5 h-5 text-white" strokeWidth={2.5} />
            </div>
            <span className="text-xl font-bold tracking-tight">mychat</span>
          </Link>
          <Link to="/" className="text-sm font-medium text-slate-600 hover:text-slate-900 flex items-center gap-1">
            <ArrowLeft className="w-4 h-4" /> {ar ? 'رجوع' : 'Back'}
          </Link>
        </div>
      </nav>

      <main className="max-w-3xl mx-auto px-6 pt-28 pb-20">
        <h1 className="text-4xl font-bold tracking-tight mb-2">{ar ? 'سياسة الخصوصية' : 'Privacy Policy'}</h1>
        <p className="text-sm text-slate-500 mb-10">{ar ? 'آخر تحديث: ٩ مايو ٢٠٢٦' : 'Last updated: May 9, 2026'}</p>

        <div className="prose prose-slate max-w-none space-y-6 text-slate-700 leading-relaxed">
          {SECTIONS.map((s, i) => (
            <section key={i}>
              <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">{s.h}</h2>
              {s.body}
            </section>
          ))}
        </div>
      </main>

      <footer className="border-t border-slate-100 py-8">
        <div className="max-w-4xl mx-auto px-6 flex items-center justify-between text-sm text-slate-500">
          <span>© 2026 mychat</span>
          <div className="flex gap-6">
            <Link to="/privacy" className="hover:text-slate-900">{ar ? 'الخصوصية' : 'Privacy'}</Link>
            <Link to="/terms" className="hover:text-slate-900">{ar ? 'الشروط' : 'Terms'}</Link>
            <Link to="/data-deletion" className="hover:text-slate-900">{ar ? 'حذف البيانات' : 'Data Deletion'}</Link>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default PrivacyPolicy;

import React from 'react';
import { Link } from 'react-router-dom';
import { MessageCircle, ArrowLeft } from 'lucide-react';
import { useTranslation } from '../lib/i18n';

const Terms = () => {
  const { lang } = useTranslation();
  const ar = lang === 'ar';

  const SECTIONS = ar ? [
    { h: '١. الموافقة', p: 'بإنشاء حساب أو استخدامك مايتشات ("الخدمة")، فإنك توافق على هذه الشروط. إن لم توافق، فلا تستخدم الخدمة.' },
    { h: '٢. وصف الخدمة', p: 'تتيح لك مايتشات ربط حساب Instagram تجاري أو إبداعي وإعداد قواعد للردّ تلقائياً على التعليقات والرسائل الخاصة. تعتمد الخدمة على واجهة Meta لـInstagram Graph وتخضع لسياسات Meta.' },
    { h: '٣. الأهلية', p: 'يجب ألّا يقلّ عمرك عن 18 عاماً، وأن تكون مخوّلاً بإدارة حساب Instagram الذي تربطه. أنت تلتزم بسياسات Meta وقواعد مجتمع Instagram وكافة القوانين السارية.' },
    { h: '٤. الاستخدام المقبول', p: 'يُحظر استخدام مايتشات في:', list: [
      'إرسال الرسائل المزعجة، أو التحرّش، أو خطاب الكراهية، أو أي محتوى يخالف سياسات Instagram',
      'انتحال شخصية شخص أو جهة',
      'إرسال رسائل تجارية غير مرغوب فيها لمستخدمين لم يوافقوا على ذلك',
      'الهندسة العكسية، أو الكشط، أو إساءة استخدام الخدمة أو واجهة Instagram',
      'محاولة تجاوز حدود الاستخدام أو ضوابط الوصول',
    ], post: 'قد تؤدّي المخالفات إلى تعليق فوري وإبلاغ Meta.' },
    { h: '٥. المحتوى الخاص بك', p: 'تحتفظ بملكية قواعد الأتمتة وقوالب الردود وأي محتوى آخر تنشئه. وتمنح مايتشات ترخيصاً محدوداً لمعالجة هذا المحتوى لتشغيل الخدمة لك فقط.' },
    { h: '٦. إنهاء الحساب', p: <>يمكنك حذف حسابك في أيّ وقت عبر <Link to="/data-deletion" className="text-blue-600 hover:underline">صفحة حذف البيانات</Link> أو بالتواصل مع الدعم. قد نقوم بتعليق أو إنهاء الحسابات المخالفة للشروط أو المُسيئة لواجهة Instagram أو التي تشكّل خطراً على المستخدمين الآخرين أو منصّة Meta.</> },
    { h: '٧. إخلاء المسؤولية', p: 'تُقدَّم الخدمة "كما هي" دون أي ضمانات. لا نضمن استمرارية التشغيل ولا معالجة كل تعليق أو رسالة — فقد تفرض واجهة Meta حدوداً أو قيوداً خارجة عن إرادتنا.' },
    { h: '٨. تحديد المسؤولية', p: 'إلى أقصى حدّ يسمح به القانون، لا تتحمّل مايتشات أي مسؤولية عن الأضرار غير المباشرة أو العرضية أو التبعية الناشئة عن استخدامك للخدمة.' },
    { h: '٩. التعديلات', p: 'قد نُحدّث هذه الشروط. استمرارك في استخدام الخدمة بعد نشر التعديلات يُعدّ موافقة عليها.' },
    { h: '١٠. التواصل', p: <>لأي استفسار حول هذه الشروط، تواصل عبر <a href="mailto:mm.mohame172000@gmail.com" className="text-blue-600 hover:underline">mm.mohame172000@gmail.com</a>.</> },
  ] : [
    { h: '1. Agreement', p: 'By creating an account or using mychat ("the Service"), you agree to these Terms. If you do not agree, do not use the Service.' },
    { h: '2. The Service', p: "mychat lets you connect an Instagram Business or Creator account and configure rules that automatically reply to comments and direct messages. The Service relies on Meta's Instagram Graph API and is subject to Meta's platform policies." },
    { h: '3. Eligibility', p: "You must be at least 18 years old and authorized to operate the Instagram account you connect. You agree to comply with Meta's Platform Terms, the Instagram Community Guidelines, and all applicable laws." },
    { h: '4. Acceptable Use', p: 'You agree NOT to use mychat to:', list: [
      "Send spam, harassment, hate speech, or content that violates Instagram's policies",
      'Impersonate any person or entity',
      'Send unsolicited commercial messages to users who did not opt in',
      'Reverse engineer, scrape, or abuse the Service or the Instagram API',
      'Attempt to circumvent rate limits or access controls',
    ], post: 'Violations may result in immediate suspension and reporting to Meta.' },
    { h: '5. Your Content', p: 'You retain ownership of the automation rules, reply templates, and any other content you create. You grant mychat a limited license to process this content solely to operate the Service for you.' },
    { h: '6. Account Termination', p: <>You may delete your account at any time through the <Link to="/data-deletion" className="text-blue-600 hover:underline">Data Deletion page</Link> or by contacting support. We may suspend or terminate accounts that violate these Terms, abuse the API, or pose risk to other users or to Meta's platform.</> },
    { h: '7. Disclaimer', p: 'The Service is provided "as is" without warranties of any kind. We do not guarantee uninterrupted operation or that every comment or DM will be processed — Meta\'s API may impose rate limits or restrictions outside our control.' },
    { h: '8. Limitation of Liability', p: 'To the maximum extent permitted by law, mychat shall not be liable for any indirect, incidental, or consequential damages arising from your use of the Service.' },
    { h: '9. Changes', p: 'We may update these Terms. Continued use of the Service after changes are posted constitutes acceptance.' },
    { h: '10. Contact', p: <>For questions about these Terms, contact <a href="mailto:mm.mohame172000@gmail.com" className="text-blue-600 hover:underline">mm.mohame172000@gmail.com</a>.</> },
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
        <h1 className="text-4xl font-bold tracking-tight mb-2">{ar ? 'شروط الاستخدام' : 'Terms of Service'}</h1>
        <p className="text-sm text-slate-500 mb-10">{ar ? 'آخر تحديث: ٩ مايو ٢٠٢٦' : 'Last updated: May 9, 2026'}</p>

        <div className="space-y-6 text-slate-700 leading-relaxed">
          {SECTIONS.map((s, i) => (
            <section key={i}>
              <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">{s.h}</h2>
              <p>{s.p}</p>
              {s.list && (
                <ul className="list-disc ps-6 space-y-1">
                  {s.list.map((item, j) => <li key={j}>{item}</li>)}
                </ul>
              )}
              {s.post && <p>{s.post}</p>}
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

export default Terms;

import React from 'react';
import { Link } from 'react-router-dom';
import { MessageCircle, ArrowLeft } from 'lucide-react';

const Terms = () => {
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
            <ArrowLeft className="w-4 h-4" /> Back
          </Link>
        </div>
      </nav>

      <main className="max-w-3xl mx-auto px-6 pt-28 pb-20">
        <h1 className="text-4xl font-bold tracking-tight mb-2">Terms of Service</h1>
        <p className="text-sm text-slate-500 mb-10">Last updated: April 25, 2026</p>

        <div className="space-y-6 text-slate-700 leading-relaxed">
          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">1. Agreement</h2>
            <p>By creating an account or using mychat ("the Service"), you agree to these Terms. If you do not agree, do not use the Service.</p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">2. The Service</h2>
            <p>mychat lets you connect an Instagram Business or Creator account and configure rules that automatically reply to comments and direct messages. The Service relies on Meta's Instagram Graph API and is subject to Meta's platform policies.</p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">3. Eligibility</h2>
            <p>You must be at least 18 years old and authorized to operate the Instagram account you connect. You agree to comply with Meta's Platform Terms, the Instagram Community Guidelines, and all applicable laws.</p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">4. Acceptable Use</h2>
            <p>You agree NOT to use mychat to:</p>
            <ul className="list-disc pl-6 space-y-1">
              <li>Send spam, harassment, hate speech, or content that violates Instagram's policies</li>
              <li>Impersonate any person or entity</li>
              <li>Send unsolicited commercial messages to users who did not opt in</li>
              <li>Reverse engineer, scrape, or abuse the Service or the Instagram API</li>
              <li>Attempt to circumvent rate limits or access controls</li>
            </ul>
            <p>Violations may result in immediate suspension and reporting to Meta.</p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">5. Your Content</h2>
            <p>You retain ownership of the automation rules, reply templates, and any other content you create. You grant mychat a limited license to process this content solely to operate the Service for you.</p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">6. Account Termination</h2>
            <p>You may delete your account at any time. We may suspend or terminate accounts that violate these Terms, abuse the API, or pose risk to other users or to Meta's platform.</p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">7. Disclaimer</h2>
            <p>The Service is provided "as is" without warranties of any kind. We do not guarantee uninterrupted operation or that every comment or DM will be processed — Meta's API may impose rate limits or restrictions outside our control.</p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">8. Limitation of Liability</h2>
            <p>To the maximum extent permitted by law, mychat shall not be liable for any indirect, incidental, or consequential damages arising from your use of the Service.</p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">9. Changes</h2>
            <p>We may update these Terms. Continued use of the Service after changes are posted constitutes acceptance.</p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">10. Contact</h2>
            <p>For questions about these Terms, contact <a href="mailto:mm.mohame172000@gmail.com" className="text-blue-600 hover:underline">mm.mohame172000@gmail.com</a>.</p>
          </section>
        </div>
      </main>

      <footer className="border-t border-slate-100 py-8">
        <div className="max-w-4xl mx-auto px-6 flex items-center justify-between text-sm text-slate-500">
          <span>© 2026 mychat</span>
          <div className="flex gap-6">
            <Link to="/privacy" className="hover:text-slate-900">Privacy</Link>
            <Link to="/terms" className="hover:text-slate-900">Terms</Link>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Terms;

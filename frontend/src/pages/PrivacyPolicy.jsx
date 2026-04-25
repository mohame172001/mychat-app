import React from 'react';
import { Link } from 'react-router-dom';
import { MessageCircle, ArrowLeft } from 'lucide-react';

const PrivacyPolicy = () => {
  return (
    <div className="min-h-screen bg-white text-slate-900">
      {/* Navbar */}
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
        <h1 className="text-4xl font-bold tracking-tight mb-2">Privacy Policy</h1>
        <p className="text-sm text-slate-500 mb-10">Last updated: April 25, 2026</p>

        <div className="prose prose-slate max-w-none space-y-6 text-slate-700 leading-relaxed">
          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">1. Introduction</h2>
            <p>
              mychat ("we", "our", "us") is an Instagram automation platform that helps businesses respond to
              comments and direct messages on Instagram. This Privacy Policy explains what information we collect,
              how we use it, and the choices you have. By using mychat, you agree to this policy.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">2. Information We Collect</h2>
            <p><strong>Account information.</strong> When you sign up we collect your name, email address, and a hashed password.</p>
            <p><strong>Instagram data.</strong> When you connect your Instagram Business or Creator account via Meta's official Login flow, we receive and store:</p>
            <ul className="list-disc pl-6 space-y-1">
              <li>Your Instagram Business Account ID, username, and profile picture URL</li>
              <li>An access token issued by Meta (encrypted at rest)</li>
              <li>The IDs and basic metadata of your published media (posts, reels)</li>
              <li>Public comments left on your media, including the commenter's Instagram-scoped ID, username, and the comment text</li>
              <li>Direct messages sent to your account through the Instagram Messaging API, including sender ID and message content</li>
            </ul>
            <p><strong>Automation configuration.</strong> The keywords, reply templates, and flow logic you create inside mychat.</p>
            <p><strong>Usage data.</strong> Standard server logs (IP address, user-agent, timestamps) for security and debugging.</p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">3. How We Use Your Information</h2>
            <p>We use the data above strictly to operate the service, specifically to:</p>
            <ul className="list-disc pl-6 space-y-1">
              <li>Detect new comments and DMs on your Instagram account in near-real time</li>
              <li>Match incoming comments and DMs against the automation rules you configured</li>
              <li>Send replies and direct messages on your behalf using the Instagram Graph API</li>
              <li>Display analytics about your automations inside the mychat dashboard</li>
              <li>Authenticate you and keep your account secure</li>
            </ul>
            <p>We do not use Instagram data to train machine-learning models, build advertising profiles, or for any purpose unrelated to operating the automation features you enabled.</p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">4. Data Sharing</h2>
            <p>We do <strong>not</strong> sell, rent, or trade your data. We share data only with:</p>
            <ul className="list-disc pl-6 space-y-1">
              <li><strong>Meta Platforms, Inc.</strong> — to send replies/DMs and fetch comment data via the Instagram Graph API</li>
              <li><strong>Infrastructure providers</strong> (Railway for hosting, MongoDB Atlas for database) — strictly to run the service</li>
              <li><strong>Law enforcement</strong> — only when required by valid legal process</li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">5. Data Retention</h2>
            <p>
              We retain your account data for as long as your account is active. Comments and DMs ingested for
              automation are retained for up to 90 days for analytics and re-processing, then automatically deleted.
              Access tokens are deleted immediately when you disconnect Instagram or delete your account.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">6. Your Rights and Choices</h2>
            <p>You can at any time:</p>
            <ul className="list-disc pl-6 space-y-1">
              <li><strong>Disconnect Instagram</strong> from the Settings page — this revokes the access token and stops all data ingestion</li>
              <li><strong>Delete your account</strong> by emailing us at the address below — this permanently erases all stored data within 30 days</li>
              <li><strong>Revoke access from Meta</strong> at any time via Instagram Settings → Apps and Websites; we will stop receiving data immediately</li>
              <li><strong>Request a copy</strong> of the personal data we hold about you</li>
            </ul>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">7. Data Deletion Requests</h2>
            <p>
              To request deletion of all your personal data and Instagram data stored by mychat, send an email to
              <a href="mailto:mm.mohame172000@gmail.com" className="text-blue-600 hover:underline"> mm.mohame172000@gmail.com</a>
              {' '}with the subject "Data Deletion Request" from the email address associated with your account. We
              will confirm receipt within 72 hours and complete deletion within 30 days.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">8. Security</h2>
            <p>
              Access tokens are encrypted at rest. All traffic is served over HTTPS. Passwords are hashed using
              industry-standard algorithms (bcrypt). Access to production data is restricted to authorized personnel.
              No method of transmission or storage is 100% secure, but we follow industry best practices.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">9. Children's Privacy</h2>
            <p>mychat is not directed to children under 13, and we do not knowingly collect data from them.</p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">10. Changes to This Policy</h2>
            <p>
              We may update this Privacy Policy from time to time. Material changes will be announced inside the
              dashboard and via email. The "Last updated" date at the top reflects the most recent revision.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mt-8 mb-3">11. Contact</h2>
            <p>
              Questions about this policy or your data can be sent to{' '}
              <a href="mailto:mm.mohame172000@gmail.com" className="text-blue-600 hover:underline">
                mm.mohame172000@gmail.com
              </a>.
            </p>
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

export default PrivacyPolicy;

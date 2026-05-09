import React, { useMemo, useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { ArrowLeft, CheckCircle2, MessageCircle, Send } from 'lucide-react';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import api from '../lib/api';

const SUPPORT_EMAIL = 'mm.mohame172000@gmail.com';

function useConfirmationCode() {
  const location = useLocation();
  return useMemo(() => {
    const params = new URLSearchParams(location.search || '');
    return params.get('confirmation_code') || '';
  }, [location.search]);
}

const DataDeletion = () => {
  const confirmationCode = useConfirmationCode();
  const [email, setEmail] = useState('');
  const [details, setDetails] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  const submit = async (event) => {
    event.preventDefault();
    setSubmitting(true);
    setError(null);
    setResult(null);
    try {
      const { data } = await api.post('/meta/data-deletion', {
        email,
        source: 'public_page',
        note_length: details.length,
      });
      setResult(data);
    } catch (_) {
      setError('We could not submit the request online. Please email support using the instructions below.');
    } finally {
      setSubmitting(false);
    }
  };

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
        <h1 className="text-4xl font-bold tracking-tight mb-2">Data Deletion</h1>
        <p className="text-sm text-slate-500 mb-10">Request deletion of your MyChat account data and connected Instagram data.</p>

        {confirmationCode && (
          <div className="mb-8 rounded-2xl border border-emerald-200 bg-emerald-50 p-4 text-sm text-emerald-800">
            <CheckCircle2 className="inline w-4 h-4 mr-1" />
            Data deletion callback received. Confirmation code: <span className="font-mono">{confirmationCode}</span>
          </div>
        )}

        <div className="space-y-8 text-slate-700 leading-relaxed">
          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mb-3">What will be deleted</h2>
            <p>
              When your request is verified, we delete or anonymize your MyChat account profile, connected Instagram
              account records, automation rules, comment/DM processing records, usage records tied to your account,
              and stored access tokens. We stop new Instagram data ingestion when the account is deleted or Instagram
              is disconnected.
            </p>
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mb-3">How to request deletion</h2>
            <p>
              Send an email to <a href={`mailto:${SUPPORT_EMAIL}`} className="text-blue-600 hover:underline">{SUPPORT_EMAIL}</a>
              {' '}from the email address associated with your MyChat account with the subject "Data Deletion Request".
              We confirm receipt within 72 hours and complete verified deletion requests within 30 days.
            </p>
          </section>

          <section className="rounded-2xl border border-slate-200 p-5">
            <h2 className="text-xl font-semibold text-slate-900 mb-3">Submit a deletion request</h2>
            <form onSubmit={submit} className="space-y-4">
              <div>
                <label htmlFor="deletion-email" className="block text-sm font-medium text-slate-700 mb-1">
                  Account email
                </label>
                <Input
                  id="deletion-email"
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  placeholder="you@example.com"
                  required
                />
              </div>
              <div>
                <label htmlFor="deletion-details" className="block text-sm font-medium text-slate-700 mb-1">
                  Optional details
                </label>
                <textarea
                  id="deletion-details"
                  value={details}
                  onChange={(e) => setDetails(e.target.value)}
                  maxLength={500}
                  className="w-full min-h-24 rounded-xl border border-slate-200 px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-slate-900/10"
                  placeholder="Add context if needed. Do not paste passwords or tokens."
                />
              </div>
              <Button type="submit" disabled={submitting} className="rounded-xl">
                <Send className="w-4 h-4 mr-2" />
                {submitting ? 'Submitting...' : 'Submit request'}
              </Button>
            </form>
            {result?.confirmation_code && (
              <p className="mt-4 rounded-xl bg-emerald-50 p-3 text-sm text-emerald-700">
                Request received. Confirmation code: <span className="font-mono">{result.confirmation_code}</span>
              </p>
            )}
            {error && (
              <p className="mt-4 rounded-xl bg-rose-50 p-3 text-sm text-rose-700">{error}</p>
            )}
          </section>

          <section>
            <h2 className="text-2xl font-semibold text-slate-900 mb-3">Meta data deletion callback</h2>
            <p>
              Meta App Review can use the backend callback endpoint at <span className="font-mono">/api/meta/data-deletion</span>.
              The endpoint returns a confirmation code and this public status page URL. It stores only hashed request
              metadata and does not expose stored user data.
            </p>
          </section>
        </div>
      </main>

      <footer className="border-t border-slate-100 py-8">
        <div className="max-w-4xl mx-auto px-6 flex items-center justify-between text-sm text-slate-500">
          <span>Copyright 2026 mychat</span>
          <div className="flex gap-6">
            <Link to="/privacy" className="hover:text-slate-900">Privacy</Link>
            <Link to="/terms" className="hover:text-slate-900">Terms</Link>
            <Link to="/data-deletion" className="hover:text-slate-900">Data Deletion</Link>
          </div>
        </div>
      </footer>
    </div>
  );
};

export default DataDeletion;

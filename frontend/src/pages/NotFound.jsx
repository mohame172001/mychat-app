import React from 'react';
import { Link } from 'react-router-dom';
import { MessageCircle } from 'lucide-react';
import { useTranslation } from '../lib/i18n';

const NotFound = () => {
  const { lang } = useTranslation();
  const ar = lang === 'ar';
  return (
    <div className="min-h-screen flex flex-col items-center justify-center bg-slate-50 text-center px-4">
      <MessageCircle className="w-16 h-16 text-slate-300 mb-6" />
      <h1 className="text-6xl font-bold text-slate-800 mb-2">404</h1>
      <p className="text-lg text-slate-500 mb-6">{ar ? 'الصفحة غير موجودة' : 'Page not found'}</p>
      <Link
        to="/"
        className="inline-flex items-center px-6 py-2.5 rounded-xl bg-slate-900 text-white text-sm font-medium hover:bg-slate-800 transition-colors"
      >
        {ar ? 'العودة إلى الرئيسية' : 'Back to home'}
      </Link>
    </div>
  );
};

export default NotFound;

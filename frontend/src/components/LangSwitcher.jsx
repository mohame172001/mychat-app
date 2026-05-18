/**
 * Phase 2.18Z — language switcher chip used everywhere (landing nav,
 * authenticated topbar, mobile menus).
 *
 * Renders a small flag + label for the currently-active language; on
 * click it pops a tiny menu showing both options. Flags are inline
 * SVG so they render identically across every OS (Chrome on Windows
 * doesn't ship emoji flag glyphs by default).
 */
import React, { useEffect, useRef, useState } from 'react';
import { useTranslation, SUPPORTED_LANGS } from '../lib/i18n';
import { ChevronDown } from 'lucide-react';


function FlagEgypt({ className = 'w-5 h-3.5' }) {
  // Egyptian flag — red/white/black tricolour with a stylised eagle
  // simplified to a gold disc so the SVG stays tiny (under 1KB).
  return (
    <svg viewBox="0 0 30 20" className={className} aria-hidden="true">
      <rect width="30" height="6.67" y="0" fill="#CE1126" />
      <rect width="30" height="6.66" y="6.67" fill="#FFFFFF" />
      <rect width="30" height="6.67" y="13.33" fill="#000000" />
      <circle cx="15" cy="10" r="2.1" fill="#C09300" />
    </svg>
  );
}


function FlagUK({ className = 'w-5 h-3.5' }) {
  // Union Jack — blue field with white + red diagonal and orthogonal
  // crosses. Pixel-perfect Union Jack would be ~3KB; this simplified
  // version is recognisable and stays under 1KB.
  return (
    <svg viewBox="0 0 60 40" className={className} aria-hidden="true">
      <rect width="60" height="40" fill="#012169" />
      {/* white diagonals */}
      <path d="M0,0 L60,40 M60,0 L0,40" stroke="#FFFFFF" strokeWidth="6" />
      {/* red diagonals */}
      <path d="M0,0 L60,40 M60,0 L0,40" stroke="#C8102E" strokeWidth="3" />
      {/* white cross */}
      <path d="M30,0 V40 M0,20 H60" stroke="#FFFFFF" strokeWidth="10" />
      {/* red cross */}
      <path d="M30,0 V40 M0,20 H60" stroke="#C8102E" strokeWidth="6" />
    </svg>
  );
}


const LANG_META = {
  ar: { label: 'العربية', Flag: FlagEgypt },
  en: { label: 'English', Flag: FlagUK },
};


export default function LangSwitcher({ variant = 'chip', className = '' }) {
  const { lang, setLang } = useTranslation();
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  const CurrentFlag = LANG_META[lang].Flag;

  // Close on outside click.
  useEffect(() => {
    function onClick(e) {
      if (!open) return;
      if (ref.current && !ref.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener('mousedown', onClick);
    return () => document.removeEventListener('mousedown', onClick);
  }, [open]);

  const onPick = (next) => {
    setLang(next);
    setOpen(false);
  };

  const base =
    variant === 'chip'
      ? 'inline-flex items-center gap-2 rounded-full border border-slate-200 px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50 transition'
      : 'inline-flex items-center gap-2 text-sm text-slate-700 hover:text-slate-900';

  return (
    <div className={`relative ${className}`} ref={ref}>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className={base}
        aria-haspopup="menu"
        aria-expanded={open}
        aria-label="Change language"
      >
        <CurrentFlag />
        <span>{LANG_META[lang].label}</span>
        <ChevronDown className="w-3.5 h-3.5 text-slate-400" />
      </button>

      {open && (
        <div
          role="menu"
          className="absolute right-0 mt-2 w-44 rounded-xl border border-slate-100 bg-white shadow-xl z-50 overflow-hidden"
        >
          {SUPPORTED_LANGS.map((code) => {
            const meta = LANG_META[code];
            const Flag = meta.Flag;
            const active = code === lang;
            return (
              <button
                key={code}
                type="button"
                role="menuitemradio"
                aria-checked={active}
                onClick={() => onPick(code)}
                className={`flex items-center gap-3 w-full px-3 py-2 text-sm text-left transition ${
                  active ? 'bg-slate-50 text-slate-900' : 'text-slate-700 hover:bg-slate-50'
                }`}
              >
                <Flag />
                <span className="flex-1">{meta.label}</span>
                {active && (
                  <span className="inline-block w-1.5 h-1.5 rounded-full bg-emerald-500" />
                )}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

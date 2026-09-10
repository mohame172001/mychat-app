import React from 'react';
import { renderToString } from 'react-dom/server';
import Landing from './Landing';
import { I18nProvider } from '../lib/i18n';

jest.mock('@/lib/utils', () => ({ cn: (...values) => values.filter(Boolean).join(' ') }), { virtual: true });

jest.mock('react-router-dom', () => ({
  Link: ({ to, children, ...props }) => <a href={to} {...props}>{children}</a>,
}));

test('landing renders without an undefined language variable', () => {
  localStorage.setItem('mychat_lang', 'en');
  expect(renderToString(<I18nProvider><Landing /></I18nProvider>)).toContain('Site navigation');
});

import React from 'react';
import { renderToString } from 'react-dom/server';
import Landing from './Landing';
import { I18nProvider } from '../lib/i18n';
import { landingCopy } from '../components/landing/landingCopy';

jest.mock('@/lib/utils', () => ({ cn: (...values) => values.filter(Boolean).join(' ') }), { virtual: true });

jest.mock('react-router-dom', () => ({
  Link: ({ to, children, ...props }) => <a href={to} {...props}>{children}</a>,
}));

test('landing renders without an undefined language variable', () => {
  localStorage.setItem('mychat_lang', 'en');
  expect(renderToString(<I18nProvider><Landing /></I18nProvider>)).toContain('Site navigation');
});

test.each(['en', 'ar'])('landing %s keeps real links and labels the example honestly', lang => {
  localStorage.setItem('mychat_lang', lang);
  const markup = renderToString(<I18nProvider><Landing /></I18nProvider>);
  const page = document.createElement('div');
  page.innerHTML = markup;
  expect(page.querySelector('h1').textContent).toContain(landingCopy[lang].title[0]);
  expect(page.querySelector('.landing-page').dir).toBe(lang === 'ar' ? 'rtl' : 'ltr');
  expect(page.textContent).toContain(landingCopy[lang].demoNote);
  expect(page.querySelectorAll('details')).toHaveLength(4);
  for (const url of ['/signup', '/login', '/privacy', '/terms', '/data-deletion', '/support', '/status']) {
    expect(page.querySelector(`a[href="${url}"]`)).not.toBeNull();
  }
  expect(page.textContent).not.toMatch(/common\.|landing\.nav\./);
});

import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { MemoryRouter, useLocation } from 'react-router-dom';
import VerifyEmail from './VerifyEmail';
import api from '../lib/api';

jest.mock('../lib/api', () => ({ post: jest.fn() }));
// CRA's older Jest resolver cannot resolve React Router's /dom export.
jest.mock('react-router-dom', () => {
  global.TextEncoder = require('util').TextEncoder;
  return jest.requireActual('react-router');
});
jest.mock('../lib/i18n', () => ({ useTranslation: () => ({ lang: 'en' }) }));
jest.mock('../components/LangSwitcher', () => () => null);

let container, root;
function Location() {
  const location = useLocation();
  return <output data-testid="location">{location.pathname + location.search}</output>;
}
async function render(path) {
  await act(async () => root.render(<MemoryRouter initialEntries={[path]}><VerifyEmail /><Location /></MemoryRouter>));
}
beforeEach(() => {
  global.IS_REACT_ACT_ENVIRONMENT = true;
  api.post.mockReset();
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
});
async function submit() {
  await act(async () => container.querySelector('form').dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })));
}

test('scrubs the token and does not consume it until the user verifies', async () => {
  await render('/verify-email?token=unit-test-token');
  expect(container.querySelector('output').textContent).toBe('/verify-email');
  expect(api.post).not.toHaveBeenCalled();
  expect(container.textContent).not.toContain('unit-test-token');
  api.post.mockResolvedValue({ data: { ok: true } });
  await submit();
  expect(api.post).toHaveBeenCalledWith('/auth/verify-email', { token: 'unit-test-token' });
  expect(container.textContent).toContain('Your email is verified');
  expect(container.querySelector('form')).toBeNull();
  expect(container.querySelector('a[href^="/login"]').getAttribute('href')).toBe('/login?next=%2Fapp');
});

test('invalid links offer a new email, not a technical error or fake success', async () => {
  await render('/verify-email?token=expired-test-token');
  api.post.mockRejectedValue({ response: { data: { detail: 'email_verification_token_expired' } } });
  await submit();
  expect(container.querySelector('[role="alert"]').textContent).toContain('invalid or expired');
  expect(container.querySelector('input[type="email"]')).not.toBeNull();
  expect(container.textContent).not.toContain('Your email is verified');
});

test('missing links allow requesting verification without exposing account existence', async () => {
  await render('/verify-email');
  expect(api.post).not.toHaveBeenCalled();
  api.post.mockResolvedValue({ data: { status: 'sent_if_account_exists' } });
  await submit();
  expect(api.post.mock.calls[0][0]).toBe('/auth/resend-verification');
  expect(container.querySelector('[role="status"]').textContent).toContain('If this account needs verification');
});

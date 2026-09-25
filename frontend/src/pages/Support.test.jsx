import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { MemoryRouter } from 'react-router-dom';
import Support from './Support';
import api from '../lib/api';

jest.mock('../lib/api', () => ({ post: jest.fn() }));
jest.mock('react-router-dom', () => {
  global.TextEncoder = require('util').TextEncoder;
  return jest.requireActual('react-router');
});
jest.mock('../lib/i18n', () => ({ useTranslation: () => ({ lang: 'en' }) }));
jest.mock('../components/LangSwitcher', () => () => null);

let container, root;
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

async function fill() {
  await act(async () => root.render(<MemoryRouter><Support /></MemoryRouter>));
  for (const [name, value, prototype] of [
    ['email', 'customer@example.com', window.HTMLInputElement.prototype],
    ['message', 'My automation is not responding to a comment.', window.HTMLTextAreaElement.prototype],
  ]) {
    const input = container.querySelector(`[name="${name}"]`);
    await act(async () => {
      Object.getOwnPropertyDescriptor(prototype, 'value').set.call(input, value);
      input.dispatchEvent(new Event('input', { bubbles: true }));
    });
  }
  await act(async () => container.querySelector('form').dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })));
}

test('submits to the backend and shows accepted only after confirmation', async () => {
  api.post.mockResolvedValue({ data: { status: 'accepted' } });
  await fill();
  expect(container.querySelector('[role="status"]').textContent).toContain('accepted for delivery');
  expect(api.post).toHaveBeenCalledWith('/support', expect.objectContaining({ email: 'customer@example.com', category: 'account' }));
  expect(container.textContent).toContain('support@mychaat.net');
});

test('shows a safe error instead of fake success', async () => {
  api.post.mockRejectedValue({ response: { status: 503 } });
  await fill();
  expect(container.querySelector('[role="alert"]').textContent).toContain('could not send');
  expect(container.querySelector('[role="status"]')).toBeNull();
});

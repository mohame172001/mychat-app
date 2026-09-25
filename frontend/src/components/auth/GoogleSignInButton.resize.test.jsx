import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import GoogleSignInButton from './GoogleSignInButton';
import { renderGoogleButton } from '../../lib/googleAuth';

jest.mock('react-router-dom', () => ({ useNavigate: () => mockNavigate }));
jest.mock('../../context/AuthContext', () => ({ useAuth: () => ({ loginWithGoogle: mockLogin }) }));
jest.mock('../../lib/i18n', () => ({ useTranslation: () => ({ lang: 'en' }) }));
jest.mock('sonner', () => ({ toast: { success: jest.fn(), error: jest.fn() } }));
jest.mock('../../lib/googleAuth', () => ({
  googleClientId: () => 'unit-test-client',
  googleStatus: () => ({ enabled: true }),
  loadGoogleRuntimeConfig: async () => ({ enabled: true }),
  renderGoogleButton: jest.fn(async () => true),
  googleErrorMessage: () => 'Sign-in failed',
}));

const mockNavigate = jest.fn();
const mockLogin = jest.fn();
let container, root, resize, disconnect, width;
const originalObserver = global.ResizeObserver;

beforeEach(() => {
  global.IS_REACT_ACT_ENVIRONMENT = true;
  width = 384;
  disconnect = jest.fn();
  global.ResizeObserver = jest.fn(callback => {
    resize = callback;
    return { observe: jest.fn(), disconnect };
  });
  jest.spyOn(HTMLElement.prototype, 'clientWidth', 'get').mockImplementation(() => width);
  renderGoogleButton.mockReset().mockResolvedValue(true);
  mockLogin.mockClear();
  mockNavigate.mockClear();
  container = document.createElement('div');
  document.body.appendChild(container);
  root = createRoot(container);
});

afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  jest.restoreAllMocks();
  global.ResizeObserver = originalObserver;
});

test('rerenders the Google button only when available width changes and disconnects on unmount', async () => {
  await act(async () => root.render(<GoogleSignInButton />));
  expect(renderGoogleButton).toHaveBeenCalledTimes(1);
  await act(async () => { width = 280; resize(); });
  expect(renderGoogleButton).toHaveBeenCalledTimes(2);
  expect(renderGoogleButton.mock.calls[1][0].clientWidth).toBe(280);
  await act(async () => resize());
  expect(renderGoogleButton).toHaveBeenCalledTimes(2);
  await act(async () => root.render(null));
  expect(disconnect).toHaveBeenCalledTimes(1);
  expect(mockLogin).not.toHaveBeenCalled();
});

test('uses the resize event when ResizeObserver is unavailable', async () => {
  global.ResizeObserver = undefined;
  await act(async () => root.render(<GoogleSignInButton />));
  await act(async () => { width = 300; window.dispatchEvent(new Event('resize')); });
  expect(renderGoogleButton).toHaveBeenCalledTimes(2);
});

test('resizing does not interrupt a sign-in already in progress', async () => {
  let finishLogin, pending;
  mockLogin.mockImplementation(() => new Promise(resolve => { finishLogin = resolve; }));
  await act(async () => root.render(<GoogleSignInButton />));
  await act(async () => { pending = renderGoogleButton.mock.calls[0][1].onCredential('unit-test-credential'); });
  await act(async () => { width = 280; resize(); });
  expect(renderGoogleButton).toHaveBeenCalledTimes(1);
  await act(async () => { finishLogin(); await pending; });
  expect(mockNavigate).toHaveBeenCalledWith('/app', { replace: true });
  expect(renderGoogleButton).toHaveBeenCalledTimes(2);
});

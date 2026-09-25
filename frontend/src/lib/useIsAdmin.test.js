import api from './api';
import { fetchAdminMe, clearAdminMeCacheForTests } from './useIsAdmin';

jest.mock('./api', () => ({ get: jest.fn() }));
jest.mock('../context/AuthContext', () => ({ useAuth: jest.fn() }));

beforeEach(() => {
  clearAdminMeCacheForTests();
  jest.clearAllMocks();
  localStorage.setItem('mychat_token', 'session-a');
});

test('deduplicates requests in one session', async () => {
  api.get.mockResolvedValue({ data: { is_admin: true } });
  const first = fetchAdminMe();
  expect(fetchAdminMe()).toBe(first);
  await first;
  expect((await fetchAdminMe()).is_admin).toBe(true);
  expect(api.get).toHaveBeenCalledTimes(1);
});

test('does not permanently cache transient failures', async () => {
  api.get.mockRejectedValueOnce(new Error('offline')).mockResolvedValue({ data: { is_admin: true } });
  expect((await fetchAdminMe()).is_admin).toBe(false);
  expect((await fetchAdminMe()).is_admin).toBe(true);
});

test('does not reuse owner rights for another session', async () => {
  api.get.mockResolvedValueOnce({ data: { is_admin: true } }).mockResolvedValue({ data: { is_admin: false } });
  await fetchAdminMe();
  localStorage.setItem('mychat_token', 'session-b');
  expect((await fetchAdminMe()).is_admin).toBe(false);
});

test('late response from old session cannot poison current cache', async () => {
  let resolveOld;
  api.get.mockImplementationOnce(() => new Promise(resolve => { resolveOld = resolve; }))
    .mockResolvedValue({ data: { is_admin: false } });
  const old = fetchAdminMe();
  localStorage.setItem('mychat_token', 'session-b');
  await fetchAdminMe();
  resolveOld({ data: { is_admin: true } });
  await old;
  expect((await fetchAdminMe()).is_admin).toBe(false);
});

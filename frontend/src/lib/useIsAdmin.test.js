import api from './api';
import { fetchAdminMe, clearAdminMeCache } from './useIsAdmin';

jest.mock('./api', () => ({ __esModule: true, default: { get: jest.fn() } }));

beforeEach(() => {
  clearAdminMeCache();
  api.get.mockReset();
});

test('switching accounts does not reuse the administrator result', async () => {
  api.get.mockResolvedValueOnce({ data: { is_admin: true } });
  expect((await fetchAdminMe()).is_admin).toBe(true);
  clearAdminMeCache();
  api.get.mockResolvedValueOnce({ data: { is_admin: false } });
  expect((await fetchAdminMe()).is_admin).toBe(false);
  expect(api.get).toHaveBeenCalledTimes(2);
});

test('a late administrator response cannot restore old privileges', async () => {
  let completeOld;
  api.get.mockImplementationOnce(() => new Promise(resolve => { completeOld = resolve; }));
  const old = fetchAdminMe();
  clearAdminMeCache();
  api.get.mockResolvedValueOnce({ data: { is_admin: false } });
  await fetchAdminMe();
  completeOld({ data: { is_admin: true } });
  expect((await old).is_admin).toBe(false);
  expect((await fetchAdminMe()).is_admin).toBe(false);
});

test('permission lookup errors fail closed', async () => {
  api.get.mockRejectedValueOnce(new Error('unavailable'));
  expect((await fetchAdminMe()).is_admin).toBe(false);
});

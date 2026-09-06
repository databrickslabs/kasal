import { beforeEach, expect, it, vi } from 'vitest';
import { apiClient } from '../shared/api/client';
import { useUserStore } from './user';
vi.mock('../shared/api/client', () => ({ apiClient: { get: vi.fn() } }));
beforeEach(() => { vi.clearAllMocks(); localStorage.clear(); useUserStore.getState().clearUser(); });
it('refreshes the server allocation when the email has not changed', async () => {
  useUserStore.setState({ currentUser: { id: 'u1', email: 'person@example.com', username: 'person' } });
  vi.mocked(apiClient.get).mockResolvedValue({ data: { id: 'u1', email: 'person@example.com', username: 'person', personal_group_id: 'user_allocated' } });
  await useUserStore.getState().fetchCurrentUser();
  expect(useUserStore.getState().currentUser?.personal_group_id).toBe('user_allocated');
});
it('never derives an allocation from a cached email after an identity failure', async () => {
  localStorage.setItem('userId', 'u1'); localStorage.setItem('userEmail', 'person@example.com');
  vi.mocked(apiClient.get).mockRejectedValue(new Error('unavailable'));
  await useUserStore.getState().fetchCurrentUser();
  expect(useUserStore.getState().currentUser?.personal_group_id).toBeUndefined();
});

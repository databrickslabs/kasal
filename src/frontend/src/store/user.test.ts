import { beforeEach, expect, it, vi } from 'vitest';
import { apiClient } from '../shared/api/client';
import { isOwnPersonalWorkspace, isPersonalWorkspaceId, useUserStore } from './user';
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
it('matches the personal workspace by the stored id, never by a user_ prefix', () => {
  expect(isPersonalWorkspaceId('user_9f8e', 'user_9f8e')).toBe(true);
  expect(isPersonalWorkspaceId('user_marketing_1a2b3c4d', 'user_9f8e')).toBe(false);
  expect(isPersonalWorkspaceId('user_9f8e', null)).toBe(false);
  expect(isPersonalWorkspaceId(null, 'user_9f8e')).toBe(false);
  useUserStore.setState({ currentUser: { id: 'u1', email: 'p@example.com', username: 'p', personal_group_id: 'user_9f8e' } });
  expect(isOwnPersonalWorkspace('user_9f8e')).toBe(true);
  expect(isOwnPersonalWorkspace('user_marketing_1a2b3c4d')).toBe(false);
});

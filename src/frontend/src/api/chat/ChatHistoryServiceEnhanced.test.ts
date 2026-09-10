import { afterEach, expect, it, vi } from 'vitest';
import { apiClient } from '../../shared/api/client';
import { ChatHistoryServiceEnhanced } from './ChatHistoryServiceEnhanced';

vi.mock('../../shared/api/client', () => ({ apiClient: { put: vi.fn() } }));
afterEach(() => { vi.useRealTimers(); vi.resetAllMocks(); localStorage.clear(); });

it('keeps builder edits in their original teamspace across retry and navigation', async () => {
  vi.useFakeTimers();
  localStorage.setItem('selectedGroupId', 'team-a');
  vi.mocked(apiClient.put).mockRejectedValueOnce({ response: { status: 503 } }).mockResolvedValueOnce({});
  const saving = ChatHistoryServiceEnhanced.updateMessageContent('message/id', 'deck', 'team-a');
  localStorage.setItem('selectedGroupId', 'team-b');
  await vi.runAllTimersAsync();
  await saving;
  expect(apiClient.put).toHaveBeenCalledTimes(2);
  for (const call of vi.mocked(apiClient.put).mock.calls) {
    expect(call).toEqual(['/chat-history/messages/message%2Fid', { content: 'deck' }, { headers: { group_id: 'team-a' } }]);
  }
});

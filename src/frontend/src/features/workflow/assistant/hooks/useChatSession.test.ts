import { act, renderHook, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
const history = vi.hoisted(() => ({
  getSessionMessages: vi.fn(), saveMessage: vi.fn(async () => ({})), generateSessionId: () => 'new-session',
}));
vi.mock('../../../../api/chat/ChatHistoryServiceEnhanced', () => ({ ChatHistoryServiceEnhanced: history }));
import { useChatSession } from './useChatSession';
import { useChatMessagesStore } from '../store/chatMessagesStore';
beforeEach(() => {
  vi.clearAllMocks(); useChatMessagesStore.getState().clearAllSessions();
  history.getSessionMessages.mockResolvedValue({ messages: [] });
});
describe('completed response persistence', () => {
  it('saves and restores the execution ID needed by Memory and Schedule', async () => {
    const { result } = renderHook(() => useChatSession('session-1'));
    await act(async () => {
      await result.current.saveMessageToBackend({
        id: 'result-1', type: 'result', content: 'Finished report', jobId: 'original-run', timestamp: new Date(),
      });
    });
    const saved = history.saveMessage.mock.calls[0][0];
    expect(saved.generation_result.jobId).toBe('original-run');
    history.getSessionMessages.mockResolvedValue({ messages: [{
      ...saved, id: 'result-1', timestamp: new Date().toISOString(),
    }] });
    await act(async () => { await result.current.loadSessionMessages('session-1'); });
    await waitFor(() => expect(useChatMessagesStore.getState().getMessages('session-1')[0]).toMatchObject({
      type: 'result', content: 'Finished report', jobId: 'original-run',
    }));
  });
});

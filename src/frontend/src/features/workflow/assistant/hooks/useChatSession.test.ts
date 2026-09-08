import { act, renderHook, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
const history = vi.hoisted(() => ({
  getSessionMessages: vi.fn(), updateMessageContent: vi.fn().mockResolvedValue(undefined), saveMessage: vi.fn(async () => ({})), generateSessionId: () => 'new-session',
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
      type: 'result', backendId: 'result-1', content: 'Finished report', jobId: 'original-run',
    }));
  });
  it('retains the backend identity and saves a surface that arrives during the original text save', async () => {
    let finish!: (value: { id: string }) => void;
    history.saveMessage.mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
    const message = { id: 'local-result', type: 'result' as const, content: 'Initial text', jobId: 'run', timestamp: new Date() };
    useChatMessagesStore.getState().addMessage('session-1', message);
    const { result } = renderHook(() => useChatSession('session-1'));
    let saving!: Promise<void>;
    act(() => { saving = result.current.saveMessageToBackend(message); });
    act(() => { useChatMessagesStore.getState().updateMessage('session-1', 'local-result', { content: 'Composed surface envelope' }); });
    await act(async () => { finish({ id: 'backend-result' }); await saving; });
    expect(useChatMessagesStore.getState().getMessages('session-1')[0]).toMatchObject({
      id: 'local-result', backendId: 'backend-result', content: 'Composed surface envelope',
    });
    expect(history.updateMessageContent).toHaveBeenCalledWith('backend-result', 'Composed surface envelope');
  });

});

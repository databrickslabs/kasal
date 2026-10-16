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

const row = (n: number) => ({
  id: `row-${n}`, session_id: 'long', message_type: 'assistant', content: `message ${n}`,
  timestamp: new Date(Date.UTC(2026, 0, 1, 0, n)).toISOString(),
});
const assistant = (id: string) => ({ id, type: 'assistant' as const, content: `text ${id}`, timestamp: new Date() });

describe('reopened conversations', () => {
  it('loads the whole history past the first page, oldest first', async () => {
    const rows = Array.from({ length: 120 }, (_, i) => row(i + 1));
    // The backend pages ascending by insert time with OFFSET/LIMIT.
    history.getSessionMessages.mockImplementation(async (_sid: string, page = 0, perPage = 50) => ({
      messages: rows.slice(page * perPage, (page + 1) * perPage), total_messages: rows.length, page, per_page: perPage,
    }));
    renderHook(() => useChatSession('long'));
    await waitFor(() => expect(useChatMessagesStore.getState().getMessages('long')).toHaveLength(120));
    expect(useChatMessagesStore.getState().getMessages('long').map(m => m.id)).toEqual(rows.map(r => r.id));
  });

  it('has no page cap that would drop the newest messages of a very long session', async () => {
    const rows = Array.from({ length: 5_050 }, (_, i) => row(i + 1));
    history.getSessionMessages.mockImplementation(async (_sid: string, page = 0, perPage = 50) => ({
      messages: rows.slice(page * perPage, (page + 1) * perPage), page, per_page: perPage,
    }));
    renderHook(() => useChatSession('long'));
    await waitFor(() => expect(useChatMessagesStore.getState().getMessages('long')).toHaveLength(5_050));
    expect(useChatMessagesStore.getState().getMessages('long').at(-1)?.id).toBe('row-5050');
  });

  it('does not duplicate this page session’s messages when switching away and back', async () => {
    const { result, rerender } = renderHook(({ id }) => useChatSession(id), { initialProps: { id: 'dup-a' } });
    const older = { ...row(1), session_id: 'dup-a', id: 'stored-older' };
    const sent = assistant('msg-client-1');
    useChatMessagesStore.getState().addMessage('dup-a', sent);
    // The server mints its own id for the row.
    history.saveMessage.mockResolvedValueOnce({ id: 'server-row-1' });
    await act(async () => { await result.current.saveMessageToBackend(sent); });
    const saved = history.saveMessage.mock.calls[0][0] as Record<string, unknown>;
    let load = 0;
    history.getSessionMessages.mockImplementation(async () => {
      load += 1;
      return { messages: [older, { ...saved, id: 'server-row-1', timestamp: new Date().toISOString() }] };
    });
    rerender({ id: 'dup-b' });
    rerender({ id: 'dup-a' });
    await waitFor(() => expect(load).toBeGreaterThan(0));
    await waitFor(() => expect(useChatMessagesStore.getState().getMessages('dup-a').map(m => m.id)).toContain('stored-older'));
    expect(useChatMessagesStore.getState().getMessages('dup-a').map(m => m.id)).toEqual(['stored-older', 'msg-client-1']);
  });
});

describe('saving after a failure streak', () => {
  it('keeps trying, so saving resumes once the service recovers', async () => {
    history.saveMessage.mockRejectedValueOnce(new Error('503')).mockRejectedValueOnce(new Error('503')).mockRejectedValueOnce(new Error('503'));
    const { result } = renderHook(() => useChatSession('flaky'));
    for (const id of ['a', 'b', 'c']) {
      await act(async () => { await result.current.saveMessageToBackend(assistant(id)); });
    }
    await act(async () => { await result.current.saveMessageToBackend(assistant('d')); });
    expect(history.saveMessage).toHaveBeenCalledTimes(4);
    expect(history.saveMessage).toHaveBeenLastCalledWith(expect.objectContaining({ content: 'text d' }));
  });
});

import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useBuilderResultSurface } from './useBuilderResultSurface';
import { useChatMessagesStore } from '../store/chatMessagesStore';
import type { ChatMessage } from '../types';
import type { StreamEvent } from '../../../chat/api/streaming';
import type { Surface } from '../../../../shared/a2ui';

const mocks = vi.hoisted(() => ({ get: vi.fn(), update: vi.fn(), close: vi.fn(), listen: vi.fn() }));
vi.mock('../../../../api/execution/ExecutionHistoryService', () => ({ runService: { getRunByJobId: mocks.get } }));
vi.mock('../../../../api/chat/ChatHistoryServiceEnhanced', () => ({ ChatHistoryServiceEnhanced: { updateMessageContent: mocks.update } }));
vi.mock('../../../chat/api/streaming', () => ({ streamExecution: mocks.listen }));

const surface: Surface = { surfaceKind: 'mindmap', root: 'mm', components: [{ id: 'mm', component: 'Mindmap', title: 'Cities' }], dataModel: {} };
let message: ChatMessage;
let onEvent: (event: StreamEvent) => void;
beforeEach(() => {
  vi.useFakeTimers();
  vi.clearAllMocks();
  mocks.get.mockResolvedValue({ result: 'Cities answer' });
  mocks.update.mockResolvedValue(undefined);
  mocks.listen.mockImplementation((_id, cb) => { onEvent = cb; return mocks.close; });
  message = { id: 'result', backendId: 'saved-result', type: 'result', jobId: 'crew-run', content: 'Cities answer', timestamp: new Date() };
  useChatMessagesStore.setState({ messagesBySession: { session: [message] } });
});
afterEach(() => { vi.useRealTimers(); });

describe('builder result surface delivery', () => {
  it('upgrades early text when the composed mindmap arrives over REST, without duplicating the result', async () => {
    mocks.get.mockResolvedValueOnce({ result: 'Cities answer' }).mockResolvedValue({ result: { text: 'Cities answer', a2ui: surface } });
    const { result } = renderHook(() => useBuilderResultSurface(message));
    await act(async () => {});
    expect(result.current.content).toBe('Cities answer');
    await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
    expect(JSON.parse(result.current.content).a2ui).toEqual(surface);
    expect(useChatMessagesStore.getState().messagesBySession.session).toHaveLength(1);
    expect(mocks.update).toHaveBeenCalledWith('saved-result', result.current.content);
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
    expect(mocks.get).toHaveBeenCalledTimes(2);
    expect(mocks.close).toHaveBeenCalled();
  });

  it('accepts the final SSE result after crew completion', async () => {
    const { result } = renderHook(() => useBuilderResultSurface(message));
    await act(async () => {});
    act(() => onEvent({ event: 'execution_update', data: { result: { text: 'Cities answer', a2ui: surface } } }));
    expect(JSON.parse(result.current.content).a2ui).toEqual(surface);
    expect(mocks.close).toHaveBeenCalled();
  });

  it('repairs an older text-only transcript with one authoritative read', async () => {
    message.timestamp = new Date(Date.now() - 86400000);
    mocks.get.mockResolvedValue({ result: { text: 'Cities answer', a2ui: surface } });
    const { result } = renderHook(() => useBuilderResultSurface(message));
    await act(async () => {});
    expect(JSON.parse(result.current.content).a2ui).toEqual(surface);
    expect(mocks.listen).not.toHaveBeenCalled();
    expect(mocks.get).toHaveBeenCalledTimes(1);
  });

  it('persists palette changes in the same envelope and does not refetch an existing surface', () => {
    message.content = JSON.stringify({ text: 'Cities answer', a2ui: surface });
    const { result } = renderHook(() => useBuilderResultSurface(message));
    const styled = { ...surface, theme: { accent: '#123456' } } as Surface;
    act(() => result.current.restyle(styled));
    expect(JSON.parse(result.current.content)).toEqual({ text: 'Cities answer', a2ui: styled });
    expect(mocks.update).toHaveBeenCalledWith('saved-result', result.current.content);
    expect(mocks.get).not.toHaveBeenCalled();
  });

  it('does not update a different session after unmount', async () => {
    let resolve!: (value: unknown) => void;
    mocks.get.mockImplementation(() => new Promise(done => { resolve = done; }));
    const view = renderHook(() => useBuilderResultSurface(message));
    view.unmount();
    await act(async () => resolve({ result: { a2ui: surface } }));
    expect(mocks.update).not.toHaveBeenCalled();
    expect(mocks.close).toHaveBeenCalled();
  });

  it('bounds polling for plain results and releases the stream', async () => {
    renderHook(() => useBuilderResultSurface(message));
    await act(async () => { await vi.advanceTimersByTimeAsync(301000); });
    const calls = mocks.get.mock.calls.length;
    await act(async () => { await vi.advanceTimersByTimeAsync(60000); });
    expect(mocks.get).toHaveBeenCalledTimes(calls);
    expect(mocks.close).toHaveBeenCalled();
  });
});


it.each(['raw', 'envelope'])('keeps a %s slide deck instead of replacing it with a late UI surface', async kind => {
  const deck = '<section class="slide"><h1>Saved edits</h1></section>';
  message.content = kind === 'raw' ? deck : JSON.stringify({ output: JSON.stringify({ text: deck }) });
  mocks.get.mockResolvedValue({ result: { text: 'Original answer', a2ui: surface } });
  const { result } = renderHook(() => useBuilderResultSurface(message));
  await act(async () => { await vi.advanceTimersByTimeAsync(5000); });
  expect(result.current.content).toBe(message.content);
  expect(mocks.get).not.toHaveBeenCalled();
  expect(mocks.listen).not.toHaveBeenCalled();
});

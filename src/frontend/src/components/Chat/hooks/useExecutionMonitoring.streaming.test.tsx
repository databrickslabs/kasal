/**
 * llm_chunk streaming into the live chat bubble is COALESCED per animation
 * frame, not painted per SSE frame.
 *
 * A hierarchical crew with large outputs emits llm_chunk at ~30/sec; painting
 * each token meant one store set() + a re-render of a bubble growing to tens of
 * KB, which froze the tab ("Page Unresponsive"). These tests pin that a burst of
 * chunks collapses into a single paint per frame, and that the buffer flushes
 * (not leaks) on cleanup.
 */

import { renderHook, act } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';

// Capture the callback streamExecution is invoked with, so the test can drive
// llm_chunk frames directly. Returns a no-op unsubscribe.
let streamCb: ((event: { event: string; data: Record<string, unknown> }) => void) | null = null;
const closeSpy = vi.fn();
vi.mock('../../ChatMode/api/streaming', () => ({
  streamExecution: vi.fn((_jobId: string, cb: (e: { event: string; data: Record<string, unknown> }) => void) => {
    streamCb = cb;
    return closeSpy;
  }),
}));

vi.mock('../../../utils/taskIdUtils', () => ({
  extractTaskId: vi.fn().mockReturnValue(null),
  extractTaskName: vi.fn().mockReturnValue(null),
  mapEventToStatus: vi.fn().mockReturnValue('running'),
}));

vi.mock('../../../api/execution/ExecutionHistoryService', () => ({
  runService: {
    getRuns: vi.fn().mockResolvedValue({ runs: [] }),
    getRunByJobId: vi.fn().mockResolvedValue(null),
  },
}));

vi.mock('../../../store/taskExecutionStore', () => ({
  useTaskExecutionStore: Object.assign(
    () => ({
      clearTaskStates: vi.fn(),
      loadTaskStates: vi.fn(),
      transition: vi.fn().mockReturnValue(true),
      getTaskStatus: vi.fn().mockReturnValue(null),
    }),
    { getState: () => ({ clearTaskStates: vi.fn(), transitionAll: vi.fn(), transition: vi.fn().mockReturnValue(true) }) }
  ),
}));

// One shared set of spies for the message store, so call counts are stable
// across getState() calls (the real store is a singleton).
const addMessage = vi.fn();
const appendToMessage = vi.fn();
const removeMessage = vi.fn();
const storeState = {
  setMessages: vi.fn(),
  addMessage,
  appendToMessage,
  removeMessage,
  getMessagesForSession: vi.fn().mockReturnValue([]),
};
vi.mock('../../../store/chatMessagesStore', () => ({
  useChatMessagesStore: Object.assign(() => storeState, { getState: () => storeState }),
}));

import { useExecutionMonitoring } from './useExecutionMonitoring';

// Drive requestAnimationFrame deterministically.
let rafQueue: FrameRequestCallback[] = [];
const runFrame = () => {
  const q = rafQueue;
  rafQueue = [];
  q.forEach(cb => cb(performance.now()));
};

beforeEach(() => {
  vi.clearAllMocks();
  streamCb = null;
  rafQueue = [];
  vi.spyOn(window, 'requestAnimationFrame').mockImplementation((cb: FrameRequestCallback) => {
    rafQueue.push(cb);
    return rafQueue.length;
  });
  vi.spyOn(window, 'cancelAnimationFrame').mockImplementation(() => {});
});

afterEach(() => {
  vi.restoreAllMocks();
});

const noop = vi.fn().mockResolvedValue(undefined);

describe('useExecutionMonitoring — llm_chunk coalescing', () => {
  it('collapses a burst of chunks into a single paint per frame', () => {
    const { result } = renderHook(() =>
      useExecutionMonitoring('session-1', noop, vi.fn())
    );

    act(() => { result.current.setExecutingJobId('job-1'); });
    expect(streamCb).toBeTruthy();

    // Fire 30 chunks WITHOUT advancing a frame.
    act(() => {
      for (let i = 0; i < 30; i += 1) {
        streamCb!({ event: 'llm_chunk', data: { chunk: `tok${i} ` } });
      }
    });

    // Nothing painted yet — all deferred to the next frame.
    expect(addMessage).not.toHaveBeenCalled();
    expect(appendToMessage).not.toHaveBeenCalled();

    // One frame: the whole burst lands as a SINGLE addMessage (first paint
    // creates the bubble), carrying all 30 tokens concatenated.
    act(() => { runFrame(); });
    expect(addMessage).toHaveBeenCalledTimes(1);
    const created = addMessage.mock.calls[0][1] as { content: string };
    expect(created.content).toContain('tok0');
    expect(created.content).toContain('tok29');
    expect(appendToMessage).not.toHaveBeenCalled();
  });

  it('appends subsequent frames into the same bubble, one paint per frame', () => {
    const { result } = renderHook(() =>
      useExecutionMonitoring('session-1', noop, vi.fn())
    );
    act(() => { result.current.setExecutingJobId('job-1'); });

    act(() => { streamCb!({ event: 'llm_chunk', data: { chunk: 'a' } }); });
    act(() => { runFrame(); });
    expect(addMessage).toHaveBeenCalledTimes(1);

    // Second burst → one append for the frame, not one per chunk.
    act(() => {
      streamCb!({ event: 'llm_chunk', data: { chunk: 'b' } });
      streamCb!({ event: 'llm_chunk', data: { chunk: 'c' } });
    });
    act(() => { runFrame(); });
    expect(appendToMessage).toHaveBeenCalledTimes(1);
    expect(appendToMessage.mock.calls[0][2]).toBe('bc');
  });

  it('drops the transient bubble on cleanup', () => {
    const { result, unmount } = renderHook(() =>
      useExecutionMonitoring('session-1', noop, vi.fn())
    );
    act(() => { result.current.setExecutingJobId('job-1'); });
    act(() => { streamCb!({ event: 'llm_chunk', data: { chunk: 'hello' } }); });
    act(() => { runFrame(); });

    unmount();
    expect(removeMessage).toHaveBeenCalled();
    expect(closeSpy).toHaveBeenCalled();
  });
});

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
vi.mock('../../../chat/api/streaming', () => ({
  streamExecution: vi.fn((_jobId: string, cb: (e: { event: string; data: Record<string, unknown> }) => void) => {
    streamCb = cb;
    return closeSpy;
  }),
}));

vi.mock('../../../../utils/taskIdUtils', () => ({
  extractTaskId: vi.fn().mockReturnValue(null),
  extractTaskName: vi.fn().mockReturnValue(null),
  mapEventToStatus: vi.fn().mockReturnValue('running'),
}));

vi.mock('../../../../api/execution/ExecutionHistoryService', () => ({
  runService: {
    getRuns: vi.fn().mockResolvedValue({ runs: [] }),
    getRunByJobId: vi.fn().mockResolvedValue(null),
  },
}));

vi.mock('../../../../store/taskExecutionStore', () => ({
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
  messagesBySession: {},
  setMessages: vi.fn(),
  addMessage,
  appendToMessage,
  removeMessage,
  getMessagesForSession: vi.fn().mockReturnValue([]),
};
vi.mock('../store/chatMessagesStore', () => ({
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
  sessionStorage.clear();
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

  it('drops the transient bubble when the execution ends', () => {
    const { result, unmount } = renderHook(() =>
      useExecutionMonitoring('session-1', noop, vi.fn())
    );
    act(() => { result.current.setExecutingJobId('job-1'); });
    act(() => { streamCb!({ event: 'llm_chunk', data: { chunk: 'hello' } }); });
    act(() => { runFrame(); });

    act(() => { window.dispatchEvent(new CustomEvent('forceClearExecution')); });
    unmount();
    expect(removeMessage).toHaveBeenCalled();
    expect(closeSpy).toHaveBeenCalled();
  });
});

describe('Execution completion reconciliation', () => {
  it.each(['completed', 'failed', 'stopped', 'cancelled'])('unblocks the input on an SSE %s status', status => {
    const { result } = renderHook(() => useExecutionMonitoring('terminal-session', noop, vi.fn()));
    act(() => { result.current.setExecutingJobId('terminal-job'); });
    act(() => { streamCb!({ event: 'execution_update', data: { status } }); });
    expect(result.current.executingJobId).toBeNull();
    expect(closeSpy).toHaveBeenCalled();
  });

  it('recovers completion when the terminal stream event is missed', async () => {
    const { runService } = await import('../../../../api/execution/ExecutionHistoryService');
    vi.mocked(runService.getRunByJobId).mockResolvedValueOnce({ status: 'COMPLETED' } as never);
    const { result } = renderHook(() => useExecutionMonitoring('poll-session', noop, vi.fn()));
    await act(async () => { result.current.setExecutingJobId('poll-job'); });
    expect(result.current.executingJobId).toBeNull();
  });

  it('ignores an old status request after switching to another execution', async () => {
    const { runService } = await import('../../../../api/execution/ExecutionHistoryService');
    let finish!: (value: never) => void;
    vi.mocked(runService.getRunByJobId).mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
    const { result } = renderHook(() => useExecutionMonitoring('switch-session', noop, vi.fn()));
    act(() => { result.current.setExecutingJobId('old-job'); });
    act(() => { result.current.setExecutingJobId('new-job'); });
    await act(async () => { finish({ status: 'completed' } as never); });
    expect(result.current.executingJobId).toBe('new-job');
  });
});

it('posts the exact run result once when completion arrives through multiple channels', async () => {
  vi.useFakeTimers();
  try {
    const { runService } = await import('../../../../api/execution/ExecutionHistoryService');
    vi.mocked(runService.getRunByJobId).mockResolvedValueOnce(null);
    const { result } = renderHook(() => useExecutionMonitoring('result-session', noop, vi.fn()));
    await act(async () => {
      window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: 'result-job', jobName: 'Research' } }));
    });
    vi.mocked(runService.getRunByJobId).mockResolvedValueOnce({ result: { output: 'Final report' } } as never);
    act(() => {
      streamCb!({ event: 'execution_update', data: { status: 'COMPLETED' } });
      window.dispatchEvent(new CustomEvent('jobCompleted', { detail: { jobId: 'result-job' } }));
    });
    await act(async () => { await vi.advanceTimersByTimeAsync(2100); });
    expect(result.current.executingJobId).toBeNull();
    expect(addMessage.mock.calls.filter(([, message]) => message.type === 'result')).toHaveLength(1);
    expect(addMessage).toHaveBeenCalledWith('result-session', expect.objectContaining({ content: 'Final report', jobId: 'result-job' }));
  } finally { vi.useRealTimers(); }
});

describe('Run recovery after mode changes and refresh', () => {
  it('reconnects to the same job after unmounting and remounting the builder', async () => {
    const first = renderHook(() => useExecutionMonitoring('recover-session', noop, vi.fn()));
    await act(async () => { first.result.current.setExecutingJobId('recover-job'); });
    first.unmount();
    const second = renderHook(() => useExecutionMonitoring('recover-session', noop, vi.fn()));
    expect(second.result.current.executingJobId).toBe('recover-job');
    expect(streamCb).toBeTruthy();
  });

  it('restores a completed result using only the persisted run binding', async () => {
    vi.useFakeTimers();
    try {
      const workspace = localStorage.getItem('selectedGroupId') || 'default';
      sessionStorage.setItem(`kasal-builder-run:${workspace}:refresh-session`, 'finished-while-away');
      const { runService } = await import('../../../../api/execution/ExecutionHistoryService');
      vi.mocked(runService.getRunByJobId).mockResolvedValueOnce({ status: 'COMPLETED' } as never)
        .mockResolvedValueOnce({ result: { output: 'Recovered report' } } as never);
      const { result } = renderHook(() => useExecutionMonitoring('refresh-session', noop, vi.fn()));
      await act(async () => { await vi.advanceTimersByTimeAsync(2100); });
      expect(result.current.executingJobId).toBeNull();
      expect(addMessage).toHaveBeenCalledWith('refresh-session', expect.objectContaining({ type: 'result', content: 'Recovered report', jobId: 'finished-while-away' }));
    } finally { vi.useRealTimers(); }
  });

  it('does not bind the previous conversation’s run to a new conversation', async () => {
    const { result, rerender } = renderHook(({ session }) => useExecutionMonitoring(session, noop, vi.fn()), { initialProps: { session: 'old-conversation' } });
    await act(async () => { result.current.setExecutingJobId('old-conversation-job'); });
    rerender({ session: 'new-conversation' });
    expect(result.current.executingJobId).toBeNull();
    const workspace = localStorage.getItem('selectedGroupId') || 'default';
    expect(sessionStorage.getItem(`kasal-builder-run:${workspace}:new-conversation`)).toBeNull();
  });
});

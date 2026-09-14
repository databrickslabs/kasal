import { act, cleanup, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';

vi.mock('../../utils/sseTransport', () => ({ SSE_ENABLED: true }));
const { apiGet, runState } = vi.hoisted(() => ({
  apiGet: vi.fn(),
  runState: { sseConnected: true, handleSSEUpdate: vi.fn(), addTraces: vi.fn() },
}));
vi.mock('../../shared/api/client', () => ({
  config: { apiUrl: '/api/v1' }, apiClient: { get: apiGet },
}));
vi.mock('../../store/runStatus', () => ({ useRunStatusStore: { getState: () => runState } }));
vi.mock('../../store/flowExecutionStore', () => ({
  useFlowExecutionStore: { getState: () => ({ currentJobId: null }) },
}));
vi.mock('../../store/taskExecutionStore', () => ({
  useTaskExecutionStore: { getState: () => ({ transition: vi.fn() }) },
}));
vi.mock('../../api/execution/HITLService', () => ({
  HITLService: { getExecutionHITLStatus: vi.fn().mockResolvedValue(null) },
}));
import { useTracePolling } from './useTracePolling';
import { useSSE } from './useSSE';

beforeEach(() => {
  vi.useFakeTimers();
  vi.clearAllMocks();
  apiGet.mockReset().mockResolvedValue({ data: { status: 'running', execution_type: 'agent', traces: [] } });
});
afterEach(() => { cleanup(); vi.useRealTimers(); vi.unstubAllGlobals(); });

function earlyTrace() {
  window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: 'job-a' } }));
  window.dispatchEvent(new CustomEvent('traceUpdate', { detail: { jobId: 'job-a', trace: { id: 1 } } }));
}

it('starts polling after disconnection despite an early successful SSE trace', async () => {
  const { rerender } = renderHook(({ state }) => useTracePolling(state), { initialProps: { state: 'connected' } });
  earlyTrace();
  await vi.advanceTimersByTimeAsync(4000);
  expect(apiGet).not.toHaveBeenCalled();
  rerender({ state: 'connecting' });
  await vi.advanceTimersByTimeAsync(0);
  expect(apiGet).toHaveBeenCalledWith('/executions/job-a', expect.anything());
});

it('reconciles a silently stalled stream without an error notification', async () => {
  renderHook(() => useTracePolling());
  earlyTrace();
  await vi.advanceTimersByTimeAsync(15000);
  expect(apiGet).toHaveBeenCalledWith('/executions/job-a', expect.anything());
});

it('reconciles immediately when returning to a tab using SSE', async () => {
  renderHook(() => useTracePolling());
  earlyTrace();
  document.dispatchEvent(new Event('visibilitychange'));
  await vi.advanceTimersByTimeAsync(0);
  expect(apiGet).toHaveBeenCalledWith('/executions/job-a', expect.anything());
});

it('does not resume polling a completed job after disconnection or silence', async () => {
  const { rerender } = renderHook(({ state }) => useTracePolling(state), { initialProps: { state: 'connected' } });
  earlyTrace();
  window.dispatchEvent(new CustomEvent('jobCompleted', { detail: { jobId: 'job-a' } }));
  rerender({ state: 'disconnected' });
  await vi.advanceTimersByTimeAsync(30000);
  expect(apiGet).not.toHaveBeenCalled();
});

it('aborts stalled requests at the deadline and allows the next poll', async () => {
  apiGet.mockImplementation(() => new Promise(() => {}));
  const { result } = renderHook(() => useTracePolling());
  act(() => result.current.startPolling('job-a'));
  const signal = apiGet.mock.calls[0][1].signal as AbortSignal;
  await vi.advanceTimersByTimeAsync(15000);
  expect(signal.aborted).toBe(true);
  await vi.advanceTimersByTimeAsync(1000);
  expect(apiGet).toHaveBeenCalledTimes(4);
});

it('finishes a completed run even when the final trace request times out', async () => {
  apiGet.mockImplementation((url: string, config: { signal: AbortSignal; params?: { limit: number } }) => {
    if (url.startsWith('/executions/')) return Promise.resolve({ data: { status: 'completed' } });
    if (config.params?.limit === 500) {
      return new Promise((_resolve, reject) => {
        config.signal.addEventListener('abort', () => reject(new Error('aborted')), { once: true });
      });
    }
    return Promise.resolve({ data: { traces: [] } });
  });
  const { result } = renderHook(() => useTracePolling());
  act(() => result.current.startPolling('job-a'));
  await vi.advanceTimersByTimeAsync(16000);
  expect(runState.handleSSEUpdate).toHaveBeenCalledWith(expect.objectContaining({ status: 'completed' }));
  const count = apiGet.mock.calls.length;
  await vi.advanceTimersByTimeAsync(10000);
  expect(apiGet).toHaveBeenCalledTimes(count);
});

it('recovers final traces before the completion event cancels polling', async () => {
  apiGet.mockImplementation((url: string) => Promise.resolve({ data: url.startsWith('/executions/')
    ? { status: 'completed' } : { traces: [{ id: 99 }] } }));
  runState.handleSSEUpdate.mockImplementation(() => {
    window.dispatchEvent(new CustomEvent('jobCompleted', { detail: { jobId: 'job-a' } }));
  });
  const { result } = renderHook(() => useTracePolling());
  act(() => result.current.startPolling('job-a'));
  await vi.advanceTimersByTimeAsync(0);
  expect(runState.addTraces).toHaveBeenCalledWith('job-a', [{ id: 99 }]);
  expect(runState.handleSSEUpdate).toHaveBeenCalledWith(expect.objectContaining({ status: 'completed' }));
  runState.handleSSEUpdate.mockReset();
});

it('cancels the old job and ignores late responses without unlocking a new poll', async () => {
  let resolveOld!: (value: unknown) => void;
  const oldResponse = new Promise(resolve => { resolveOld = resolve; });
  apiGet.mockImplementation((url: string) => url.includes('job-a') ? oldResponse : new Promise(() => {}));
  const { result, unmount } = renderHook(() => useTracePolling());
  act(() => result.current.startPolling('job-a'));
  const oldSignal = apiGet.mock.calls[0][1].signal as AbortSignal;
  act(() => result.current.startPolling('job-b'));
  const newSignal = apiGet.mock.calls[2][1].signal as AbortSignal;
  expect(oldSignal.aborted).toBe(true);
  resolveOld({ data: { status: 'completed', traces: [{ id: 99 }] } });
  await vi.advanceTimersByTimeAsync(4000);
  expect(apiGet).toHaveBeenCalledTimes(4);
  expect(runState.handleSSEUpdate).not.toHaveBeenCalled();
  expect(runState.addTraces).not.toHaveBeenCalled();
  unmount();
  expect(newSignal.aborted).toBe(true);
});

function mockSources() {
  const instances: FakeSource[] = [];
  class FakeSource {
    static CONNECTING = 0;
    static OPEN = 1;
    static CLOSED = 2;
    readyState = 0;
    onerror: (() => void) | null = null;
    onopen: (() => void) | null = null;
    listeners = new Map<string, (event: unknown) => void>();
    constructor(public url: string) { instances.push(this); }
    addEventListener(type: string, listener: (event: unknown) => void) { this.listeners.set(type, listener); }
    close() { this.readyState = 2; }
  }
  vi.stubGlobal('EventSource', FakeSource);
  return instances;
}

it('replaces a CLOSED EventSource with its replay cursor and ignores stale callbacks', async () => {
  const instances = mockSources();
  const onMessage = vi.fn();
  renderHook(() => useSSE('/sse/executions/stream-all', onMessage));
  const first = instances[0];
  act(() => {
    first.readyState = 1;
    first.onopen?.();
    first.listeners.get('trace')?.({ lastEventId: '42', data: '{}' });
    first.close();
    first.onerror?.();
  });
  await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
  expect(instances).toHaveLength(2);
  expect(new URL(instances[1].url, window.location.origin).searchParams.get('last_event_id')).toBe('42');
  act(() => { first.listeners.get('trace')?.({ lastEventId: '43', data: '{}' }); });
  expect(onMessage).toHaveBeenCalledTimes(1);
});

it('bounds retries across CLOSED replacements instead of resetting the error count', async () => {
  const instances = mockSources();
  const onError = vi.fn();
  renderHook(() => useSSE('/test', vi.fn(), { onError, maxReconnectAttempts: 2 }));
  act(() => { instances[0].close(); instances[0].onerror?.(); });
  await act(async () => { await vi.advanceTimersByTimeAsync(1000); });
  act(() => { instances[1].close(); instances[1].onerror?.(); });
  await act(async () => { await vi.advanceTimersByTimeAsync(60000); });
  expect(instances).toHaveLength(2);
  expect(onError).toHaveBeenLastCalledWith(expect.objectContaining({ isFatal: true, reconnectAttempt: 2 }));
});

it('replaces stuck CONNECTING connections and cancels pending recovery on unmount', async () => {
  const instances = mockSources();
  const { unmount } = renderHook(() => useSSE('/test', vi.fn()));
  await act(async () => { await vi.advanceTimersByTimeAsync(31000); });
  expect(instances).toHaveLength(2);
  expect(instances[0].readyState).toBe(2);
  act(() => { instances[1].close(); instances[1].onerror?.(); });
  unmount();
  await vi.advanceTimersByTimeAsync(60000);
  expect(instances).toHaveLength(2);
});

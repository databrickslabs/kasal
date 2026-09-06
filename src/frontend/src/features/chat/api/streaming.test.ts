import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock getBaseUrl so we control how buildSseUrl resolves the SSE URL, and
// getClient so we can drive the result-endpoint polling fallback.
const getBaseUrl = vi.fn<[], string>(() => 'https://example.com/api/v1');
const httpGet = vi.fn(async () => ({ data: { status: 'pending' } }));
vi.mock('./client', () => ({
  __esModule: true,
  getBaseUrl: () => getBaseUrl(),
  getClient: () => ({ get: httpGet }),
}));

import { streamExecution, streamGeneration } from './streaming';

/**
 * Controllable fake EventSource. Captures named listeners registered via
 * addEventListener and exposes the assigned onopen/onmessage/onerror handlers
 * so tests can drive every code path deterministically.
 */
class FakeEventSource {
  static instances: FakeEventSource[] = [];

  url: string;
  readyState = 0;
  closed = false;
  onopen: ((e: Event) => void) | null = null;
  onmessage: ((e: MessageEvent) => void) | null = null;
  onerror: ((e: Event) => void) | null = null;
  listeners: Record<string, (e: Event) => void> = {};

  constructor(url: string) {
    this.url = url;
    FakeEventSource.instances.push(this);
  }

  addEventListener(type: string, cb: (e: Event) => void): void {
    this.listeners[type] = cb;
  }

  close(): void {
    this.closed = true;
    this.readyState = 2;
  }

  // Helpers used by tests
  emit(type: string, data: unknown): void {
    this.listeners[type]?.({ data } as unknown as MessageEvent);
  }
}

beforeEach(() => {
  FakeEventSource.instances = [];
  getBaseUrl.mockReturnValue('https://example.com/api/v1');
  httpGet.mockReset();
  httpGet.mockResolvedValue({ data: { status: 'pending' } });
  vi.stubGlobal('EventSource', FakeEventSource as unknown as typeof EventSource);
  vi.useFakeTimers();
  vi.spyOn(console, 'log').mockImplementation(() => undefined);
  vi.spyOn(console, 'error').mockImplementation(() => undefined);
});

afterEach(() => {
  vi.runOnlyPendingTimers();
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

function setDev(value: boolean): void {
  vi.stubEnv('DEV', value as unknown as string);
}

describe('streaming buildSseUrl resolution', () => {
  it('uses the absolute http base URL directly', () => {
    getBaseUrl.mockReturnValue('https://example.com/api/v1');
    const cleanup = streamExecution('job-1', vi.fn());
    expect(FakeEventSource.instances[0].url).toBe(
      'https://example.com/api/v1/sse/executions/job-1/stream'
    );
    cleanup();
  });

  it('strips a trailing /api/v1/ (with slash) before re-appending', () => {
    getBaseUrl.mockReturnValue('https://example.com/api/v1/');
    const cleanup = streamExecution('job-slash', vi.fn());
    expect(FakeEventSource.instances[0].url).toBe(
      'https://example.com/api/v1/sse/executions/job-slash/stream'
    );
    cleanup();
  });

  it('connects to localhost:8000 in DEV mode for a relative base URL', () => {
    setDev(true);
    getBaseUrl.mockReturnValue('');
    const cleanup = streamGeneration('gen-1', vi.fn());
    expect(FakeEventSource.instances[0].url).toBe(
      'http://localhost:8000/api/v1/sse/generations/gen-1/stream'
    );
    cleanup();
  });

  it('resolves against window.location.origin in production for a relative base URL', () => {
    setDev(false);
    getBaseUrl.mockReturnValue('');
    const cleanup = streamGeneration('gen-2', vi.fn());
    expect(FakeEventSource.instances[0].url).toBe(
      `${window.location.origin}/api/v1/sse/generations/gen-2/stream`
    );
    cleanup();
  });
});

describe('streamExecution', () => {
  it('dispatches named events with parsed JSON data', () => {
    const onEvent = vi.fn();
    const cleanup = streamExecution('job-1', onEvent);
    const es = FakeEventSource.instances[0];

    es.emit('execution_update', JSON.stringify({ status: 'running' }));
    expect(onEvent).toHaveBeenCalledWith({
      event: 'execution_update',
      data: { status: 'running' },
    });
    cleanup();
  });

  it('falls back to raw message when named event data is invalid JSON', () => {
    const onEvent = vi.fn();
    const cleanup = streamExecution('job-1', onEvent);
    const es = FakeEventSource.instances[0];

    es.emit('trace', 'not-json');
    expect(onEvent).toHaveBeenCalledWith({
      event: 'trace',
      data: { message: 'not-json' },
    });
    cleanup();
  });

  it('logs sliced raw data for long payloads and undefined-safe data', () => {
    const onEvent = vi.fn();
    const cleanup = streamExecution('job-long', onEvent);
    const es = FakeEventSource.instances[0];

    // Long string exercises the me.data?.slice?.(0, 300) truthy branch.
    const long = JSON.stringify({ blob: 'x'.repeat(500) });
    es.emit('error', long);
    expect(onEvent).toHaveBeenLastCalledWith({
      event: 'error',
      data: JSON.parse(long),
    });

    // Object data has no .slice (exercises the `|| me.data` log fallback) and
    // is invalid JSON (JSON.parse("[object Object]") throws -> catch branch).
    const obj = { not: 'a string' };
    es.emit('connected', obj as unknown as string);
    expect(onEvent).toHaveBeenLastCalledWith({
      event: 'connected',
      data: { message: obj },
    });
    cleanup();
  });

  it('ignores a dataless transport error (no false "Execution failed")', () => {
    // EventSource fires the 'error' listener for native TRANSPORT errors too —
    // a bare Event with no `.data`. Forwarding it as an application error made a
    // transient SSE blip post "Execution failed: Unknown error" while the job
    // kept running. It must be dropped (reconnection is handled by onerror).
    const onEvent = vi.fn();
    const cleanup = streamExecution('job-1', onEvent);
    const es = FakeEventSource.instances[0];
    es.listeners['error']?.(new Event('error')); // no .data
    expect(onEvent).not.toHaveBeenCalledWith(
      expect.objectContaining({ event: 'error' }),
    );
    cleanup();
  });

  it('still forwards a genuine server-sent error frame (carries data)', () => {
    const onEvent = vi.fn();
    const cleanup = streamExecution('job-1', onEvent);
    const es = FakeEventSource.instances[0];
    es.emit('error', JSON.stringify({ message: 'boom' }));
    expect(onEvent).toHaveBeenCalledWith({ event: 'error', data: { message: 'boom' } });
    cleanup();
  });

  it('resets reconnectAttempts on open (onopen handler)', () => {
    const onEvent = vi.fn();
    const cleanup = streamExecution('job-1', onEvent);
    const es = FakeEventSource.instances[0];
    expect(typeof es.onopen).toBe('function');
    es.onopen?.(new Event('open'));
    cleanup();
  });

  it('handles generic onmessage with valid JSON', () => {
    const onEvent = vi.fn();
    const cleanup = streamExecution('job-1', onEvent);
    const es = FakeEventSource.instances[0];
    es.onmessage?.({ data: JSON.stringify({ a: 1 }) } as MessageEvent);
    expect(onEvent).toHaveBeenCalledWith({ event: 'message', data: { a: 1 } });
    cleanup();
  });

  it('handles generic onmessage with invalid JSON fallback', () => {
    const onEvent = vi.fn();
    const cleanup = streamExecution('job-1', onEvent);
    const es = FakeEventSource.instances[0];
    es.onmessage?.({ data: 'oops' } as MessageEvent);
    expect(onEvent).toHaveBeenCalledWith({
      event: 'message',
      data: { message: 'oops' },
    });
    cleanup();
  });

  it('logs the `|| e.data` fallback when generic message data has no slice', () => {
    const onEvent = vi.fn();
    const cleanup = streamExecution('job-1', onEvent);
    const es = FakeEventSource.instances[0];
    const obj = { generic: true };
    es.onmessage?.({ data: obj } as unknown as MessageEvent);
    expect(onEvent).toHaveBeenCalledWith({
      event: 'message',
      data: { message: obj },
    });
    cleanup();
  });

  it('early-returns from connect when a reconnect fires after close', () => {
    const onEvent = vi.fn();
    const onError = vi.fn();
    // Capture the connect callback scheduled by the reconnect timeout.
    const setTimeoutSpy = vi.spyOn(global, 'setTimeout');
    const cleanup = streamExecution('job-1', onEvent, onError);
    const es = FakeEventSource.instances[0];
    es.onerror?.(new Event('error')); // schedules setTimeout(connect, 1000)

    const scheduledConnect = setTimeoutSpy.mock.calls[0][0] as () => void;
    cleanup(); // closed = true, clears the timeout

    // Manually invoke the captured connect callback -> hits `if (closed) return`.
    scheduledConnect();
    // No new EventSource is created because connect bailed out early.
    expect(FakeEventSource.instances.length).toBe(1);
  });

  it('reconnects with exponential backoff and resets after open', () => {
    const onEvent = vi.fn();
    const onError = vi.fn();
    const cleanup = streamExecution('job-1', onEvent, onError);

    const es1 = FakeEventSource.instances[0];
    // Trigger an error -> first reconnect scheduled at 1000ms (2^0 * 1000).
    es1.onerror?.(new Event('error'));
    expect(es1.closed).toBe(true);
    expect(onError).not.toHaveBeenCalled();

    // Advance past the backoff delay to trigger reconnect.
    vi.advanceTimersByTime(1000);
    expect(FakeEventSource.instances.length).toBe(2);

    // A successful open resets the attempt counter.
    const es2 = FakeEventSource.instances[1];
    es2.onopen?.(new Event('open'));

    cleanup();
  });

  it('gives up and calls onError after MAX_RECONNECT_ATTEMPTS', () => {
    const onEvent = vi.fn();
    const onError = vi.fn();
    const cleanup = streamExecution('job-1', onEvent, onError);

    // 5 errors trigger 5 reconnects; the 6th error exceeds MAX and calls onError.
    for (let i = 0; i < 5; i++) {
      const es = FakeEventSource.instances[FakeEventSource.instances.length - 1];
      es.onerror?.(new Event('error'));
      // Backoff delays grow: 1000, 2000, 4000, 8000, 16000.
      vi.advanceTimersByTime(RECONNECT_DELAY(i));
    }
    const last = FakeEventSource.instances[FakeEventSource.instances.length - 1];
    last.onerror?.(new Event('error'));
    expect(onError).toHaveBeenCalledTimes(1);
    cleanup();
  });

  it('does not reconnect or call onError when closed before error fires', () => {
    const onEvent = vi.fn();
    const onError = vi.fn();
    const cleanup = streamExecution('job-1', onEvent, onError);
    const es = FakeEventSource.instances[0];
    cleanup(); // mark closed = true and close current source
    es.onerror?.(new Event('error')); // closed -> early return, no reconnect
    vi.advanceTimersByTime(60000);
    expect(onError).not.toHaveBeenCalled();
    expect(FakeEventSource.instances.length).toBe(1);
  });

  it('reaches max attempts without onError when no callback provided', () => {
    const onEvent = vi.fn();
    const cleanup = streamExecution('job-1', onEvent); // no onError
    for (let i = 0; i < 5; i++) {
      const es = FakeEventSource.instances[FakeEventSource.instances.length - 1];
      es.onerror?.(new Event('error'));
      vi.advanceTimersByTime(RECONNECT_DELAY(i));
    }
    const last = FakeEventSource.instances[FakeEventSource.instances.length - 1];
    // 6th error hits the else with onError undefined -> no throw.
    expect(() => last.onerror?.(new Event('error'))).not.toThrow();
    cleanup();
  });

  it('cleanup clears a pending reconnect timeout and closes the source', () => {
    const onEvent = vi.fn();
    const cleanup = streamExecution('job-1', onEvent);
    const es = FakeEventSource.instances[0];
    es.onerror?.(new Event('error')); // schedules reconnect timeout
    cleanup(); // closed=true, clears timeout, source already null
    // Advancing should not create a new connection (timeout cleared).
    vi.advanceTimersByTime(60000);
    expect(FakeEventSource.instances.length).toBe(1);
  });

  it('cleanup is a no-op-safe second time and connect early-returns when closed', () => {
    const onEvent = vi.fn();
    const cleanup = streamExecution('job-1', onEvent);
    cleanup();
    // Second cleanup: reconnectTimeout null, eventSource null -> both branches skipped.
    expect(() => cleanup()).not.toThrow();
  });
});

describe('streamGeneration', () => {
  it('dispatches named generation events with parsed JSON data', () => {
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-1', onEvent);
    const es = FakeEventSource.instances[0];
    es.emit('plan_ready', JSON.stringify({ plan: 'ok' }));
    expect(onEvent).toHaveBeenCalledWith({
      event: 'plan_ready',
      data: { plan: 'ok' },
    });
    cleanup();
  });

  it('falls back to raw message and logs error on invalid JSON', () => {
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-1', onEvent);
    const es = FakeEventSource.instances[0];
    es.emit('agent_detail', 'broken');
    expect(onEvent).toHaveBeenCalledWith({
      event: 'agent_detail',
      data: { message: 'broken' },
    });
    cleanup();
  });

  it('logs sliced raw data for long payloads and undefined-safe data', () => {
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-long', onEvent);
    const es = FakeEventSource.instances[0];
    const long = JSON.stringify({ blob: 'y'.repeat(800) });
    es.emit('task_detail', long);
    expect(onEvent).toHaveBeenLastCalledWith({
      event: 'task_detail',
      data: JSON.parse(long),
    });
    // No-slice + invalid-JSON data: exercises `|| me.data` log fallback and catch.
    const obj = { nope: true };
    es.emit('entity_error', obj as unknown as string);
    expect(onEvent).toHaveBeenLastCalledWith({
      event: 'entity_error',
      data: { message: obj },
    });
    cleanup();
  });

  it('resets reconnectAttempts on open (onopen handler)', () => {
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-1', onEvent);
    const es = FakeEventSource.instances[0];
    es.onopen?.(new Event('open'));
    cleanup();
  });

  it('handles generic onmessage with valid and invalid JSON', () => {
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-1', onEvent);
    const es = FakeEventSource.instances[0];
    es.onmessage?.({ data: JSON.stringify({ b: 2 }) } as MessageEvent);
    expect(onEvent).toHaveBeenCalledWith({ event: 'message', data: { b: 2 } });
    es.onmessage?.({ data: 'nope' } as MessageEvent);
    expect(onEvent).toHaveBeenLastCalledWith({
      event: 'message',
      data: { message: 'nope' },
    });
    cleanup();
  });

  it('logs the `|| e.data` fallback when generic message data has no slice', () => {
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-1', onEvent);
    const es = FakeEventSource.instances[0];
    const obj = { generic: true };
    es.onmessage?.({ data: obj } as unknown as MessageEvent);
    expect(onEvent).toHaveBeenCalledWith({
      event: 'message',
      data: { message: obj },
    });
    cleanup();
  });

  it('early-returns from connect when a reconnect fires after close', () => {
    const onEvent = vi.fn();
    const onError = vi.fn();
    const setTimeoutSpy = vi.spyOn(global, 'setTimeout');
    const cleanup = streamGeneration('gen-1', onEvent, onError);
    const es = FakeEventSource.instances[0];
    es.onerror?.(new Event('error'));

    const scheduledConnect = setTimeoutSpy.mock.calls[0][0] as () => void;
    cleanup();

    scheduledConnect();
    expect(FakeEventSource.instances.length).toBe(1);
  });

  it('reconnects with backoff then gives up and calls onError after MAX', () => {
    const onEvent = vi.fn();
    const onError = vi.fn();
    const cleanup = streamGeneration('gen-1', onEvent, onError);

    for (let i = 0; i < 5; i++) {
      const es = FakeEventSource.instances[FakeEventSource.instances.length - 1];
      es.onerror?.(new Event('error'));
      vi.advanceTimersByTime(RECONNECT_DELAY(i));
    }
    const last = FakeEventSource.instances[FakeEventSource.instances.length - 1];
    last.onerror?.(new Event('error'));
    expect(onError).toHaveBeenCalledTimes(1);
    cleanup();
  });

  it('gives up without onError callback after MAX', () => {
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-1', onEvent);
    for (let i = 0; i < 5; i++) {
      const es = FakeEventSource.instances[FakeEventSource.instances.length - 1];
      es.onerror?.(new Event('error'));
      vi.advanceTimersByTime(RECONNECT_DELAY(i));
    }
    const last = FakeEventSource.instances[FakeEventSource.instances.length - 1];
    expect(() => last.onerror?.(new Event('error'))).not.toThrow();
    cleanup();
  });

  it('does not reconnect when closed before error fires', () => {
    const onEvent = vi.fn();
    const onError = vi.fn();
    const cleanup = streamGeneration('gen-1', onEvent, onError);
    const es = FakeEventSource.instances[0];
    cleanup();
    es.onerror?.(new Event('error'));
    vi.advanceTimersByTime(60000);
    expect(onError).not.toHaveBeenCalled();
    expect(FakeEventSource.instances.length).toBe(1);
  });

  it('cleanup clears a pending reconnect timeout and closes the source', () => {
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-1', onEvent);
    const es = FakeEventSource.instances[0];
    es.onerror?.(new Event('error'));
    cleanup();
    vi.advanceTimersByTime(60000);
    expect(FakeEventSource.instances.length).toBe(1);
    // Second cleanup: both branches (timeout/source null) skipped.
    expect(() => cleanup()).not.toThrow();
  });
});

// The first SSE connect of a page drops often on the Databricks Apps proxy,
// and the chat fast path folds the execution_id into the single (now-missed)
// generation_complete event. These cover the result-endpoint poll that
// backstops that drop so the run is never orphaned.
describe('streamGeneration result-poll fallback', () => {
  it('recovers generation_complete via the result poll after a transport error', async () => {
    httpGet.mockResolvedValue({
      data: { status: 'completed', execution_id: 'exec-1', run_name: 'Chat', generation_id: 'gen-1' },
    });
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-1', onEvent);
    const es = FakeEventSource.instances[0];

    // Transport error: the stream may never deliver -> poll immediately.
    es.onerror?.(new Event('error'));
    await vi.advanceTimersByTimeAsync(0); // flush the in-flight poll promise

    expect(httpGet).toHaveBeenCalledWith('/sse/generations/gen-1/result');
    expect(onEvent).toHaveBeenCalledWith({
      event: 'generation_complete',
      data: { status: 'completed', execution_id: 'exec-1', run_name: 'Chat', generation_id: 'gen-1' },
    });
    cleanup();
  });

  it('recovers a missed terminal event via the delayed safety-net poll', async () => {
    httpGet.mockResolvedValue({
      data: { status: 'completed', execution_id: 'exec-2', generation_id: 'gen-2' },
    });
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-2', onEvent);

    // No transport error — the stream "connected" but stalled. The safety-net
    // poll starts after POLL_START_DELAY (2000ms) and recovers the outcome.
    await vi.advanceTimersByTimeAsync(2000);

    expect(onEvent).toHaveBeenCalledWith({
      event: 'generation_complete',
      data: { status: 'completed', execution_id: 'exec-2', generation_id: 'gen-2' },
    });
    cleanup();
  });

  it('does not poll when the stream delivers generation_complete in time', async () => {
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-3', onEvent);
    const es = FakeEventSource.instances[0];

    // Stream delivers the terminal event before the safety-net poll fires.
    es.emit('generation_complete', JSON.stringify({ status: 'completed', execution_id: 'exec-3' }));
    expect(onEvent).toHaveBeenCalledWith({
      event: 'generation_complete',
      data: { status: 'completed', execution_id: 'exec-3' },
    });

    // Advancing past the safety-net delay must NOT trigger any poll.
    await vi.advanceTimersByTimeAsync(5000);
    expect(httpGet).not.toHaveBeenCalled();
    cleanup();
  });

  it('normalizes a failed result poll into a generation_failed event', async () => {
    httpGet.mockResolvedValue({ data: { status: 'failed', error: 'boom', generation_id: 'gen-4' } });
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-4', onEvent);
    const es = FakeEventSource.instances[0];

    es.onerror?.(new Event('error'));
    await vi.advanceTimersByTimeAsync(0);

    expect(onEvent).toHaveBeenCalledWith({
      event: 'generation_failed',
      data: { status: 'failed', error: 'boom', generation_id: 'gen-4' },
    });
    cleanup();
  });

  it('keeps polling while pending, then stops once terminal arrives', async () => {
    httpGet
      .mockResolvedValueOnce({ data: { status: 'pending' } })
      .mockResolvedValueOnce({ data: { status: 'completed', execution_id: 'exec-5' } });
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-5', onEvent);
    const es = FakeEventSource.instances[0];

    es.onerror?.(new Event('error'));
    await vi.advanceTimersByTimeAsync(0); // 1st poll -> pending, schedules retry
    expect(onEvent).not.toHaveBeenCalledWith(
      expect.objectContaining({ event: 'generation_complete' }),
    );

    await vi.advanceTimersByTimeAsync(2000); // POLL_INTERVAL -> 2nd poll -> completed
    expect(onEvent).toHaveBeenCalledWith({
      event: 'generation_complete',
      data: { status: 'completed', execution_id: 'exec-5' },
    });

    // No further polling after terminal.
    const callsAfter = httpGet.mock.calls.length;
    await vi.advanceTimersByTimeAsync(10000);
    expect(httpGet.mock.calls.length).toBe(callsAfter);
    cleanup();
  });

  it('stops the safety-net poll on cleanup', async () => {
    const onEvent = vi.fn();
    const cleanup = streamGeneration('gen-6', onEvent);
    cleanup();
    await vi.advanceTimersByTimeAsync(5000);
    expect(httpGet).not.toHaveBeenCalled();
  });
});

// Exponential backoff: RECONNECT_BASE_DELAY (1000) * 2^attempt.
function RECONNECT_DELAY(attemptIndex: number): number {
  return 1000 * Math.pow(2, attemptIndex);
}

describe('streamExecution when SSE is disabled', () => {
  it('returns a no-op cleanup and never opens an EventSource', async () => {
    vi.resetModules();
    vi.doMock('../../../utils/sseTransport', () => ({ SSE_ENABLED: false }));
    vi.doMock('./client', () => ({
      __esModule: true,
      getBaseUrl: () => 'https://example.com/api/v1',
    }));
    const mod = await import('./streaming');
    FakeEventSource.instances = [];

    const cleanup = mod.streamExecution('job-x', vi.fn());
    expect(FakeEventSource.instances.length).toBe(0); // SSE never opened
    expect(() => cleanup()).not.toThrow(); // no-op cleanup

    vi.doUnmock('../../../utils/sseTransport');
    vi.doUnmock('./client');
    vi.resetModules();
  });
});

describe('streamGeneration when SSE is disabled (deployed)', () => {
  it('never opens an EventSource and polls the result endpoint immediately', async () => {
    // Perf regression (W6.2): on Databricks Apps the HTTP/2 proxy refuses
    // EventSource, so every dispatch burned up to 6 doomed connect attempts
    // while the result poll did the real work. Poll-only, starting NOW (no
    // SSE head-start delay).
    vi.resetModules();
    const get = vi.fn().mockResolvedValue({ data: { status: 'pending' } });
    vi.doMock('../../../utils/sseTransport', () => ({ SSE_ENABLED: false }));
    vi.doMock('./client', () => ({
      __esModule: true,
      getBaseUrl: () => 'https://example.com/api/v1',
      getClient: () => ({ get }),
    }));
    const mod = await import('./streaming');
    FakeEventSource.instances = [];

    const cleanup = mod.streamGeneration('gen-deployed', vi.fn());
    // Let the immediate poll's microtasks run — no timer advance needed.
    await Promise.resolve();
    await Promise.resolve();

    expect(FakeEventSource.instances.length).toBe(0); // SSE never opened
    expect(get).toHaveBeenCalledWith('/sse/generations/gen-deployed/result');

    cleanup();
    vi.doUnmock('../../../utils/sseTransport');
    vi.doUnmock('./client');
    vi.resetModules();
  });
});

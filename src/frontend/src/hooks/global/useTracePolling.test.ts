/**
 * Unit tests for useTracePolling — the REST polling fallback for execution
 * state when SSE is unavailable (Databricks Apps).
 *
 * Regression focus: a job whose execution row no longer exists for the current
 * workspace (deleted, or it belongs to a group you no longer have selected)
 * makes /executions/{id} return 404 on every poll. The poller MUST stop after a
 * few consecutive 404s and dispatch a `jobNotFound` event — otherwise it loops
 * 404s against /executions + /traces every 2s forever (the bug this fixes).
 */
import { renderHook } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';

// SSE disabled (Databricks Apps) so jobCreated starts polling immediately,
// without the 4s SSE grace period — keeps the timing in tests simple.
vi.mock('../../utils/sseTransport', () => ({ SSE_ENABLED: false }));

const apiGet = vi.fn();
vi.mock('../../config/api/ApiConfig', () => ({
  apiClient: { get: (...args: unknown[]) => apiGet(...args) },
}));

const runStatusState = {
  sseConnected: false,
  handleSSEUpdate: vi.fn(),
  addTrace: vi.fn(),
};
vi.mock('../../store/runStatus', () => ({
  useRunStatusStore: { getState: () => runStatusState },
}));

const flowState = { currentJobId: null as string | null, crewNodeStates: new Map() };
vi.mock('../../store/flowExecutionStore', () => ({
  useFlowExecutionStore: { getState: () => flowState, setState: vi.fn() },
}));

const taskState = { transition: vi.fn() };
vi.mock('../../store/taskExecutionStore', () => ({
  useTaskExecutionStore: { getState: () => taskState },
}));

import { useTracePolling } from './useTracePolling';

const JOB = 'gone-job-123';

/** Count how many /executions/{id} status probes have been fired so far. */
const execProbeCount = () =>
  apiGet.mock.calls.filter(([url]) => url === `/executions/${JOB}`).length;

beforeEach(() => {
  vi.useFakeTimers();
  apiGet.mockReset();
  flowState.currentJobId = null;
});

afterEach(() => {
  vi.runOnlyPendingTimers();
  vi.useRealTimers();
});

describe('useTracePolling - gone job (404 loop)', () => {
  it('stops polling and dispatches jobNotFound after consecutive 404s', async () => {
    // /executions/{id} 404s; everything else (traces, task-states) resolves empty.
    apiGet.mockImplementation((url: string) => {
      if (url === `/executions/${JOB}`) {
        return Promise.reject({ response: { status: 404 } });
      }
      return Promise.resolve({ data: { traces: [] } });
    });

    const notFound = vi.fn();
    window.addEventListener('jobNotFound', notFound as EventListener);

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));

    // Poll #1 (immediate), then #2 and #3 on the 2s interval. The 3rd consecutive
    // 404 (NOT_FOUND_LIMIT) trips the stop.
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(2000);
    await vi.advanceTimersByTimeAsync(2000);

    expect(notFound).toHaveBeenCalledTimes(1);
    expect((notFound.mock.calls[0][0] as CustomEvent).detail).toEqual({ jobId: JOB });

    // Polling has stopped: probe count is frozen across further interval ticks.
    const frozen = execProbeCount();
    expect(frozen).toBe(3);
    await vi.advanceTimersByTimeAsync(6000);
    expect(execProbeCount()).toBe(frozen);

    window.removeEventListener('jobNotFound', notFound as EventListener);
  });

  it('does NOT stop on a transient (non-404) failure', async () => {
    // A 5xx / network error must NOT be mistaken for a gone job.
    apiGet.mockImplementation((url: string) => {
      if (url === `/executions/${JOB}`) {
        return Promise.reject({ response: { status: 503 } });
      }
      return Promise.resolve({ data: { traces: [] } });
    });

    const notFound = vi.fn();
    window.addEventListener('jobNotFound', notFound as EventListener);

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));

    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(2000);
    await vi.advanceTimersByTimeAsync(2000);
    await vi.advanceTimersByTimeAsync(2000);

    expect(notFound).not.toHaveBeenCalled();
    // Still polling (transient errors keep retrying).
    expect(execProbeCount()).toBeGreaterThanOrEqual(4);

    window.removeEventListener('jobNotFound', notFound as EventListener);
  });

  it('a successful status poll resets the 404 counter (no false stop)', async () => {
    // 404, 404, then a valid status, then 404, 404 — never 3 in a row, so the
    // job is never abandoned.
    const statuses: Array<{ status: number } | { ok: true }> = [
      { status: 404 },
      { status: 404 },
      { ok: true },
      { status: 404 },
      { status: 404 },
    ];
    let i = 0;
    apiGet.mockImplementation((url: string) => {
      if (url === `/executions/${JOB}`) {
        const step = statuses[Math.min(i, statuses.length - 1)];
        i += 1;
        if ('ok' in step) {
          return Promise.resolve({ data: { status: 'running' } });
        }
        return Promise.reject({ response: { status: step.status } });
      }
      return Promise.resolve({ data: { traces: [] } });
    });

    const notFound = vi.fn();
    window.addEventListener('jobNotFound', notFound as EventListener);

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));

    // Exactly 5 polls: #1 immediate + 4 interval ticks, matching the 5 scripted
    // statuses [404, 404, running, 404, 404] — max 2 consecutive 404s, never 3.
    await vi.advanceTimersByTimeAsync(0);
    for (let n = 0; n < 4; n += 1) {
      await vi.advanceTimersByTimeAsync(2000);
    }

    expect(execProbeCount()).toBe(5);
    expect(notFound).not.toHaveBeenCalled();

    window.removeEventListener('jobNotFound', notFound as EventListener);
  });

  it('stops when an external jobNotFound arrives for the ACTIVE job', async () => {
    // Status keeps returning 'running' (not terminal) — only the external
    // jobNotFound (e.g. the ChatMode reconnect backstop) stops the poll.
    apiGet.mockImplementation((url: string) => {
      if (url === `/executions/${JOB}`) return Promise.resolve({ data: { status: 'running' } });
      return Promise.resolve({ data: { traces: [] } });
    });

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(2000);
    const before = execProbeCount();
    expect(before).toBeGreaterThanOrEqual(2);

    window.dispatchEvent(new CustomEvent('jobNotFound', { detail: { jobId: JOB } }));
    await vi.advanceTimersByTimeAsync(6000);

    expect(execProbeCount()).toBe(before); // polling stopped — no further probes
  });

  it('ignores a jobNotFound for a DIFFERENT job (keeps polling the active one)', async () => {
    apiGet.mockImplementation((url: string) => {
      if (url === `/executions/${JOB}`) return Promise.resolve({ data: { status: 'running' } });
      return Promise.resolve({ data: { traces: [] } });
    });

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));
    await vi.advanceTimersByTimeAsync(0);
    const before = execProbeCount();

    window.dispatchEvent(new CustomEvent('jobNotFound', { detail: { jobId: 'a-different-job' } }));
    await vi.advanceTimersByTimeAsync(2000);

    expect(execProbeCount()).toBeGreaterThan(before); // still polling the active job
  });
});

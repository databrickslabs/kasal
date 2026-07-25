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
  addTraces: vi.fn(),
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

describe('useTracePolling - since_id cursor', () => {
  it('advances since_id to the highest seen trace id (regression: offset polls re-shipped the whole trace set)', async () => {
    let traceCall = 0;
    apiGet.mockImplementation((url: string) => {
      if (url === `/executions/${JOB}`) return Promise.resolve({ data: { status: 'running' } });
      if (url === `/traces/job/${JOB}`) {
        traceCall += 1;
        if (traceCall === 1) {
          return Promise.resolve({ data: { traces: [{ id: 5 }, { id: 7 }] } });
        }
        return Promise.resolve({ data: { traces: [] } });
      }
      return Promise.resolve({ data: {} });
    });

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(2000);

    const traceRequests = apiGet.mock.calls
      .filter(([url]) => url === `/traces/job/${JOB}`)
      .map(([, cfg]) => (cfg as { params?: Record<string, number> })?.params);
    expect(traceRequests[0]).toEqual({ limit: 50, since_id: 0 });
    expect(traceRequests[1]).toEqual({ limit: 50, since_id: 7 });
  });
});

describe('useTracePolling - task-states gating by run type', () => {
  const taskStateCount = () =>
    apiGet.mock.calls.filter(([url]) => url === `/traces/job/${JOB}/task-states`).length;

  it('skips the per-tick task-states request for agent (ChatMode) runs', async () => {
    apiGet.mockImplementation((url: string) => {
      if (url === `/executions/${JOB}`) {
        return Promise.resolve({ data: { status: 'running', execution_type: 'agent' } });
      }
      return Promise.resolve({ data: { traces: [] } });
    });

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(2000);
    await vi.advanceTimersByTimeAsync(2000);

    // The first tick may probe before execution_type is known; after that,
    // agent runs must not generate task-states requests (nothing consumes
    // taskExecutionStore for ChatMode).
    expect(taskStateCount()).toBeLessThanOrEqual(1);
  });

  it('keeps polling task-states for crew runs', async () => {
    apiGet.mockImplementation((url: string) => {
      if (url === `/executions/${JOB}`) {
        return Promise.resolve({ data: { status: 'running', execution_type: 'crew' } });
      }
      if (url === `/traces/job/${JOB}/task-states`) {
        return Promise.resolve({ data: {} });
      }
      return Promise.resolve({ data: { traces: [] } });
    });

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(2000);

    expect(taskStateCount()).toBeGreaterThanOrEqual(2);
  });
});

describe('useTracePolling - hidden-tab pacing', () => {
  const setHidden = (hidden: boolean) => {
    Object.defineProperty(document, 'hidden', { value: hidden, configurable: true });
    document.dispatchEvent(new Event('visibilitychange'));
  };

  afterEach(() => {
    Object.defineProperty(document, 'hidden', { value: false, configurable: true });
  });

  it('drops to a slow heartbeat while hidden and resumes immediately on return', async () => {
    apiGet.mockImplementation((url: string) => {
      if (url === `/executions/${JOB}`) return Promise.resolve({ data: { status: 'running' } });
      return Promise.resolve({ data: { traces: [] } });
    });

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));
    await vi.advanceTimersByTimeAsync(0);
    const baseline = execProbeCount();
    expect(baseline).toBe(1);

    // Hide the tab: fast 2s ticks must stop; only the slow heartbeat remains.
    setHidden(true);
    await vi.advanceTimersByTimeAsync(4000);
    expect(execProbeCount()).toBe(baseline); // no fast ticks while hidden
    await vi.advanceTimersByTimeAsync(15000);
    expect(execProbeCount()).toBe(baseline + 1); // one slow heartbeat

    // Back to visible: immediate poll + fast cadence restored.
    setHidden(false);
    await vi.advanceTimersByTimeAsync(0);
    expect(execProbeCount()).toBe(baseline + 2);
    await vi.advanceTimersByTimeAsync(2000);
    expect(execProbeCount()).toBe(baseline + 3);
  });
});

describe('useTracePolling - terminal events are gated on the active job', () => {
  const runningResponse = (url: string) => {
    if (url === `/executions/${JOB}`) return Promise.resolve({ data: { status: 'running' } });
    return Promise.resolve({ data: { traces: [] } });
  };

  it.each(['jobCompleted', 'jobFailed', 'jobStopped'])(
    'ignores %s for a DIFFERENT job (regression: backgrounded run completing froze the foreground poller)',
    async (eventName) => {
      apiGet.mockImplementation(runningResponse);

      renderHook(() => useTracePolling());
      window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));
      await vi.advanceTimersByTimeAsync(0);
      const before = execProbeCount();

      // A different (backgrounded) run reaches a terminal state.
      window.dispatchEvent(new CustomEvent(eventName, { detail: { jobId: 'an-older-run' } }));
      await vi.advanceTimersByTimeAsync(2000);

      // The foreground job's poller must keep going.
      expect(execProbeCount()).toBeGreaterThan(before);
    },
  );

  it.each(['jobCompleted', 'jobFailed', 'jobStopped'])(
    'stops on %s for the ACTIVE job',
    async (eventName) => {
      apiGet.mockImplementation(runningResponse);

      renderHook(() => useTracePolling());
      window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));
      await vi.advanceTimersByTimeAsync(0);
      await vi.advanceTimersByTimeAsync(2000);
      const before = execProbeCount();
      expect(before).toBeGreaterThanOrEqual(2);

      window.dispatchEvent(new CustomEvent(eventName, { detail: { jobId: JOB } }));
      await vi.advanceTimersByTimeAsync(6000);

      expect(execProbeCount()).toBe(before); // polling stopped
    },
  );
});

describe('useTracePolling - HITL pending-approval probe (SSE parity)', () => {
  const hitlProbeCount = () =>
    apiGet.mock.calls.filter(([url]) => url === `/hitl/execution/${JOB}`).length;

  const pendingApproval = (over: Record<string, unknown> = {}) => ({
    id: 42,
    execution_id: JOB,
    status: 'pending',
    is_expired: false,
    gate_config: {
      kind: 'tool_call',
      tool_name: 'GenieTool',
      tool_args: { q: 'select 1' },
      agent_role: 'Analyst',
      task_name: null,
      timeout_seconds: 300,
      timeout_action: 'auto_reject',
      message: "Agent wants to run 'GenieTool' — approval required.",
    },
    ...over,
  });

  it('dispatches ONE hitlRequest with the SSE-shaped flat detail when a pending approval appears', async () => {
    apiGet.mockImplementation((url: string) => {
      if (url === `/executions/${JOB}`) {
        return Promise.resolve({ data: { status: 'waiting_for_approval' } });
      }
      if (url === `/hitl/execution/${JOB}`) {
        return Promise.resolve({
          data: { execution_id: JOB, has_pending_approval: true, pending_approval: pendingApproval() },
        });
      }
      return Promise.resolve({ data: { traces: [] } });
    });

    const hitl = vi.fn();
    window.addEventListener('hitlRequest', hitl as EventListener);

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(2000);
    await vi.advanceTimersByTimeAsync(2000);

    // Deduped by approval id: the gate fires exactly once even though the
    // probe keeps running every tick while WAITING_FOR_APPROVAL.
    expect(hitl).toHaveBeenCalledTimes(1);
    const detail = (hitl.mock.calls[0][0] as CustomEvent).detail;
    // Same flat shape SSEConnectionManager dispatches for the SSE event.
    expect(detail).toMatchObject({
      job_id: JOB,
      approval_id: '42',
      kind: 'tool_call',
      tool_name: 'GenieTool',
      tool_args: { q: 'select 1' },
      agent_role: 'Analyst',
      message: "Agent wants to run 'GenieTool' — approval required.",
    });
    expect(hitlProbeCount()).toBeGreaterThanOrEqual(3); // every tick while waiting

    window.removeEventListener('hitlRequest', hitl as EventListener);
  });

  it('probes only every 2nd tick while the run is plain RUNNING', async () => {
    apiGet.mockImplementation((url: string) => {
      if (url === `/executions/${JOB}`) return Promise.resolve({ data: { status: 'running' } });
      if (url === `/hitl/execution/${JOB}`) {
        return Promise.resolve({ data: { execution_id: JOB, has_pending_approval: false } });
      }
      return Promise.resolve({ data: { traces: [] } });
    });

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));
    // Ticks 1..4: probe fires on even ticks only (2 and 4).
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(2000);
    await vi.advanceTimersByTimeAsync(2000);
    await vi.advanceTimersByTimeAsync(2000);

    expect(execProbeCount()).toBe(4);
    expect(hitlProbeCount()).toBe(2);
  });

  it('ignores an expired pending approval', async () => {
    apiGet.mockImplementation((url: string) => {
      if (url === `/executions/${JOB}`) {
        return Promise.resolve({ data: { status: 'waiting_for_approval' } });
      }
      if (url === `/hitl/execution/${JOB}`) {
        return Promise.resolve({
          data: {
            execution_id: JOB,
            has_pending_approval: true,
            pending_approval: pendingApproval({ is_expired: true }),
          },
        });
      }
      return Promise.resolve({ data: { traces: [] } });
    });

    const hitl = vi.fn();
    window.addEventListener('hitlRequest', hitl as EventListener);

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(2000);

    expect(hitl).not.toHaveBeenCalled();
    window.removeEventListener('hitlRequest', hitl as EventListener);
  });

  it('enriches task_review gates with output_preview from the ui-view approval fetch', async () => {
    const longOutput = 'x'.repeat(900);
    apiGet.mockImplementation((url: string, cfg?: { params?: Record<string, unknown> }) => {
      if (url === `/executions/${JOB}`) {
        return Promise.resolve({ data: { status: 'waiting_for_approval' } });
      }
      if (url === `/hitl/execution/${JOB}`) {
        return Promise.resolve({
          data: {
            execution_id: JOB,
            has_pending_approval: true,
            pending_approval: pendingApproval({
              id: 7,
              gate_config: {
                kind: 'task_review',
                task_name: 'write_report',
                message: "Task 'write_report' finished — review the output.",
              },
              has_previous_crew_output: true,
            }),
          },
        });
      }
      if (url === `/hitl/approvals/7`) {
        expect(cfg?.params).toEqual({ view: 'ui' });
        return Promise.resolve({ data: { id: 7, previous_crew_output: longOutput } });
      }
      return Promise.resolve({ data: { traces: [] } });
    });

    const hitl = vi.fn();
    window.addEventListener('hitlRequest', hitl as EventListener);

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(2000);

    expect(hitl).toHaveBeenCalledTimes(1);
    const detail = (hitl.mock.calls[0][0] as CustomEvent).detail;
    expect(detail).toMatchObject({
      job_id: JOB,
      approval_id: '7',
      kind: 'task_review',
      task_name: 'write_report',
    });
    expect(detail.output_preview).toBe('x'.repeat(500)); // sliced to the SSE preview length

    window.removeEventListener('hitlRequest', hitl as EventListener);
  });

  it('a probe failure never breaks the main poll loop', async () => {
    apiGet.mockImplementation((url: string) => {
      if (url === `/executions/${JOB}`) return Promise.resolve({ data: { status: 'running' } });
      if (url === `/hitl/execution/${JOB}`) return Promise.reject(new Error('boom'));
      return Promise.resolve({ data: { traces: [] } });
    });

    renderHook(() => useTracePolling());
    window.dispatchEvent(new CustomEvent('jobCreated', { detail: { jobId: JOB } }));
    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(2000);
    await vi.advanceTimersByTimeAsync(2000);
    await vi.advanceTimersByTimeAsync(2000);

    expect(execProbeCount()).toBe(4); // status polling unaffected
  });
});

/**
 * The reprocess path is throttled.
 *
 * A live run streams trace events in bursts of tens per second. Each event lands
 * in the runStatus store; without throttling, every one triggered a full
 * `processTraces` pass over the whole accumulated array plus a rebuild of the
 * expanded sets, and the tab froze once the array grew large (reloading — which
 * drops the in-memory array — was the only fix). These tests pin the coalescing
 * and the additive (non-resetting) expansion.
 */

import { renderHook, act } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';

// fetchTraceData runs on activation; stub the network so it resolves to "no
// traces yet" and leaves the store as our test drives it.
vi.mock('../../api/execution/TraceService', () => ({
  default: {
    checkRunExists: vi.fn().mockResolvedValue(true),
    getRunDetails: vi.fn().mockResolvedValue({ job_id: 'job-1' }),
    getTraces: vi.fn().mockResolvedValue([]),
    getTraceById: vi.fn(),
    getTaskDetails: vi.fn(),
  },
}));

import { useTraceData } from './useTraceData';
import { useRunStatusStore, Trace } from '../../store/runStatus';

const JOB = 'job-1';

let n = 0;
const makeTrace = (over: Partial<Trace> = {}): Trace => {
  n += 1;
  return {
    id: n,
    created_at: new Date(Date.now() + n).toISOString(),
    event_type: 'agent_execution',
    event_source: `Agent ${n}`,
    event_context: 'do the thing',
    ...over,
  } as Trace;
};

beforeEach(() => {
  n = 0;
  useRunStatusStore.getState().clearTracesForJob(JOB);
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe('useTraceData — throttled reprocessing', () => {
  it('defers a burst of updates and flushes the freshest data after the window', async () => {
    const { result } = renderHook(() =>
      useTraceData({ runId: 'run-1', jobId: JOB, runStatus: 'running', isActive: true })
    );

    // Seed one trace and flush the throttle window so it renders.
    act(() => {
      useRunStatusStore.getState().addTraces(JOB, [makeTrace()]);
    });
    await act(async () => {
      vi.advanceTimersByTime(500);
      await Promise.resolve();
    });
    expect(result.current.processedTraces?.agents.length).toBe(1);

    // Fire a burst of updates inside a single throttle window (no timer advance).
    act(() => {
      for (let i = 0; i < 30; i += 1) {
        useRunStatusStore.getState().addTraces(JOB, [makeTrace()]);
      }
    });

    // The burst is NOT reflected synchronously — it was deferred to a trailing
    // pass rather than reprocessed 30 times.
    expect(result.current.processedTraces?.agents.length).toBe(1);

    // Advance past the throttle window; one trailing pass flushes everything.
    await act(async () => {
      vi.advanceTimersByTime(500);
      await Promise.resolve();
    });

    expect(result.current.processedTraces?.agents.length).toBe(31);
  });

  it('keeps a collapsed agent collapsed while adding newly-arrived agents', async () => {
    const { result } = renderHook(() =>
      useTraceData({ runId: 'run-1', jobId: JOB, runStatus: 'running', isActive: true })
    );

    act(() => {
      useRunStatusStore.getState().addTraces(JOB, [makeTrace()]);
    });
    await act(async () => {
      vi.advanceTimersByTime(500);
      await Promise.resolve();
    });

    // Initial expand-all opened agent 0.
    expect(result.current.expandedAgents.has(0)).toBe(true);

    // User collapses it.
    act(() => { result.current.toggleAgent(0); });
    expect(result.current.expandedAgents.has(0)).toBe(false);

    // A trace for a brand-new agent arrives.
    act(() => {
      useRunStatusStore.getState().addTraces(JOB, [makeTrace()]);
    });
    await act(async () => {
      vi.advanceTimersByTime(500);
      await Promise.resolve();
    });

    // The new agent is auto-expanded (additive)...
    expect(result.current.expandedAgents.has(1)).toBe(true);
    // ...but the one the user collapsed stays collapsed — no silent re-open.
    expect(result.current.expandedAgents.has(0)).toBe(false);
  });
});

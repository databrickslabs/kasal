/**
 * Trace polling fallback for when SSE is unavailable (e.g. Databricks Apps HTTP/2 proxy).
 *
 * When SSE fails, this hook polls lightweight REST endpoints for execution
 * state updates and dispatches the same window events that SSE would, so all
 * downstream consumers (useExecutionMonitoring, flowExecutionStore,
 * taskExecutionStore) work identically regardless of the transport.
 *
 * Uses server-computed endpoints to minimize payload size:
 *   - GET /traces/job/{id}/crew-node-states  (~200 bytes per crew)
 *   - GET /traces/job/{id}/task-states       (~200 bytes per task)
 *   - GET /executions/{id}                   (for job lifecycle status)
 *   - GET /traces/job/{id}?since_id=N&limit=50 (only NEW traces via id cursor
 *     — the server filters id > N, so a poll never re-ships prior rows)
 *
 * ACTIVATION STRATEGY:
 *   Polling always starts on jobCreated (after a short delay to give SSE a chance).
 *   If SSE delivers a real message for the active job, polling is stopped.
 *   This avoids relying on the flapping sseConnected state.
 */

import { useEffect, useRef, useCallback } from 'react';
import { apiClient } from '../../config/api/ApiConfig';
import { SSE_ENABLED } from '../../utils/sseTransport';
import { useRunStatusStore } from '../../store/runStatus';
import { useFlowExecutionStore } from '../../store/flowExecutionStore';
import { useTaskExecutionStore } from '../../store/taskExecutionStore';

const POLL_INTERVAL_MS = 2000;
/**
 * Cadence while the tab is hidden. Background tabs used to keep polling at the
 * full 2s × 3-requests rate for the whole run — the single biggest source of
 * sustained deployed backend load. A hidden tab only needs a slow heartbeat;
 * returning to the tab polls immediately and restores the fast cadence.
 */
const HIDDEN_POLL_INTERVAL_MS = 15000;
/** Delay before starting polling after jobCreated — gives SSE a chance to work */
const SSE_GRACE_PERIOD_MS = 4000;
/**
 * Consecutive 404s on the /executions/{id} status probe before we conclude the
 * job is gone (deleted, or it belongs to a workspace you no longer have
 * selected) and stop polling. Requiring several in a row — not just one — keeps
 * a brief just-created/not-yet-persisted race from killing a real run.
 */
const NOT_FOUND_LIMIT = 3;

/**
 * Hook that polls for execution state when SSE is unavailable.
 * Always activates on jobCreated; stops if SSE proves it's working.
 */
export const useTracePolling = () => {
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const graceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const activeJobIdRef = useRef<string | null>(null);
  const isPollingRef = useRef(false);
  /** Highest trace id seen — the server-side `since_id` cursor. */
  const lastTraceIdRef = useRef<number>(0);
  const lastStatusRef = useRef<string | null>(null);
  /** Consecutive 404s on the status probe — a gone job (deleted / different group). */
  const notFoundCountRef = useRef(0);
  /** Set to true if SSE delivers a real trace/execution_update for the active job */
  const sseProvenWorkingRef = useRef(false);

  const pollStates = useCallback(async () => {
    const jobId = activeJobIdRef.current;
    if (!jobId || isPollingRef.current) return;

    isPollingRef.current = true;
    try {
      const isFlow = useFlowExecutionStore.getState().currentJobId === jobId;

      // Build requests — always poll status + new traces; add crew-node-states for flows.
      // The status probe is a RAW /executions/{id}: a 404 must surface as a rejected
      // result so we can act on it (a job that 404s for several consecutive polls is
      // gone — see NOT_FOUND_LIMIT below — and we stop instead of looping 404s
      // forever). The previous /executions/history?job_id={id} fallback was dead code:
      // /executions/history is shadowed by the /executions/{execution_id} route, so it
      // resolved as execution_id="history" and just 404'd, adding noise without ever
      // matching the job.
      const requests: Promise<any>[] = [
        apiClient.get(`/executions/${jobId}`),
        apiClient.get(`/traces/job/${jobId}`, {
          params: { limit: 50, since_id: lastTraceIdRef.current }
        }),
      ];

      if (isFlow) {
        requests.push(apiClient.get(`/traces/job/${jobId}/crew-node-states`));
      }

      console.log(`[TracePolling] Polling | job=${jobId} | isFlow=${isFlow} | since_id=${lastTraceIdRef.current}`);
      const results = await Promise.allSettled(requests);

      // --- 1. Process execution status ---
      if (results[0].status === 'fulfilled') {
        notFoundCountRef.current = 0;
        const execData = results[0].value.data;
        if (execData?.status) {
          const status = execData.status.toLowerCase();

          if (status !== lastStatusRef.current) {
            lastStatusRef.current = status;
            console.log(`[TracePolling] Status change: ${status} | job=${jobId}`);

            useRunStatusStore.getState().handleSSEUpdate({
              job_id: jobId,
              status: execData.status,
              run_name: execData.run_name,
              error: execData.error,
              result: execData.result,
              created_at: execData.created_at,
              updated_at: execData.updated_at,
              completed_at: execData.completed_at,
              group_id: execData.group_id,
              execution_type: execData.execution_type,
            });
          }

          // If job finished, do a final poll and stop
          if (['completed', 'failed', 'stopped', 'cancelled'].includes(status)) {
            console.log(`[TracePolling] Job ${jobId} finished (${status}), final trace fetch`);
            try {
              const finalResp = await apiClient.get(`/traces/job/${jobId}`, {
                params: { limit: 500, since_id: lastTraceIdRef.current }
              });
              const finalTraces = finalResp.data?.traces;
              if (Array.isArray(finalTraces) && finalTraces.length > 0) {
                console.log(`[TracePolling] Final fetch: ${finalTraces.length} traces`);
                for (const trace of finalTraces) {
                  window.dispatchEvent(new CustomEvent('traceUpdate', {
                    detail: { jobId, trace }
                  }));
                  useRunStatusStore.getState().addTrace(jobId, trace);
                  if (typeof trace?.id === 'number' && trace.id > lastTraceIdRef.current) {
                    lastTraceIdRef.current = trace.id;
                  }
                }
              }
            } catch { /* non-fatal */ }

            // Stop polling inline
            if (intervalRef.current) {
              clearInterval(intervalRef.current);
              intervalRef.current = null;
            }
            activeJobIdRef.current = null;
            lastTraceIdRef.current = 0;
            lastStatusRef.current = null;
            console.log(`[TracePolling] Stopped after job finished`);
            return;
          }
        }
      } else {
        // The status probe was rejected. A 404 (NotFoundError) means the row no
        // longer exists for this workspace — deleted, or it belongs to a group you
        // no longer have selected. Anything else (offline, 5xx, timeout) is
        // transient. Stop only after several CONSECUTIVE 404s, so a momentary
        // just-created/not-yet-persisted race can't kill a real run; otherwise the
        // poller would hammer /executions + /traces every 2s with 404s forever (and
        // a page refresh would re-detect the dead job and resume the storm).
        const reason = results[0].status === 'rejected'
          ? (results[0].reason as { response?: { status?: number }; message?: string })
          : undefined;
        const httpStatus = reason?.response?.status;
        if (httpStatus === 404) {
          notFoundCountRef.current += 1;
          console.log(`[TracePolling] Status 404 for job ${jobId} (${notFoundCountRef.current}/${NOT_FOUND_LIMIT})`);
          if (notFoundCountRef.current >= NOT_FOUND_LIMIT) {
            console.warn(`[TracePolling] Job ${jobId} not found after ${NOT_FOUND_LIMIT} polls — stopping and abandoning`);
            if (intervalRef.current) {
              clearInterval(intervalRef.current);
              intervalRef.current = null;
            }
            activeJobIdRef.current = null;
            lastTraceIdRef.current = 0;
            lastStatusRef.current = null;
            notFoundCountRef.current = 0;
            // Let ChatMode (and any other consumer) drop the running banner + the
            // durable IndexedDB reconnect marker so a refresh / session-switch
            // can't resurrect this dead job.
            window.dispatchEvent(new CustomEvent('jobNotFound', { detail: { jobId } }));
            return;
          }
        } else {
          // Transient failure — don't count it toward "gone".
          notFoundCountRef.current = 0;
          console.log(`[TracePolling] Execution status poll failed (transient):`, reason?.message || httpStatus || 'unknown');
        }
      }

      // --- 2. Process new traces (incremental via since_id cursor) ---
      if (results[1].status === 'fulfilled') {
        const traces = results[1].value.data?.traces;
        if (Array.isArray(traces) && traces.length > 0) {
          console.log(`[TracePolling] ${traces.length} new traces for job ${jobId}`);

          for (const trace of traces) {
            window.dispatchEvent(new CustomEvent('traceUpdate', {
              detail: { jobId, trace }
            }));
            useRunStatusStore.getState().addTrace(jobId, trace);
            if (typeof trace?.id === 'number' && trace.id > lastTraceIdRef.current) {
              lastTraceIdRef.current = trace.id;
            }
          }
        }
      }

      // --- 3. Process crew node states (flow monitoring) ---
      if (isFlow && results.length > 2 && results[2].status === 'fulfilled') {
        const crewStates = results[2].value.data;
        if (crewStates && typeof crewStates === 'object' && Object.keys(crewStates).length > 0) {
          const store = useFlowExecutionStore.getState();
          const newCrewNodeStates = new Map(store.crewNodeStates);
          let changed = false;

          for (const [crewName, state] of Object.entries(crewStates)) {
            const serverState = state as any;
            const current = newCrewNodeStates.get(crewName);

            if (!current || current.status !== serverState.status) {
              newCrewNodeStates.set(crewName, {
                status: serverState.status,
                started_at: serverState.started_at,
                completed_at: serverState.completed_at,
                failed_at: serverState.failed_at,
                task_count: serverState.task_count,
                completed_count: serverState.completed_count,
              });
              changed = true;
            }
          }

          if (changed) {
            useFlowExecutionStore.setState({ crewNodeStates: newCrewNodeStates });
            console.log('[TracePolling] Updated crew node states:', Object.keys(crewStates));
          }
        }
      }

      // --- 4. Update task states (for TaskNode visual indicators) ---
      if (!isFlow) {
        try {
          const taskResp = await apiClient.get(`/traces/job/${jobId}/task-states`);
          const taskStates = taskResp.data;
          if (taskStates && typeof taskStates === 'object' && Object.keys(taskStates).length > 0) {
            const store = useTaskExecutionStore.getState();
            for (const [taskId, state] of Object.entries(taskStates)) {
              const serverState = state as any;
              store.transition(taskId, serverState.status, {
                task_name: serverState.task_name || '',
                started_at: serverState.started_at,
                completed_at: serverState.completed_at,
                failed_at: serverState.failed_at,
              });
            }
            console.log('[TracePolling] Updated task states:', Object.keys(taskStates).length, 'tasks');
          }
        } catch { /* non-fatal */ }
      }
    } catch (error) {
      console.log('[TracePolling] Poll error (non-fatal):', error);
    } finally {
      isPollingRef.current = false;
    }
  }, []);

  /**
   * (Re)arm the poll interval at the cadence appropriate for tab visibility:
   * fast while the user is watching, slow heartbeat while the tab is hidden.
   */
  const armInterval = useCallback(() => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
    }
    const cadence = typeof document !== 'undefined' && document.hidden
      ? HIDDEN_POLL_INTERVAL_MS
      : POLL_INTERVAL_MS;
    intervalRef.current = setInterval(pollStates, cadence);
  }, [pollStates]);

  const startPolling = useCallback((jobId: string) => {
    activeJobIdRef.current = jobId;
    lastTraceIdRef.current = 0;
    lastStatusRef.current = null;
    notFoundCountRef.current = 0;
    sseProvenWorkingRef.current = false;
    console.log(`[TracePolling] ▶ Starting polling for job ${jobId}`);

    // Poll immediately, then at intervals
    pollStates();
    armInterval();
  }, [pollStates, armInterval]);

  const stopPolling = useCallback(() => {
    if (intervalRef.current) {
      console.log('[TracePolling] ■ Stopping polling');
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
    if (graceTimerRef.current) {
      clearTimeout(graceTimerRef.current);
      graceTimerRef.current = null;
    }
    activeJobIdRef.current = null;
    lastTraceIdRef.current = 0;
    lastStatusRef.current = null;
    notFoundCountRef.current = 0;
  }, []);

  // Listen for job lifecycle events
  useEffect(() => {
    const handleJobCreated = (event: CustomEvent) => {
      const { jobId } = event.detail || {};
      if (!jobId) return;

      console.log(`[TracePolling] jobCreated received | job=${jobId} | sseConnected=${useRunStatusStore.getState().sseConnected}`);

      // SSE is disabled (Databricks Apps) — it's the primary transport now, so
      // poll immediately instead of waiting out the SSE grace period.
      if (!SSE_ENABLED) {
        activeJobIdRef.current = jobId;
        startPolling(jobId);
        return;
      }

      // Always schedule polling after a grace period.
      // If SSE proves it works (delivers a real message), we cancel.
      sseProvenWorkingRef.current = false;
      activeJobIdRef.current = jobId;

      if (graceTimerRef.current) {
        clearTimeout(graceTimerRef.current);
      }

      graceTimerRef.current = setTimeout(() => {
        graceTimerRef.current = null;
        if (sseProvenWorkingRef.current) {
          console.log(`[TracePolling] SSE delivered data during grace period, skipping polling`);
          return;
        }
        console.log(`[TracePolling] SSE did NOT deliver data in ${SSE_GRACE_PERIOD_MS}ms, activating polling fallback`);
        startPolling(jobId);
      }, SSE_GRACE_PERIOD_MS);
    };

    // If SSE delivers a real trace for the active job, it's working — stop polling
    const handleSSETrace = (event: CustomEvent) => {
      const detail = event.detail;
      if (!detail?.trace || !activeJobIdRef.current) return;

      // Only count it as "SSE working" if the trace came from SSE, not from our own polling.
      // We detect this by checking if polling is currently active — if it is, the trace
      // might have been dispatched by us. But if polling hasn't started yet (grace period),
      // then this trace definitely came from SSE.
      if (!intervalRef.current && detail.jobId === activeJobIdRef.current) {
        console.log(`[TracePolling] SSE delivered trace during grace period — SSE is working`);
        sseProvenWorkingRef.current = true;
      }
    };

    // Terminal events must only stop the poller when they are for OUR job.
    // Multiple runs can be live at once (ChatMode backgrounds older runs), so
    // an unGated stop meant any run completing anywhere froze the foreground
    // run's traces until the 10s reconciliation loop. Mirror the
    // handleJobNotFound gate: act only on the active job.
    const makeTerminalHandler = (kind: string) => (event: CustomEvent) => {
      const { jobId } = event.detail || {};
      if (jobId && jobId === activeJobIdRef.current) {
        console.log(`[TracePolling] ${kind} received for active job — stopping`);
        stopPolling();
      }
    };
    const handleJobCompleted = makeTerminalHandler('jobCompleted');
    const handleJobFailed = makeTerminalHandler('jobFailed');
    const handleJobStopped = makeTerminalHandler('jobStopped');
    // Another consumer (the ChatMode reconnect backstop) proved this job is gone
    // before our grace timer even fired. Stop for it specifically — gated on the
    // active job so a jobNotFound for a different job can't kill a live poll, and
    // so our OWN dispatch (which clears activeJobIdRef first) is a harmless no-op.
    const handleJobNotFound = (event: CustomEvent) => {
      const { jobId } = event.detail || {};
      if (jobId && jobId === activeJobIdRef.current) {
        console.log(`[TracePolling] jobNotFound received for active job — stopping`);
        stopPolling();
      }
    };

    // Re-pace polling when tab visibility flips: hidden → slow heartbeat,
    // visible → immediate poll + fast cadence. Only acts while a poll is live.
    const handleVisibilityChange = () => {
      if (!activeJobIdRef.current || !intervalRef.current) return;
      armInterval();
      if (!document.hidden) {
        console.log('[TracePolling] Tab visible again — polling immediately');
        pollStates();
      }
    };

    window.addEventListener('jobCreated', handleJobCreated as EventListener);
    window.addEventListener('traceUpdate', handleSSETrace as EventListener);
    window.addEventListener('jobCompleted', handleJobCompleted as EventListener);
    window.addEventListener('jobFailed', handleJobFailed as EventListener);
    window.addEventListener('jobStopped', handleJobStopped as EventListener);
    window.addEventListener('jobNotFound', handleJobNotFound as EventListener);
    document.addEventListener('visibilitychange', handleVisibilityChange);

    console.log('[TracePolling] Hook mounted, event listeners registered');

    return () => {
      window.removeEventListener('jobCreated', handleJobCreated as EventListener);
      window.removeEventListener('traceUpdate', handleSSETrace as EventListener);
      window.removeEventListener('jobCompleted', handleJobCompleted as EventListener);
      window.removeEventListener('jobFailed', handleJobFailed as EventListener);
      window.removeEventListener('jobStopped', handleJobStopped as EventListener);
      window.removeEventListener('jobNotFound', handleJobNotFound as EventListener);
      document.removeEventListener('visibilitychange', handleVisibilityChange);
      stopPolling();
      console.log('[TracePolling] Hook unmounted');
    };
  }, [startPolling, stopPolling, armInterval, pollStates]);

  return { startPolling, stopPolling };
};

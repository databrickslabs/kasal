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
 *   - GET /traces/job/{id}?offset=N&limit=50 (only new traces, for chat panel)
 *
 * ACTIVATION STRATEGY:
 *   Polling always starts on jobCreated (after a short delay to give SSE a chance).
 *   If SSE delivers a real message for the active job, polling is stopped.
 *   This avoids relying on the flapping sseConnected state.
 */

import { useEffect, useRef, useCallback } from 'react';
import { apiClient } from '../../config/api/ApiConfig';
import { useRunStatusStore } from '../../store/runStatus';
import { useFlowExecutionStore } from '../../store/flowExecutionStore';
import { useTaskExecutionStore } from '../../store/taskExecutionStore';

const POLL_INTERVAL_MS = 2000;
/** Delay before starting polling after jobCreated — gives SSE a chance to work */
const SSE_GRACE_PERIOD_MS = 4000;

/**
 * Hook that polls for execution state when SSE is unavailable.
 * Always activates on jobCreated; stops if SSE proves it's working.
 */
export const useTracePolling = () => {
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const graceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const activeJobIdRef = useRef<string | null>(null);
  const isPollingRef = useRef(false);
  const seenTraceCountRef = useRef<number>(0);
  const lastStatusRef = useRef<string | null>(null);
  /** Set to true if SSE delivers a real trace/execution_update for the active job */
  const sseProvenWorkingRef = useRef(false);

  const pollStates = useCallback(async () => {
    const jobId = activeJobIdRef.current;
    if (!jobId || isPollingRef.current) return;

    isPollingRef.current = true;
    try {
      const isFlow = useFlowExecutionStore.getState().currentJobId === jobId;

      // Build requests — always poll status + new traces; add crew-node-states for flows
      const requests: Promise<any>[] = [
        apiClient.get(`/executions/${jobId}`),
        apiClient.get(`/traces/job/${jobId}`, {
          params: { limit: 50, offset: seenTraceCountRef.current }
        }),
      ];

      if (isFlow) {
        requests.push(apiClient.get(`/traces/job/${jobId}/crew-node-states`));
      }

      console.log(`[TracePolling] Polling | job=${jobId} | isFlow=${isFlow} | offset=${seenTraceCountRef.current}`);
      const results = await Promise.allSettled(requests);

      // --- 1. Process execution status ---
      if (results[0].status === 'fulfilled') {
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
                params: { limit: 500, offset: seenTraceCountRef.current }
              });
              const finalTraces = finalResp.data?.traces;
              if (Array.isArray(finalTraces) && finalTraces.length > 0) {
                console.log(`[TracePolling] Final fetch: ${finalTraces.length} traces`);
                for (const trace of finalTraces) {
                  window.dispatchEvent(new CustomEvent('traceUpdate', {
                    detail: { jobId, trace }
                  }));
                  useRunStatusStore.getState().addTrace(jobId, trace);
                }
                seenTraceCountRef.current += finalTraces.length;
              }
            } catch { /* non-fatal */ }

            // Stop polling inline
            if (intervalRef.current) {
              clearInterval(intervalRef.current);
              intervalRef.current = null;
            }
            activeJobIdRef.current = null;
            seenTraceCountRef.current = 0;
            lastStatusRef.current = null;
            console.log(`[TracePolling] Stopped after job finished`);
            return;
          }
        }
      } else {
        console.log(`[TracePolling] Execution status poll failed:`, results[0].status === 'rejected' ? results[0].reason?.message : 'unknown');
      }

      // --- 2. Process new traces (incremental via offset) ---
      if (results[1].status === 'fulfilled') {
        const traces = results[1].value.data?.traces;
        if (Array.isArray(traces) && traces.length > 0) {
          console.log(`[TracePolling] ${traces.length} new traces for job ${jobId}`);
          seenTraceCountRef.current += traces.length;

          for (const trace of traces) {
            window.dispatchEvent(new CustomEvent('traceUpdate', {
              detail: { jobId, trace }
            }));
            useRunStatusStore.getState().addTrace(jobId, trace);
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

  const startPolling = useCallback((jobId: string) => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
    }

    activeJobIdRef.current = jobId;
    seenTraceCountRef.current = 0;
    lastStatusRef.current = null;
    sseProvenWorkingRef.current = false;
    console.log(`[TracePolling] ▶ Starting polling for job ${jobId}`);

    // Poll immediately, then at intervals
    pollStates();
    intervalRef.current = setInterval(pollStates, POLL_INTERVAL_MS);
  }, [pollStates]);

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
    seenTraceCountRef.current = 0;
    lastStatusRef.current = null;
  }, []);

  // Listen for job lifecycle events
  useEffect(() => {
    const handleJobCreated = (event: CustomEvent) => {
      const { jobId } = event.detail || {};
      if (!jobId) return;

      console.log(`[TracePolling] jobCreated received | job=${jobId} | sseConnected=${useRunStatusStore.getState().sseConnected}`);

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

    const handleJobCompleted = () => {
      console.log(`[TracePolling] jobCompleted received`);
      stopPolling();
    };
    const handleJobFailed = () => {
      console.log(`[TracePolling] jobFailed received`);
      stopPolling();
    };
    const handleJobStopped = () => {
      console.log(`[TracePolling] jobStopped received`);
      stopPolling();
    };

    window.addEventListener('jobCreated', handleJobCreated as EventListener);
    window.addEventListener('traceUpdate', handleSSETrace as EventListener);
    window.addEventListener('jobCompleted', handleJobCompleted as EventListener);
    window.addEventListener('jobFailed', handleJobFailed as EventListener);
    window.addEventListener('jobStopped', handleJobStopped as EventListener);

    console.log('[TracePolling] Hook mounted, event listeners registered');

    return () => {
      window.removeEventListener('jobCreated', handleJobCreated as EventListener);
      window.removeEventListener('traceUpdate', handleSSETrace as EventListener);
      window.removeEventListener('jobCompleted', handleJobCompleted as EventListener);
      window.removeEventListener('jobFailed', handleJobFailed as EventListener);
      window.removeEventListener('jobStopped', handleJobStopped as EventListener);
      stopPolling();
      console.log('[TracePolling] Hook unmounted');
    };
  }, [startPolling, stopPolling]);

  return { startPolling, stopPolling };
};

import { useState, useEffect, useCallback, useRef } from 'react';
import { ChatMessage } from '../types';

import { runService } from '../../../api/ExecutionHistoryService';
import { Run } from '../../../types/run';
import { useTaskExecutionStore } from '../../../store/taskExecutionStore';
import { useChatMessagesStore } from '../../../store/chatMessagesStore';
import { extractTaskId, extractTaskName, mapEventToStatus } from '../../../utils/taskIdUtils';

// Module-level storage for execution state per session
// This persists execution state when switching tabs
interface SessionExecutionState {
  executingJobId: string | null;
  lastExecutionJobId: string | null;
  executionStartTime: Date | null;
  processedTraceIds: Set<string>;
}

const sessionExecutionStates = new Map<string, SessionExecutionState>();

// Helper function to clear execution state for a specific jobId across all sessions
// This is called when a job completes to ensure no session is left blocked
const clearExecutionStateForJob = (jobId: string) => {
  sessionExecutionStates.forEach((_state, sessionId) => {
    if (_state.executingJobId === jobId) {
      sessionExecutionStates.delete(sessionId);
    }
  });
};

export const useExecutionMonitoring = (
  sessionId: string,
  saveMessageToBackend: (message: ChatMessage) => Promise<void>,
  _setMessages: React.Dispatch<React.SetStateAction<ChatMessage[]>>
) => {
  const [executingJobId, setExecutingJobId] = useState<string | null>(null);
  const [lastExecutionJobId, setLastExecutionJobId] = useState<string | null>(null);
  const [processedTraceIds, setProcessedTraceIds] = useState<Set<string>>(new Set());
  const [executionStartTime, setExecutionStartTime] = useState<Date | null>(null);

  // Track the previous sessionId to detect tab switches
  const prevSessionIdRef = useRef<string>(sessionId);

  // Track if this session is expecting a job to start (prevents other tabs from claiming the job)
  const pendingExecutionRef = useRef<boolean>(false);

  // Refs to access current values without adding them as dependencies
  const executingJobIdRef = useRef<string | null>(executingJobId);
  const lastExecutionJobIdRef = useRef<string | null>(lastExecutionJobId);
  const executionStartTimeRef = useRef<Date | null>(executionStartTime);
  const processedTraceIdsRef = useRef<Set<string>>(processedTraceIds);

  // Keep refs in sync with state
  useEffect(() => {
    executingJobIdRef.current = executingJobId;
  }, [executingJobId]);

  useEffect(() => {
    lastExecutionJobIdRef.current = lastExecutionJobId;
  }, [lastExecutionJobId]);

  useEffect(() => {
    executionStartTimeRef.current = executionStartTime;
  }, [executionStartTime]);

  useEffect(() => {
    processedTraceIdsRef.current = processedTraceIds;
  }, [processedTraceIds]);

  // Save and restore execution state when sessionId changes (tab switch)
  // This ensures each tab maintains its own execution state
  useEffect(() => {
    if (prevSessionIdRef.current !== sessionId) {
      const prevSessionId = prevSessionIdRef.current;

      // Save current execution state for the previous session
      if (prevSessionId && (executingJobIdRef.current || lastExecutionJobIdRef.current)) {
        sessionExecutionStates.set(prevSessionId, {
          executingJobId: executingJobIdRef.current,
          lastExecutionJobId: lastExecutionJobIdRef.current,
          executionStartTime: executionStartTimeRef.current,
          processedTraceIds: new Set(processedTraceIdsRef.current),
        });
      }

      // Restore execution state for the new session (if any)
      const savedState = sessionExecutionStates.get(sessionId);
      if (savedState && savedState.executingJobId) {
        // Verify the job is still running before restoring blocked state
        runService.getRuns(100).then(runs => {
          const job = runs.runs.find((r: Run) => r.job_id === savedState.executingJobId);
          const isStillRunning = job && job.status?.toLowerCase() === 'running';

          if (isStillRunning) {
            setExecutingJobId(savedState.executingJobId);
            setLastExecutionJobId(savedState.lastExecutionJobId);
            setExecutionStartTime(savedState.executionStartTime);
            setProcessedTraceIds(savedState.processedTraceIds);
          } else {
            // Job completed while we were away, clear the saved state
            sessionExecutionStates.delete(sessionId);
            setExecutingJobId(null);
            setLastExecutionJobId(savedState.lastExecutionJobId);
            setExecutionStartTime(null);
            setProcessedTraceIds(new Set());
          }
        }).catch(error => {
          console.error('[ExecutionMonitoring] Error checking job status:', error);
          // On error, restore state anyway to be safe (can manually refresh)
          setExecutingJobId(savedState.executingJobId);
          setLastExecutionJobId(savedState.lastExecutionJobId);
          setExecutionStartTime(savedState.executionStartTime);
          setProcessedTraceIds(savedState.processedTraceIds);
        });
      } else if (savedState) {
        // Has saved state but no executingJobId, restore other state
        setExecutingJobId(null);
        setLastExecutionJobId(savedState.lastExecutionJobId);
        setExecutionStartTime(null);
        setProcessedTraceIds(savedState.processedTraceIds);
      } else {
        // No saved state, start fresh for this session
        setExecutingJobId(null);
        setLastExecutionJobId(null);
        setExecutionStartTime(null);
        setProcessedTraceIds(new Set());
      }

      prevSessionIdRef.current = sessionId;
    }
  }, [sessionId]);

  // Get Zustand store methods
  const { addMessage } = useChatMessagesStore();

  // Listen for execution events
  useEffect(() => {
    const handleJobCreated = (event: CustomEvent) => {
      const { jobId, jobName, planningEnabled } = event.detail;

      // Check if this session initiated the execution via markPendingExecution
      const isPendingForThisSession = pendingExecutionRef.current;

      // Clear the pending flag if it was set
      if (isPendingForThisSession) {
        pendingExecutionRef.current = false;
      }

      // Update refs IMMEDIATELY (before React's async state update)
      executingJobIdRef.current = jobId;
      lastExecutionJobIdRef.current = jobId;
      processedTraceIdsRef.current = new Set();
      executionStartTimeRef.current = new Date();

      // Also update state for UI re-renders
      setExecutingJobId(jobId);
      setLastExecutionJobId(jobId);
      setProcessedTraceIds(new Set());
      setExecutionStartTime(new Date());

      // Clear previous task states so the precedence guard doesn't block
      // 'running' updates for tasks that were 'completed' in a previous run.
      useTaskExecutionStore.getState().clearTaskStates();

      // Set planning phase indicator if planning is enabled
      if (planningEnabled) {
        useTaskExecutionStore.getState().setIsPlanningPhase(true);
      }

      const sessionJobNames = JSON.parse(localStorage.getItem('chatSessionJobNames') || '{}');
      sessionJobNames[sessionId] = jobName;
      localStorage.setItem('chatSessionJobNames', JSON.stringify(sessionJobNames));
    };

    const handleJobCompleted = (event: CustomEvent) => {
      const { jobId } = event.detail;
      const currentExecutingJobId = executingJobIdRef.current;
      const currentLastExecutionJobId = lastExecutionJobIdRef.current;

      // Clear saved state for this job across all sessions
      clearExecutionStateForJob(jobId);

      const shouldClear = currentExecutingJobId === jobId || jobId === currentLastExecutionJobId;

      if (shouldClear) {
        // Clear planning phase when job completes
        useTaskExecutionStore.getState().setIsPlanningPhase(false);

        // Transition any remaining "running" or "planning" tasks to "completed"
        useTaskExecutionStore.getState().transitionAll(
          ['running', 'planning'],
          'completed',
          { completed_at: new Date().toISOString() }
        );

        // Clear refs IMMEDIATELY
        executingJobIdRef.current = null;
        executionStartTimeRef.current = null;
        processedTraceIdsRef.current = new Set();

        // Clear state for UI re-renders
        setExecutingJobId(null);
        setExecutionStartTime(null);
        setProcessedTraceIds(new Set());
        sessionExecutionStates.delete(sessionId);
        window.dispatchEvent(new CustomEvent('forceClearExecution'));

        // Fetch the run result after a delay
        setTimeout(() => {
          runService.getRuns(100).then(runs => {
            const run = runs.runs.find((r: Run) => r.job_id === jobId);

            if (run?.result?.output) {
              let formattedOutput = run.result.output;
              try {
                const parsed = JSON.parse(run.result.output);
                formattedOutput = JSON.stringify(parsed, null, 2);
              } catch {
                formattedOutput = run.result.output;
              }

              const resultMessage: ChatMessage = {
                id: `exec-result-${Date.now()}`,
                type: 'result',
                content: formattedOutput,
                timestamp: new Date(),
                jobId
              };

              addMessage(sessionId, resultMessage);
              saveMessageToBackend(resultMessage);
            } else if (run?.result) {
              let resultContent = typeof run.result === 'string'
                ? run.result
                : JSON.stringify(run.result, null, 2);

              if (typeof run.result === 'string') {
                try {
                  const parsed = JSON.parse(run.result);
                  resultContent = JSON.stringify(parsed, null, 2);
                } catch {
                  // Not JSON, use as-is
                }
              }

              const resultMessage: ChatMessage = {
                id: `exec-result-${Date.now()}`,
                type: 'result',
                content: resultContent,
                timestamp: new Date(),
                jobId
              };

              addMessage(sessionId, resultMessage);
              saveMessageToBackend(resultMessage);
            } else {
              console.warn('[WorkflowChat] No result found for completed job!');
            }
          }).catch(error => {
            console.error('[WorkflowChat] Error fetching job result:', error);
          });
        }, 2000);
      }
    };

    const handleJobFailed = (event: CustomEvent) => {
      const { jobId, error } = event.detail;
      const currentExecutingJobId = executingJobIdRef.current;
      const currentLastExecutionJobId = lastExecutionJobIdRef.current;

      clearExecutionStateForJob(jobId);

      if (currentExecutingJobId === jobId || jobId === currentLastExecutionJobId) {
        // Clear planning phase when job fails
        useTaskExecutionStore.getState().setIsPlanningPhase(false);

        // Transition all "running" or "planning" tasks to "failed"
        useTaskExecutionStore.getState().transitionAll(
          ['running', 'planning'],
          'failed',
          { failed_at: new Date().toISOString() }
        );

        const failureMessage: ChatMessage = {
          id: `exec-failed-${Date.now()}`,
          type: 'execution',
          content: `❌ Execution failed: ${error}`,
          timestamp: new Date(),
          jobId
        };

        addMessage(sessionId, failureMessage);
        saveMessageToBackend(failureMessage);

        // Clear refs IMMEDIATELY
        executingJobIdRef.current = null;
        executionStartTimeRef.current = null;
        processedTraceIdsRef.current = new Set();

        // Clear state for UI re-renders
        setExecutingJobId(null);
        setExecutionStartTime(null);
        setProcessedTraceIds(new Set());
        sessionExecutionStates.delete(sessionId);
        window.dispatchEvent(new CustomEvent('forceClearExecution'));
      }
    };

    const handleTraceUpdate = (event: CustomEvent) => {
      const { jobId, trace } = event.detail;
      const currentExecutingJobId = executingJobIdRef.current;

      if (jobId === currentExecutingJobId && trace) {
        // Generate consistent trace ID — use DB id when available,
        // fall back to event signature for relayed traces (which lack a DB id)
        const traceId = trace.id
          ? `${trace.id}-${trace.created_at}`
          : `${trace.event_type}-${trace.event_context}-${trace.created_at}`;

        // Check if this trace has already been processed
        if (processedTraceIdsRef.current.has(traceId)) {
          return;
        }

        // Also check for semantic duplicates: relay and DB-poll send the same
        // event with different id formats, so match on event signature too
        const eventSignature = `${trace.event_type}-${trace.event_context}-${trace.created_at}`;
        if (trace.id && processedTraceIdsRef.current.has(eventSignature)) {
          return;
        }

        // --- Task status processing via shared utilities ---
        const isTaskEvent = trace.event_type === 'task_started' ||
                           trace.event_type === 'task_completed' ||
                           trace.event_type === 'task_failed';

        if (isTaskEvent) {
          // Clear planning phase on first task event — tasks are now executing
          if (useTaskExecutionStore.getState().isPlanningPhase) {
            useTaskExecutionStore.getState().setIsPlanningPhase(false);
          }

          const taskId = extractTaskId(trace);
          const taskName = extractTaskName(trace);
          const status = mapEventToStatus(trace.event_type);

          if (taskId) {
            useTaskExecutionStore.getState().transition(taskId, status, {
              task_name: taskName ?? '',
              ...(status === 'running' && { started_at: trace.created_at }),
              ...(status === 'completed' && { completed_at: trace.created_at }),
              ...(status === 'failed' && { failed_at: trace.created_at }),
            });
          }
        }

        // --- Chat message creation ---
        // Extract the human-readable content from the trace output.
        // The backend stores output as { content: string, time_since_init, extra_data }.
        let traceContent: string;
        if (typeof trace.output === 'string') {
          traceContent = trace.output;
        } else if (trace.output && typeof trace.output === 'object' && trace.output.content) {
          traceContent = typeof trace.output.content === 'string'
            ? trace.output.content
            : JSON.stringify(trace.output.content, null, 2);
        } else if (trace.output) {
          traceContent = JSON.stringify(trace.output, null, 2);
        } else {
          traceContent = '';
        }

        // Skip chat messages for task lifecycle events that only carry
        // the task name — these are already handled by the task state machine
        // and don't provide useful content for the chat panel.
        if (isTaskEvent && !traceContent) {
          // Still mark as processed so the DB-polled duplicate is also skipped
          setProcessedTraceIds(prev => {
            const newSet = new Set(prev);
            newSet.add(traceId);
            newSet.add(eventSignature);
            return newSet;
          });
          return;
        }

        const traceMessage: ChatMessage = {
          id: `trace-${traceId}`,
          type: 'trace',
          content: traceContent || `[${trace.event_type}]`,
          timestamp: new Date(trace.created_at || Date.now()),
          isIntermediate: true,
          eventSource: trace.event_source,
          eventContext: trace.event_context,
          eventType: trace.event_type,
          jobId
        };

        addMessage(sessionId, traceMessage);
        saveMessageToBackend(traceMessage);

        // Mark this trace as processed (both the specific ID and event signature
        // to prevent duplicates from relay + DB-poll paths)
        setProcessedTraceIds(prev => {
          const newSet = new Set(prev);
          newSet.add(traceId);
          newSet.add(eventSignature);
          return newSet;
        });
      }
    };

    const handleExecutionError = (event: CustomEvent) => {
      const { message } = event.detail;

      setExecutingJobId(null);

      const errorMessage: ChatMessage = {
        id: `exec-error-${Date.now()}`,
        type: 'execution',
        content: `❌ ${message}`,
        timestamp: new Date(),
      };

      addMessage(sessionId, errorMessage);
    };

    const handleForceClearExecution = () => {
      executingJobIdRef.current = null;
      executionStartTimeRef.current = null;
      processedTraceIdsRef.current = new Set();

      setExecutingJobId(null);
      setExecutionStartTime(null);
      setProcessedTraceIds(new Set());
      sessionExecutionStates.delete(sessionId);
    };

    const handleJobStopped = (event: CustomEvent) => {
      const { jobId, partialResults } = event.detail;
      const currentExecutingJobId = executingJobIdRef.current;
      const currentLastExecutionJobId = lastExecutionJobIdRef.current;

      clearExecutionStateForJob(jobId);

      if (currentExecutingJobId === jobId || jobId === currentLastExecutionJobId) {
        const stoppedMessage: ChatMessage = {
          id: `exec-stopped-${Date.now()}`,
          type: 'execution',
          content: `⏹️ Execution stopped by user${partialResults ? ' (partial results saved)' : ''}`,
          timestamp: new Date(),
          jobId
        };

        addMessage(sessionId, stoppedMessage);
        saveMessageToBackend(stoppedMessage);

        executingJobIdRef.current = null;
        executionStartTimeRef.current = null;
        processedTraceIdsRef.current = new Set();

        setExecutingJobId(null);
        setExecutionStartTime(null);
        setProcessedTraceIds(new Set());
        sessionExecutionStates.delete(sessionId);

        window.dispatchEvent(new CustomEvent('forceClearExecution'));
      }
    };

    window.addEventListener('jobCreated', handleJobCreated as EventListener);
    window.addEventListener('jobCompleted', handleJobCompleted as EventListener);
    window.addEventListener('jobFailed', handleJobFailed as EventListener);
    window.addEventListener('jobStopped', handleJobStopped as EventListener);
    window.addEventListener('traceUpdate', handleTraceUpdate as EventListener);
    window.addEventListener('executionError', handleExecutionError as EventListener);
    window.addEventListener('forceClearExecution', handleForceClearExecution);

    return () => {
      window.removeEventListener('jobCreated', handleJobCreated as EventListener);
      window.removeEventListener('jobCompleted', handleJobCompleted as EventListener);
      window.removeEventListener('jobFailed', handleJobFailed as EventListener);
      window.removeEventListener('jobStopped', handleJobStopped as EventListener);
      window.removeEventListener('traceUpdate', handleTraceUpdate as EventListener);
      window.removeEventListener('executionError', handleExecutionError as EventListener);
      window.removeEventListener('forceClearExecution', handleForceClearExecution);
    };
  // CRITICAL: Using refs for executingJobId, lastExecutionJobId, and processedTraceIds
  // so they don't need to be in the dependency array (avoids re-registering event handlers)
  }, [saveMessageToBackend, sessionId, addMessage]);

  // Function to mark that this session is about to start an execution
  const markPendingExecution = useCallback(() => {
    pendingExecutionRef.current = true;
  }, []);

  return {
    executingJobId,
    setExecutingJobId,
    lastExecutionJobId,
    setLastExecutionJobId,
    executionStartTime,
    markPendingExecution,
  };
};

import { useState, useEffect, useCallback, useRef } from 'react';
import { ChatMessage } from '../types/index';
import { useBuilderCanvasStore } from '../../../../app/sessions/builderCanvasStore';
import { streamExecution } from '../../../chat/api/streaming';
import { builderResultContent } from '../utils/resultContent';

import { runService } from '../../../../api/execution/ExecutionHistoryService';
import { useTaskExecutionStore } from '../../../../store/taskExecutionStore';
import { useChatMessagesStore } from '../store/chatMessagesStore';
import { extractTaskId, extractTaskName, mapEventToStatus } from '../../../../utils/taskIdUtils';

export const useExecutionMonitoring = (
  sessionId: string,
  saveMessageToBackend: (message: ChatMessage) => Promise<void>,
  _setMessages: React.Dispatch<React.SetStateAction<ChatMessage[]>>
) => {
  const [executingJobId, setExecutingJobId] = useState<string | null>(null);
  const [lastExecutionJobId, setLastExecutionJobId] = useState<string | null>(null);
  const [processedTraceIds, setProcessedTraceIds] = useState<Set<string>>(new Set());
  const [executionStartTime, setExecutionStartTime] = useState<Date | null>(null);

  // Track if this session is expecting a job to start (prevents other tabs from claiming the job)
  const pendingExecutionRef = useRef<boolean>(false);
  const settledJobsRef = useRef(new Set<string>());

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

  // Retain the last run across mode unmounts AND browser refreshes. The job
  // endpoint remains authoritative: a restored id is reconciled below, so a
  // run that finished while away restores its result instead of staying busy.
  const recoveryKey = `kasal-builder-run:${localStorage.getItem('selectedGroupId') || 'default'}:${sessionId}`;
  useEffect(() => {
    let restoredJobId: string | null = null;
    try { restoredJobId = sessionStorage.getItem(recoveryKey); } catch { /* Storage may be unavailable. */ }
    const jobId = restoredJobId;
    executingJobIdRef.current = jobId;
    lastExecutionJobIdRef.current = restoredJobId;
    executionStartTimeRef.current = null;
    processedTraceIdsRef.current = new Set();
    setExecutingJobId(jobId);
    setLastExecutionJobId(lastExecutionJobIdRef.current);
    setExecutionStartTime(executionStartTimeRef.current);
    setProcessedTraceIds(processedTraceIdsRef.current);
  }, [sessionId, recoveryKey]);

  useEffect(() => {
    if (executingJobId && executingJobIdRef.current === executingJobId) {
      try { sessionStorage.setItem(recoveryKey, executingJobId); } catch { /* Keep monitoring without browser storage. */ }
    }
  }, [executingJobId, recoveryKey]);

  // Get Zustand store methods
  const { addMessage } = useChatMessagesStore();

  // Listen for execution events
  useEffect(() => {
    const handleJobCreated = (event: CustomEvent) => {
      const { jobId, jobName } = event.detail;
      settledJobsRef.current.delete(jobId);
      useBuilderCanvasStore.setState(state => ({ canvases: state.canvases.map(tab =>
        tab.chatSessionId === sessionId && !tab.executionJobIds?.includes(jobId)
          ? { ...tab, executionJobIds: [...(tab.executionJobIds || []), jobId] } : tab
      ) }));

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

      const sessionJobNames = JSON.parse(localStorage.getItem('chatSessionJobNames') || '{}');
      sessionJobNames[sessionId] = jobName;
      localStorage.setItem('chatSessionJobNames', JSON.stringify(sessionJobNames));
    };

    const handleJobCompleted = (event: CustomEvent) => {
      const { jobId } = event.detail;
      if (settledJobsRef.current.has(jobId)) return;
      const currentExecutingJobId = executingJobIdRef.current;
      const currentLastExecutionJobId = lastExecutionJobIdRef.current;


      const shouldClear = currentExecutingJobId === jobId || jobId === currentLastExecutionJobId;

      if (shouldClear) {
        settledJobsRef.current.add(jobId);
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
        window.dispatchEvent(new CustomEvent('forceClearExecution'));

        // Fetch the run result after a delay
        setTimeout(() => {
          runService.getRunByJobId(jobId).then(run => {
            const existing = useChatMessagesStore.getState().messagesBySession[sessionId] || [];
            if (existing.some(message => message.jobId === jobId && message.type === 'result' && !message.isIntermediate)) return;
            const resultContent = builderResultContent(run?.result);
            if (resultContent) {
              const resultMessage: ChatMessage = {
                id: `exec-result-${jobId}`,
                type: 'result',
                content: resultContent,
                timestamp: new Date(),
                jobId,
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
      if (settledJobsRef.current.has(jobId)) return;
      const currentExecutingJobId = executingJobIdRef.current;
      const currentLastExecutionJobId = lastExecutionJobIdRef.current;


      if (currentExecutingJobId === jobId || jobId === currentLastExecutionJobId) {
        settledJobsRef.current.add(jobId);
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

        // Traces are NOT posted into the chat anymore: the run's live output
        // streams token-by-token into an assistant bubble (see the llm_chunk
        // stream effect below), and the full trace detail lives in ShowTrace —
        // posting rows here rendered the same information twice. The trace is
        // still consumed above for the task state machine (canvas node status).
        void traceContent;

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
    };

    const handleJobStopped = (event: CustomEvent) => {
      const { jobId, partialResults } = event.detail;
      if (settledJobsRef.current.has(jobId)) return;
      const currentExecutingJobId = executingJobIdRef.current;
      const currentLastExecutionJobId = lastExecutionJobIdRef.current;


      if (currentExecutingJobId === jobId || jobId === currentLastExecutionJobId) {
        settledJobsRef.current.add(jobId);
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

  // The transcript's trace polling can show completion even when the global
  // run-status event was missed. Reconcile the input against this exact job,
  // independent of whether the Runs panel (or its stream) is mounted.
  const reconcileStatus = useCallback((jobId: string, status: unknown, error?: unknown) => {
    if (executingJobIdRef.current !== jobId) return;
    const normalized = String(status || '').toLowerCase();
    const event = normalized === 'completed' ? 'jobCompleted'
      : normalized === 'failed' ? 'jobFailed'
      : ['stopped', 'cancelled'].includes(normalized) ? 'jobStopped' : null;
    if (event) window.dispatchEvent(new CustomEvent(event, {
      detail: { jobId, status: normalized, error },
    }));
  }, []);

  useEffect(() => {
    if (!executingJobId) return;
    let disposed = false;
    let timer: ReturnType<typeof setTimeout>;
    const check = async () => {
      try {
        const run = await runService.getRunByJobId(executingJobId);
        if (!disposed && run) reconcileStatus(executingJobId, run.status, run.error);
      } catch {
        // A transient request failure must not end the run or stop reconciliation.
      } finally {
        if (!disposed && executingJobIdRef.current === executingJobId) timer = setTimeout(check, 3000);
      }
    };
    void check();
    return () => { disposed = true; clearTimeout(timer); };
  }, [executingJobId, sessionId, reconcileStatus]);

  // ── Live token streaming into the chat ────────────────────────────────
  // While a job runs, llm_chunk frames (crew subprocess → event pipe → SSE)
  // append into ONE assistant bubble — the live view that replaced per-trace
  // chat rows (full trace detail lives in ShowTrace). SSE-gated: without SSE
  // the completion message still arrives via polling exactly as before. The
  // bubble is transient — the terminal result message is authoritative. Keep
  // it across mode switches, then remove it when the run ends.
  //
  // Chunks are COALESCED per animation frame, never painted per SSE frame. A
  // hierarchical crew with large outputs emits llm_chunk at ~30/sec, and each
  // token used to trigger a store set() + a re-render of a markdown bubble that
  // grows to tens of KB — O(n²) work that froze the tab ("Page Unresponsive").
  // Buffering into one append per frame caps the work at the display refresh
  // rate regardless of token rate. (ChatMode paces the same way — see
  // enqueueStreamText in ChatMode/store/executionStore.ts.)
  const streamBubbleRef = useRef<string | null>(null);
  useEffect(() => {
    if (!executingJobId) return;
    const jobId = executingJobId;
    const bubbleId = `stream-${jobId}`;
    streamBubbleRef.current = useChatMessagesStore.getState().messagesBySession?.[sessionId]?.some(message => message.id === bubbleId) ? bubbleId : null;

    let pending = '';
    let rafId: number | null = null;

    const paint = (text: string) => {
      const store = useChatMessagesStore.getState();
      if (streamBubbleRef.current !== bubbleId) {
        streamBubbleRef.current = bubbleId;
        store.addMessage(sessionId, {
          id: bubbleId,
          type: 'assistant',
          content: text,
          timestamp: new Date(),
          isIntermediate: true,
          jobId,
        } as ChatMessage);
      } else {
        store.appendToMessage(sessionId, bubbleId, text);
      }
    };

    const flush = () => {
      rafId = null;
      if (!pending) return;
      const text = pending;
      pending = '';
      paint(text);
    };

    const close = streamExecution(jobId, (event) => {
      if (event.event === 'execution_update') {
        reconcileStatus(jobId, event.data.status, event.data.error || event.data.message);
        return;
      }
      if (event.event !== 'llm_chunk') return;
      const chunk = (event.data.chunk as string) || '';
      if (!chunk) return;
      pending += chunk;
      if (rafId === null) {
        rafId = requestAnimationFrame(flush);
      }
    });
    return () => {
      close();
      if (rafId !== null) {
        cancelAnimationFrame(rafId);
        rafId = null;
      }
      pending = '';
      if (streamBubbleRef.current && executingJobIdRef.current !== jobId) {
        useChatMessagesStore.getState().removeMessage(sessionId, streamBubbleRef.current);
        streamBubbleRef.current = null;
      }
    };
  }, [executingJobId, sessionId, reconcileStatus]);

  return {
    executingJobId,
    setExecutingJobId,
    lastExecutionJobId,
    setLastExecutionJobId,
    executionStartTime,
    markPendingExecution,
  };
};

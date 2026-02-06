import { useState, useEffect, useCallback, useRef } from 'react';
import TraceService from '../../../api/TraceService';
import { ChatMessage } from '../types';
import { stripAnsiEscapes } from '../utils/textProcessing';
import { runService } from '../../../api/ExecutionHistoryService';
import { Run } from '../../../types/run';
import { useTaskExecutionStore } from '../../../store/taskExecutionStore';
import { useChatMessagesStore } from '../../../store/chatMessagesStore';

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
  sessionExecutionStates.forEach((state, sessionId) => {
    if (state.executingJobId === jobId) {
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

  // Ref to track which jobId has had its initial trace fetch done
  // This prevents the continuous polling loop caused by monitorTraces recreation
  const initialFetchDoneForJobRef = useRef<string | null>(null);

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
            initialFetchDoneForJobRef.current = savedState.executingJobId;
          } else {
            // Job completed while we were away, clear the saved state
            sessionExecutionStates.delete(sessionId);
            setExecutingJobId(null);
            setLastExecutionJobId(savedState.lastExecutionJobId);
            setExecutionStartTime(null);
            setProcessedTraceIds(new Set());
            initialFetchDoneForJobRef.current = null;
          }
        }).catch(error => {
          console.error('[ExecutionMonitoring] Error checking job status:', error);
          // On error, restore state anyway to be safe (can manually refresh)
          setExecutingJobId(savedState.executingJobId);
          setLastExecutionJobId(savedState.lastExecutionJobId);
          setExecutionStartTime(savedState.executionStartTime);
          setProcessedTraceIds(savedState.processedTraceIds);
          initialFetchDoneForJobRef.current = savedState.executingJobId;
        });
      } else if (savedState) {
        // Has saved state but no executingJobId, restore other state
        setExecutingJobId(null);
        setLastExecutionJobId(savedState.lastExecutionJobId);
        setExecutionStartTime(null);
        setProcessedTraceIds(savedState.processedTraceIds);
        initialFetchDoneForJobRef.current = null;
      } else {
        // No saved state, start fresh for this session
        setExecutingJobId(null);
        setLastExecutionJobId(null);
        setExecutionStartTime(null);
        setProcessedTraceIds(new Set());
        initialFetchDoneForJobRef.current = null;
      }

      prevSessionIdRef.current = sessionId;
    }
  }, [sessionId]);

  // Get task execution store methods
  const { setTaskState } = useTaskExecutionStore();

  // Get Zustand store methods
  const { addMessage } = useChatMessagesStore();

  // Monitor traces for the executing job
  const monitorTraces = useCallback(async (jobId: string) => {
    try {
      const traces = await TraceService.getTraces(jobId);
      
      if (traces && Array.isArray(traces)) {
        const relevantTraces = traces.filter(trace => {
          const traceId = `${trace.id}-${trace.created_at}`;
          return !processedTraceIds.has(traceId);
        });
        
        
        relevantTraces.forEach((trace) => {
          // Extract task status from trace events
          if (trace.event_type) {
            // Check for task-related events
            const isTaskEvent = trace.event_type === 'task_started' || 
                               trace.event_type === 'task_completed' || 
                               trace.event_type === 'task_failed' ||
                               trace.event_type === 'task_status';
            
            // Log agent_execution events for debugging
            if (trace.event_type === 'agent_execution') {
              // Agent execution events are handled below
            }
            
            if (isTaskEvent && trace.event_context) {
              // Extract task name from event_context
              let taskName = trace.event_context;

              // Track if this is a completion event based on original event_context
              // We need to remember this because taskName gets replaced with actual task name
              const isCompletionEventFromContext = taskName === 'completing_task' || taskName === 'task_completion';
              const isStartEventFromContext = taskName === 'starting_task';

              // Handle different event_context formats based on backend patterns
              // Note: Backend sends 'task_completion' via AgentExecutionCompletedEvent
              if (taskName === 'starting_task' || taskName === 'completing_task' || taskName === 'task_completion') {
                // These are generic contexts used by logging_callbacks.py
                // The actual task name should be in trace_metadata or output
                if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
                  const metadata = trace.trace_metadata as Record<string, unknown>;
                  if (metadata.task_name) {
                    taskName = String(metadata.task_name);
                  }
                }
              } else if (taskName.includes('task:')) {
                // Extract task name after "task:" for execution_callback.py format
                taskName = taskName.split('task:')[1]?.trim() || taskName;
              }
              // Otherwise, event_context contains the actual task description
              
              // Skip if it's not a real task name
              let skipTrace = false;
              if (!taskName ||
                  taskName === 'task_completion' ||
                  taskName === 'starting_task' ||
                  taskName === 'completing_task' ||
                  taskName.length < 5) { // Too short to be a real task
                // First try trace_metadata (where backend puts task_name)
                if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
                  const metadata = trace.trace_metadata as Record<string, unknown>;
                  if (metadata.task_name) {
                    taskName = String(metadata.task_name);
                    skipTrace = false;
                  }
                }
                // Fallback: Try to extract from output.extra_data if still not found
                if (skipTrace !== false && typeof trace.output === 'object' && trace.output !== null && 'extra_data' in trace.output) {
                  const extraData = trace.output.extra_data as Record<string, unknown>;
                  if (extraData?.task_name) {
                    taskName = String(extraData.task_name);
                    skipTrace = false;
                  } else {
                    // Skip this trace as it doesn't have a valid task name
                    skipTrace = true;
                  }
                } else if (skipTrace !== false) {
                  // Skip this trace as it doesn't have a valid task name
                  skipTrace = true;
                }
              }
              
              if (!skipTrace) {
              
              // Generate multiple possible task IDs
              const taskIds: string[] = [];
              
              // Add the task name itself (most important for matching with TaskNode label)
              taskIds.push(taskName);
              
              // IMPORTANT: Also add the actual task_id from metadata if available
              // This ensures we can match with the TaskNode's taskId property
              if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
                const metadata = trace.trace_metadata as Record<string, unknown>;
                if (metadata.task_id && typeof metadata.task_id === 'string') {
                  taskIds.push(metadata.task_id);
                }
                // CRITICAL: Add the frontend_task_id if available
                // This is the original task ID from the workflow designer
                if (metadata.frontend_task_id && typeof metadata.frontend_task_id === 'string') {
                  taskIds.push(metadata.frontend_task_id);
                }
              }
              
              // Generate variations for better matching
              // Split task name into words for partial matching
              const words = taskName.split(/\s+/).filter(w => w.length > 0);
              
              // Add first N words as potential short labels (common pattern for task descriptions)
              for (let i = 1; i <= Math.min(5, words.length); i++) {
                const shortLabel = words.slice(0, i).join(' ');
                taskIds.push(shortLabel);
                taskIds.push(shortLabel.toLowerCase());
              }
              
              // Add last N words (sometimes task names end with key action)
              if (words.length > 5) {
                for (let i = 1; i <= 3; i++) {
                  const endLabel = words.slice(-i).join(' ');
                  taskIds.push(endLabel);
                  taskIds.push(endLabel.toLowerCase());
                }
              }
              
              // Extract potential action words (common task prefixes)
              const actionWords = words.filter(w => {
                const lower = w.toLowerCase();
                return ['research', 'analyze', 'compare', 'recommend', 'create', 'update', 
                        'delete', 'fetch', 'process', 'validate', 'generate', 'build',
                        'test', 'deploy', 'configure', 'setup', 'install', 'check'].includes(lower);
              });
              
              if (actionWords.length > 0) {
                // Add action word alone and with next 1-2 words
                const actionIndex = words.indexOf(actionWords[0]);
                taskIds.push(actionWords[0]);
                taskIds.push(actionWords[0].toLowerCase());
                
                if (actionIndex < words.length - 1) {
                  const withNext = words.slice(actionIndex, Math.min(actionIndex + 3, words.length)).join(' ');
                  taskIds.push(withNext);
                  taskIds.push(withNext.toLowerCase());
                }
              }
              
              // Add label-based ID
              const labelBasedId = `task_${taskName.replace(/\s+/g, '_').toLowerCase()}`;
              taskIds.push(labelBasedId);
              
              // Add lowercase version
              const taskNameLower = taskName.toLowerCase();
              if (taskNameLower !== taskName) {
                taskIds.push(taskNameLower);
              }
              
              // Add version with underscores replaced by spaces
              const taskNameWithSpaces = taskName.replace(/_/g, ' ');
              if (taskNameWithSpaces !== taskName) {
                taskIds.push(taskNameWithSpaces);
              }
              
              // Remove duplicates
              const uniqueTaskIds = Array.from(new Set(taskIds));
              
              // Determine status - check both event_type AND event_context
              let status: 'running' | 'completed' | 'failed' = 'running';
              if (trace.event_type === 'task_completed' || isCompletionEventFromContext) {
                // Mark as completed if event_type is task_completed OR if event_context was 'task_completion'/'completing_task'
                status = 'completed';
              } else if (trace.event_type === 'task_failed') {
                status = 'failed';
              } else if (trace.event_type === 'task_started' || isStartEventFromContext) {
                status = 'running';
              } else if (trace.event_type === 'agent_execution') {
                // For agent_execution, check if it contains Final Answer (task completion)
                status = 'running'; // Default to running

                // Check various output formats for Final Answer
                if (typeof trace.output === 'object' && trace.output !== null) {
                  let outputText = '';
                  if ('content' in trace.output) {
                    outputText = String(trace.output.content);
                  } else if ('agent_execution' in trace.output) {
                    outputText = String(trace.output.agent_execution);
                  } else if ('raw' in trace.output) {
                    outputText = String(trace.output.raw);
                  } else {
                    // Try to stringify the entire output
                    outputText = JSON.stringify(trace.output);
                  }

                  if (outputText.includes('Final Answer:') || outputText.includes('## Final Answer')) {
                    status = 'completed';
                  }
                } else if (typeof trace.output === 'string') {
                  // Direct string output
                  if (trace.output.includes('Final Answer:') || trace.output.includes('## Final Answer')) {
                    status = 'completed';
                  }
                }
              }

              // Update state for all possible task ID formats
              uniqueTaskIds.forEach(id => {
                // Get existing state to preserve previous status if needed
                const existingState = useTaskExecutionStore.getState().getTaskStatus(id);

                // Only update if status has changed or if it's a new task
                if (!existingState || existingState.status !== status) {
                  setTaskState(id, {
                    status,
                    task_name: taskName,
                    ...(status === 'running' && { started_at: trace.created_at }),
                    ...(status === 'completed' && { completed_at: trace.created_at }),
                    ...(status === 'failed' && { failed_at: trace.created_at }),
                    // Preserve existing timestamps if transitioning to new state
                    ...(existingState && {
                      ...(existingState.started_at && { started_at: existingState.started_at }),
                      ...(existingState.completed_at && status !== 'running' && { completed_at: existingState.completed_at })
                    })
                  });
                }
              });
              } // Close the if (!skipTrace) block
            }
          
          // Also check for task status in output content (for compatibility with new backend)
            if (typeof trace.output === 'object' && trace.output !== null && 'extra_data' in trace.output) {
              const extraData = trace.output.extra_data as Record<string, unknown>;
              if (extraData?.task_name) {
                const taskName = String(extraData.task_name);
                const taskId = extraData.task_id ? String(extraData.task_id) : `task_${taskName.replace(/\s+/g, '_').toLowerCase()}`;
                
                // Store under multiple formats for compatibility:
                const taskIds = [taskId, taskName];  // Include task name itself
                
                // CRITICAL: Add the frontend_task_id if available
                // This is the original task ID from the workflow designer
                if (extraData.frontend_task_id && typeof extraData.frontend_task_id === 'string') {
                  taskIds.push(String(extraData.frontend_task_id));
                }
                
                // Add lowercase version
                const taskNameLower = taskName.toLowerCase();
                if (taskNameLower !== taskName) {
                  taskIds.push(taskNameLower);
                }
                
                // Add version with underscores replaced by spaces
                const taskNameWithSpaces = taskName.replace(/_/g, ' ');
                if (taskNameWithSpaces !== taskName) {
                  taskIds.push(taskNameWithSpaces);
                }
                
                // If taskId doesn't start with task- or task_, add those variants
                if (!taskId.startsWith('task-') && !taskId.startsWith('task_')) {
                  taskIds.push(`task-${taskId}`);
                }
                
                // Determine status based on event type or output content
                let status: 'running' | 'completed' | 'failed' = 'running';
                if (trace.event_type === 'task_completed' || extraData?.status === 'completed') {
                  status = 'completed';
                } else if (trace.event_type === 'task_failed' || extraData?.status === 'failed') {
                  status = 'failed';
                } else if (trace.event_type === 'task_started' || extraData?.status === 'running') {
                  status = 'running';
                }
              
                
                // Update state for all possible task ID formats
                taskIds.forEach(id => {
                  // Get existing state to preserve previous status if needed
                  const existingState = useTaskExecutionStore.getState().getTaskStatus(id);
                  
                  // Only update if status has changed or if it's a new task
                  if (!existingState || existingState.status !== status) {
                    setTaskState(id, {
                      status,
                      task_name: taskName,
                      ...(status === 'running' && { started_at: trace.created_at }),
                      ...(status === 'completed' && { completed_at: trace.created_at }),
                      ...(status === 'failed' && { failed_at: trace.created_at }),
                      // Preserve existing timestamps if transitioning to new state
                      ...(existingState && {
                        ...(existingState.started_at && { started_at: existingState.started_at }),
                        ...(existingState.completed_at && status !== 'running' && { completed_at: existingState.completed_at })
                      })
                    });
                  }
                });
              }
            }
          }
          
          let content = '';
          if (typeof trace.output === 'string') {
            content = stripAnsiEscapes(trace.output);
          } else if (trace.output?.agent_execution && typeof trace.output.agent_execution === 'string') {
            content = stripAnsiEscapes(trace.output.agent_execution);
          } else if (trace.output?.content && typeof trace.output.content === 'string') {
            content = stripAnsiEscapes(trace.output.content);
          } else if (trace.output) {
            content = JSON.stringify(trace.output, null, 2);
          }
          
          if (!content.trim()) return;
          
          const traceId = `${trace.id}-${trace.created_at}`;
          const traceMessage: ChatMessage = {
            id: `trace-${traceId}`,
            type: 'trace',
            content,
            timestamp: new Date(trace.created_at || Date.now()),
            isIntermediate: true,
            eventSource: trace.event_source,
            eventContext: trace.event_context,
            eventType: trace.event_type,
            jobId
          };
          
          addMessage(sessionId, traceMessage);
          saveMessageToBackend(traceMessage);
          
          setProcessedTraceIds(prev => {
            const newSet = new Set(prev);
            newSet.add(traceId);
            return newSet;
          });
        });
      }
    } catch (error) {
      console.error('[ChatPanel] Error monitoring traces:', error);
      if (error instanceof Error) {
        console.error('[ChatPanel] Error details:', error.message);
        console.error('[ChatPanel] Error stack:', error.stack);
      }
    }
  }, [processedTraceIds, saveMessageToBackend, addMessage, sessionId, setTaskState]);

  // Listen for execution events
  useEffect(() => {
    const handleJobCreated = (event: CustomEvent) => {
      const { jobId, jobName } = event.detail;

      // Check if this session initiated the execution via markPendingExecution
      // If pendingExecution is true, this session owns the job
      // If not, we still track it (for backwards compatibility with single-tab usage)
      const isPendingForThisSession = pendingExecutionRef.current;


      // Clear the pending flag if it was set
      if (isPendingForThisSession) {
        pendingExecutionRef.current = false;
      }

      // Update refs IMMEDIATELY (before React's async state update)
      // This ensures handleJobCompleted can find the jobId even if it fires quickly
      executingJobIdRef.current = jobId;
      lastExecutionJobIdRef.current = jobId;
      processedTraceIdsRef.current = new Set();
      executionStartTimeRef.current = new Date();

      // Also update state for UI re-renders
      setExecutingJobId(jobId);
      setLastExecutionJobId(jobId);
      setProcessedTraceIds(new Set());
      setExecutionStartTime(new Date());

      // Don't clear task states here - WorkflowDesigner handles this
      // clearTaskStates();
      
      const sessionJobNames = JSON.parse(localStorage.getItem('chatSessionJobNames') || '{}');
      sessionJobNames[sessionId] = jobName;
      localStorage.setItem('chatSessionJobNames', JSON.stringify(sessionJobNames));
      
      // Remove pending execution messages from Zustand store
      // Note: This is handled by the state management, no direct filter needed
    };

    const handleJobCompleted = (event: CustomEvent) => {
      const { jobId } = event.detail;
      // Use refs to get current values (avoid stale closure issues)
      const currentExecutingJobId = executingJobIdRef.current;
      const currentLastExecutionJobId = lastExecutionJobIdRef.current;

      // Clear saved state for this job across all sessions
      clearExecutionStateForJob(jobId);

      // ALWAYS clear state for this job, regardless of whether we're currently tracking it
      // This handles the case where the event fires when we're on a different tab
      const shouldClear = currentExecutingJobId === jobId || jobId === currentLastExecutionJobId;

      if (shouldClear) {

        // Clear refs IMMEDIATELY (before React's async state update)
        executingJobIdRef.current = null;
        executionStartTimeRef.current = null;
        processedTraceIdsRef.current = new Set();

        // Clear state for UI re-renders
        setExecutingJobId(null);
        setExecutionStartTime(null);
        setProcessedTraceIds(new Set());
        sessionExecutionStates.delete(sessionId);
        window.dispatchEvent(new CustomEvent('forceClearExecution'));

        // Add a small delay to ensure the backend has updated the result
        setTimeout(() => {
          
          // Fetch the run details to get the result
          runService.getRuns(100).then(runs => {
            
            const run = runs.runs.find((r: Run) => r.job_id === jobId);
            
            // Debug: Show all runs for this job
            // const _allRunsForJob = runs.runs.filter((r: Run) => r.job_id === jobId);
            
            if (run?.result?.output) {
              
              // Format the output properly
              let formattedOutput = run.result.output;
              
              // Try to parse and prettify JSON
              try {
                const parsed = JSON.parse(run.result.output);
                formattedOutput = JSON.stringify(parsed, null, 2);
              } catch (e) {
                // Not JSON, use as-is
                formattedOutput = run.result.output;
              }
              
              const resultMessage: ChatMessage = {
                id: `exec-result-${Date.now()}`,
                type: 'result', // Special type for final results
                content: formattedOutput,
                timestamp: new Date(),
                jobId
              };
              
              addMessage(sessionId, resultMessage);
              saveMessageToBackend(resultMessage);
            } else if (run?.result) {
              
              // If output is not in the result.output field, try to display the entire result
              let resultContent = typeof run.result === 'string' 
                ? run.result 
                : JSON.stringify(run.result, null, 2);
              
              // If result is a string that might be JSON, try to parse and prettify it
              if (typeof run.result === 'string') {
                try {
                  const parsed = JSON.parse(run.result);
                  resultContent = JSON.stringify(parsed, null, 2);
                } catch (e) {
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
              
              // Try to find result in the last trace message
            }
          }).catch(error => {
            console.error('[WorkflowChat] Error fetching job result:', error);
          });
        }, 2000); // Wait 2 seconds for the backend to update
      }
    };

    const handleJobFailed = (event: CustomEvent) => {
      const { jobId, error } = event.detail;
      // Use refs to get current values (avoid stale closure issues)
      const currentExecutingJobId = executingJobIdRef.current;
      const currentLastExecutionJobId = lastExecutionJobIdRef.current;

      // Clear saved state for this job across all sessions
      clearExecutionStateForJob(jobId);

      if (currentExecutingJobId === jobId || jobId === currentLastExecutionJobId) {

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
      // Use refs to get current values (avoid stale closure issues)
      const currentExecutingJobId = executingJobIdRef.current;


      if (jobId === currentExecutingJobId && trace) {
        // Generate consistent trace ID
        const traceId = `${trace.id}-${trace.created_at}`;

        // Check if this trace has already been processed (use ref for current value)
        if (processedTraceIdsRef.current.has(traceId)) {
          return;
        }

        // Extract task status from trace events (same logic as monitorTraces)
        if (trace.event_type) {
          const isTaskEvent = trace.event_type === 'task_started' ||
                             trace.event_type === 'task_completed' ||
                             trace.event_type === 'task_failed' ||
                             trace.event_type === 'task_status';

          if (isTaskEvent && trace.event_context) {
            let taskName = trace.event_context;

            // Track if this is a completion event based on original event_context
            // We need to remember this because taskName gets replaced with actual task name
            const isCompletionEventFromContext = taskName === 'completing_task' || taskName === 'task_completion';
            const isStartEventFromContext = taskName === 'starting_task';

            // Handle generic context formats (including 'task_completion' from AgentExecutionCompletedEvent)
            if (taskName === 'starting_task' || taskName === 'completing_task' || taskName === 'task_completion') {
              if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
                const metadata = trace.trace_metadata as Record<string, unknown>;
                if (metadata.task_name) {
                  taskName = String(metadata.task_name);
                }
              }
            } else if (taskName.includes('task:')) {
              taskName = taskName.split('task:')[1]?.trim() || taskName;
            }

            // Skip if not a valid task name
            let skipTrace = false;
            if (!taskName ||
                taskName === 'task_completion' ||
                taskName === 'starting_task' ||
                taskName === 'completing_task' ||
                taskName.length < 5) {
              // First try trace_metadata (where backend puts task_name)
              if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
                const metadata = trace.trace_metadata as Record<string, unknown>;
                if (metadata.task_name) {
                  taskName = String(metadata.task_name);
                  skipTrace = false;
                }
              }
              // Fallback to output.extra_data for legacy format
              if (skipTrace !== false && typeof trace.output === 'object' && trace.output !== null && 'extra_data' in trace.output) {
                const extraData = trace.output.extra_data as Record<string, unknown>;
                if (extraData?.task_name) {
                  taskName = String(extraData.task_name);
                } else {
                  skipTrace = true;
                }
              } else if (skipTrace !== false) {
                skipTrace = true;
              }
            }

            if (!skipTrace) {
              // Generate multiple possible task IDs for matching
              const taskIds: string[] = [taskName];

              // Add task_id from metadata if available
              if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
                const metadata = trace.trace_metadata as Record<string, unknown>;
                if (metadata.task_id && typeof metadata.task_id === 'string') {
                  taskIds.push(metadata.task_id);
                }
                if (metadata.frontend_task_id && typeof metadata.frontend_task_id === 'string') {
                  taskIds.push(String(metadata.frontend_task_id));
                }
              }

              // Add variations for better matching
              const words = taskName.split(/\s+/).filter((w: string) => w.length > 0);
              for (let i = 1; i <= Math.min(5, words.length); i++) {
                const shortLabel = words.slice(0, i).join(' ');
                taskIds.push(shortLabel);
                taskIds.push(shortLabel.toLowerCase());
              }

              const labelBasedId = `task_${taskName.replace(/\s+/g, '_').toLowerCase()}`;
              taskIds.push(labelBasedId);
              taskIds.push(taskName.toLowerCase());
              taskIds.push(taskName.replace(/_/g, ' '));

              // Determine status - check both event_type AND event_context
              let status: 'running' | 'completed' | 'failed' = 'running';
              if (trace.event_type === 'task_completed' || isCompletionEventFromContext) {
                status = 'completed';
              } else if (trace.event_type === 'task_failed') {
                status = 'failed';
              } else if (trace.event_type === 'task_started' || isStartEventFromContext) {
                status = 'running';
              }

              // Update state for all possible task ID formats
              const uniqueTaskIds = Array.from(new Set(taskIds));
              uniqueTaskIds.forEach(id => {
                const existingState = useTaskExecutionStore.getState().getTaskStatus(id);
                if (!existingState || existingState.status !== status) {
                  setTaskState(id, {
                    status,
                    task_name: taskName,
                    ...(status === 'running' && { started_at: trace.created_at }),
                    ...(status === 'completed' && { completed_at: trace.created_at }),
                    ...(status === 'failed' && { failed_at: trace.created_at }),
                    ...(existingState && {
                      ...(existingState.started_at && { started_at: existingState.started_at }),
                    })
                  });
                }
              });
            }
          }
        }

        const traceMessage: ChatMessage = {
          id: `trace-${traceId}`,
          type: 'trace',
          content: typeof trace.output === 'string' ? trace.output : JSON.stringify(trace.output, null, 2),
          timestamp: new Date(trace.created_at || Date.now()),
          isIntermediate: true,
          eventSource: trace.event_source,
          eventContext: trace.event_context,
          eventType: trace.event_type,
          jobId
        };

        addMessage(sessionId, traceMessage);
        saveMessageToBackend(traceMessage);

        // Mark this trace as processed
        setProcessedTraceIds(prev => {
          const newSet = new Set(prev);
          newSet.add(traceId);
          return newSet;
        });
      }
    };

    const handleExecutionError = (event: CustomEvent) => {
      const { message } = event.detail;
      
      setExecutingJobId(null);
      
      // Add execution error message
      const errorMessage: ChatMessage = {
        id: `exec-error-${Date.now()}`,
        type: 'execution',
        content: `❌ ${message}`,
        timestamp: new Date(),
      };

      addMessage(sessionId, errorMessage);
    };

    const handleForceClearExecution = () => {
      // Clear refs IMMEDIATELY
      executingJobIdRef.current = null;
      executionStartTimeRef.current = null;
      processedTraceIdsRef.current = new Set();
      initialFetchDoneForJobRef.current = null;

      // Clear state for UI re-renders
      setExecutingJobId(null);
      setExecutionStartTime(null);
      setProcessedTraceIds(new Set());
      sessionExecutionStates.delete(sessionId);
    };

    const handleJobStopped = (event: CustomEvent) => {
      const { jobId, partialResults } = event.detail;
      // Use refs to get current values (avoid stale closure issues)
      const currentExecutingJobId = executingJobIdRef.current;
      const currentLastExecutionJobId = lastExecutionJobIdRef.current;

      // Clear saved state for this job across all sessions
      clearExecutionStateForJob(jobId);

      if (currentExecutingJobId === jobId || jobId === currentLastExecutionJobId) {

        // Add a message about the execution being stopped
        const stoppedMessage: ChatMessage = {
          id: `exec-stopped-${Date.now()}`,
          type: 'execution',
          content: `⏹️ Execution stopped by user${partialResults ? ' (partial results saved)' : ''}`,
          timestamp: new Date(),
          jobId
        };

        addMessage(sessionId, stoppedMessage);
        saveMessageToBackend(stoppedMessage);

        // Clear refs IMMEDIATELY
        executingJobIdRef.current = null;
        executionStartTimeRef.current = null;
        processedTraceIdsRef.current = new Set();

        // Clear state for UI re-renders
        setExecutingJobId(null);
        setExecutionStartTime(null);
        setProcessedTraceIds(new Set());
        sessionExecutionStates.delete(sessionId);

        // Force clear any lingering execution state
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
  }, [saveMessageToBackend, sessionId, addMessage, setTaskState]);

  // Initial trace fetch when execution begins - SSE handles real-time updates
  // CRITICAL: Only depends on executingJobId, NOT monitorTraces
  // monitorTraces is called via ref pattern to avoid continuous polling loop
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    if (executingJobId && initialFetchDoneForJobRef.current !== executingJobId) {
      // Mark this jobId as having its initial fetch scheduled
      // This prevents re-running if monitorTraces callback is recreated
      initialFetchDoneForJobRef.current = executingJobId;

      // Do a single initial fetch after a short delay to get any traces that might have been created
      // before the SSE connection was established
      const initialTimeout = setTimeout(() => monitorTraces(executingJobId), 2000);

      return () => {
        clearTimeout(initialTimeout);
      };
    }
  }, [executingJobId]);

  // Function to mark that this session is about to start an execution
  // Call this BEFORE calling executeCrew/executeFlow
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
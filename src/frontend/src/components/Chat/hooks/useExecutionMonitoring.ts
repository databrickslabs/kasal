import { useState, useEffect, useCallback } from 'react';
import TraceService from '../../../api/TraceService';
import { ChatMessage } from '../types';
import { stripAnsiEscapes } from '../utils/textProcessing';
import { runService } from '../../../api/ExecutionHistoryService';
import { Run } from '../../../types/run';
import { useTaskExecutionStore } from '../../../store/taskExecutionStore';
import { useChatMessagesStore } from '../../../store/chatMessagesStore';

export const useExecutionMonitoring = (
  sessionId: string,
  saveMessageToBackend: (message: ChatMessage) => Promise<void>,
  setMessages: React.Dispatch<React.SetStateAction<ChatMessage[]>>
) => {
  const [executingJobId, setExecutingJobId] = useState<string | null>(null);
  const [lastExecutionJobId, setLastExecutionJobId] = useState<string | null>(null);
  const [processedTraceIds, setProcessedTraceIds] = useState<Set<string>>(new Set());
  const [executionStartTime, setExecutionStartTime] = useState<Date | null>(null);

  // Get task execution store methods
  const { setTaskState, clearTaskStates } = useTaskExecutionStore();

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
            // DEBUG: Log all traces
            console.log(`[DEBUG useExecutionMonitoring] Processing trace:`, {
              event_type: trace.event_type,
              event_context: trace.event_context,
              event_source: trace.event_source,
              trace_metadata: trace.trace_metadata,
              created_at: trace.created_at
            });
            
            // Check for task-related events
            const isTaskEvent = trace.event_type === 'task_started' || 
                               trace.event_type === 'task_completed' || 
                               trace.event_type === 'task_failed' ||
                               trace.event_type === 'task_status';
            
            // Log agent_execution events for debugging
            if (trace.event_type === 'agent_execution') {
              // Agent execution events are handled below
            }
            
            if (isTaskEvent) {
              console.log(`[DEBUG useExecutionMonitoring] Task event detected: ${trace.event_type}, context: "${trace.event_context}"`);
            }
            
            if (isTaskEvent && trace.event_context) {
              // Extract task name from event_context
              let taskName = trace.event_context;
              
              // Handle different event_context formats based on backend patterns
              if (taskName === 'starting_task' || taskName === 'completing_task') {
                console.log(`[DEBUG useExecutionMonitoring] Generic context "${taskName}" - checking metadata`);
                console.log(`[DEBUG useExecutionMonitoring] Full trace_metadata:`, trace.trace_metadata);
                
                // These are generic contexts used by logging_callbacks.py
                // The actual task name should be in trace_metadata or output
                if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
                  const metadata = trace.trace_metadata as Record<string, unknown>;
                  console.log(`[DEBUG useExecutionMonitoring] Metadata keys:`, Object.keys(metadata));
                  
                  if (metadata.task_name) {
                    taskName = String(metadata.task_name);
                    console.log(`[DEBUG useExecutionMonitoring] Extracted task_name from metadata: "${taskName}"`);
                  } else {
                    console.log(`[DEBUG useExecutionMonitoring] No task_name in metadata, taskName remains: "${taskName}"`);
                  }
                } else {
                  console.log(`[DEBUG useExecutionMonitoring] No metadata object available`);
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
                // Try to extract from output if available
                if (typeof trace.output === 'object' && trace.output !== null && 'extra_data' in trace.output) {
                  const extraData = trace.output.extra_data as Record<string, unknown>;
                  if (extraData?.task_name) {
                    taskName = String(extraData.task_name);
                  } else {
                    // Skip this trace as it doesn't have a valid task name
                    skipTrace = true;
                  }
                } else {
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
                  console.log(`[DEBUG useExecutionMonitoring] Found task_id in metadata: ${metadata.task_id}`);
                }
                // CRITICAL: Add the frontend_task_id if available
                // This is the original task ID from the workflow designer
                if (metadata.frontend_task_id && typeof metadata.frontend_task_id === 'string') {
                  console.log(`[DEBUG useExecutionMonitoring] Raw frontend_task_id from metadata: "${metadata.frontend_task_id}"`);
                  taskIds.push(metadata.frontend_task_id);
                  console.log(`[DEBUG useExecutionMonitoring] Added frontend_task_id to taskIds: ${metadata.frontend_task_id}`);
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
              
              // Determine status
              let status: 'running' | 'completed' | 'failed' = 'running';
              if (trace.event_type === 'task_completed') {
                status = 'completed';
              } else if (trace.event_type === 'task_failed') {
                status = 'failed';
              } else if (trace.event_type === 'task_started') {
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
              console.log(`[DEBUG useExecutionMonitoring] About to update task states for ${uniqueTaskIds.length} IDs`);
              console.log(`[DEBUG useExecutionMonitoring] Task IDs to update:`, uniqueTaskIds.slice(0, 5));
              
              uniqueTaskIds.forEach(id => {
                // Get existing state to preserve previous status if needed
                const existingState = useTaskExecutionStore.getState().getTaskStatus(id);
                
                console.log(`[DEBUG useExecutionMonitoring] Storing task state for ID: "${id}" - existing state:`, existingState, 'new status:', status);
                
                // Only update if status has changed or if it's a new task
                if (!existingState || existingState.status !== status) {
                  console.log(`[DEBUG useExecutionMonitoring] Actually calling setTaskState for "${id}" with status:`, status);
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
                  console.log(`[DEBUG useExecutionMonitoring] Found frontend_task_id in extra_data: ${extraData.frontend_task_id}`);
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
      
      if (executingJobId || jobId === lastExecutionJobId) {
        
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
        
        setExecutingJobId(null);
        setExecutionStartTime(null);
        setProcessedTraceIds(new Set());
        window.dispatchEvent(new CustomEvent('forceClearExecution'));
      } else {
        // No execution in progress, nothing to clear
      }
    };

    const handleJobFailed = (event: CustomEvent) => {
      const { jobId, error } = event.detail;
      
      if (executingJobId || jobId === lastExecutionJobId) {
        const failureMessage: ChatMessage = {
          id: `exec-failed-${Date.now()}`,
          type: 'execution',
          content: `❌ Execution failed: ${error}`,
          timestamp: new Date(),
          jobId
        };
        
        addMessage(sessionId, failureMessage);
        saveMessageToBackend(failureMessage);
        
        setExecutingJobId(null);
        setExecutionStartTime(null);
        setProcessedTraceIds(new Set());
        window.dispatchEvent(new CustomEvent('forceClearExecution'));
      }
    };

    const handleTraceUpdate = (event: CustomEvent) => {
      const { jobId, trace } = event.detail;
      if (jobId === executingJobId && trace) {
        // Generate consistent trace ID
        const traceId = `${trace.id}-${trace.created_at}`;
        
        // Check if this trace has already been processed
        if (processedTraceIds.has(traceId)) {
          return;
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
      setExecutingJobId(null);
      setExecutionStartTime(null);
      setProcessedTraceIds(new Set());
    };

    const handleJobStopped = (event: CustomEvent) => {
      const { jobId, partialResults } = event.detail;
      
      if (executingJobId === jobId || jobId === lastExecutionJobId) {
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
        
        // Clear execution state to re-enable input
        setExecutingJobId(null);
        setExecutionStartTime(null);
        setProcessedTraceIds(new Set());
        
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
  }, [executingJobId, lastExecutionJobId, processedTraceIds, executionStartTime, saveMessageToBackend, sessionId, addMessage, clearTaskStates]);

  // Start trace monitoring when execution begins
  useEffect(() => {
    let interval: NodeJS.Timeout | null = null;
    
    if (executingJobId) {
      const initialTimeout = setTimeout(() => monitorTraces(executingJobId), 2000);
      interval = setInterval(() => monitorTraces(executingJobId), 2000);
      
      return () => {
        clearTimeout(initialTimeout);
        if (interval) clearInterval(interval);
      };
    }
    
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [executingJobId, monitorTraces]);

  return {
    executingJobId,
    setExecutingJobId,
    lastExecutionJobId,
    setLastExecutionJobId,
    executionStartTime,
  };
};
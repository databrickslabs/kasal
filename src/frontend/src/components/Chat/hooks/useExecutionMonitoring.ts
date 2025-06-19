import { useState, useEffect, useCallback } from 'react';
import TraceService from '../../../api/TraceService';
import { ChatMessage } from '../types';
import { stripAnsiEscapes } from '../utils/textProcessing';
import { runService } from '../../../api/ExecutionHistoryService';
import { Run } from '../../../types/run';

export const useExecutionMonitoring = (
  sessionId: string,
  saveMessageToBackend: (message: ChatMessage) => Promise<void>,
  setMessages: React.Dispatch<React.SetStateAction<ChatMessage[]>>
) => {
  const [executingJobId, setExecutingJobId] = useState<string | null>(null);
  const [lastExecutionJobId, setLastExecutionJobId] = useState<string | null>(null);
  const [processedTraceIds, setProcessedTraceIds] = useState<Set<string>>(new Set());
  const [executionStartTime, setExecutionStartTime] = useState<Date | null>(null);

  // Monitor traces for the executing job
  const monitorTraces = useCallback(async (jobId: string) => {
    try {
      console.log(`[ChatPanel] Monitoring traces for job ${jobId}`);
      const traces = await TraceService.getTraces(jobId);
      console.log(`[ChatPanel] Found ${traces?.length || 0} total traces for job ${jobId}`);
      
      if (traces && Array.isArray(traces)) {
        const relevantTraces = traces.filter(trace => {
          const traceId = `${trace.id}-${trace.created_at}`;
          return !processedTraceIds.has(traceId);
        });
        
        console.log(`[ChatPanel] ${relevantTraces.length} new traces to display`);
        
        relevantTraces.forEach((trace) => {
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
          
          setMessages(prev => [...prev, traceMessage]);
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
  }, [processedTraceIds, saveMessageToBackend, setMessages]);

  // Listen for execution events
  useEffect(() => {
    const handleJobCreated = (event: CustomEvent) => {
      const { jobId, jobName } = event.detail;
      console.log('[WorkflowChat] Received jobCreated event:', { jobId, jobName });
      setExecutingJobId(jobId);
      setLastExecutionJobId(jobId);
      setProcessedTraceIds(new Set());
      setExecutionStartTime(new Date());
      
      const sessionJobNames = JSON.parse(localStorage.getItem('chatSessionJobNames') || '{}');
      sessionJobNames[sessionId] = jobName;
      localStorage.setItem('chatSessionJobNames', JSON.stringify(sessionJobNames));
      
      setMessages(prev => prev.filter(msg => 
        !(msg.type === 'execution' && msg.content.includes('⏳ Preparing to execute crew...'))
      ));
    };

    const handleJobCompleted = (event: CustomEvent) => {
      const { jobId } = event.detail;
      console.log('[WorkflowChat] === JOB COMPLETED EVENT ===');
      console.log('[WorkflowChat] JobId:', jobId);
      console.log('[WorkflowChat] ExecutingJobId:', executingJobId);
      console.log('[WorkflowChat] LastExecutionJobId:', lastExecutionJobId);
      console.log('[WorkflowChat] Event detail:', event.detail);
      
      if (executingJobId || jobId === lastExecutionJobId) {
        console.log('[WorkflowChat] Job ID matches, proceeding to fetch result...');
        
        // Add a small delay to ensure the backend has updated the result
        setTimeout(() => {
          console.log('[WorkflowChat] Fetching runs from backend...');
          
          // Fetch the run details to get the result
          runService.getRuns(100).then(runs => {
            console.log('[WorkflowChat] Received runs response:', runs);
            console.log('[WorkflowChat] Total runs:', runs.runs.length);
            
            const run = runs.runs.find((r: Run) => r.job_id === jobId);
            console.log('[WorkflowChat] === FOUND RUN ===');
            console.log('[WorkflowChat] Run:', run);
            console.log('[WorkflowChat] Run result:', run?.result);
            console.log('[WorkflowChat] Run result type:', typeof run?.result);
            console.log('[WorkflowChat] Run result.output:', run?.result?.output);
            console.log('[WorkflowChat] Run status:', run?.status);
            
            // Debug: Show all runs for this job
            const allRunsForJob = runs.runs.filter((r: Run) => r.job_id === jobId);
            console.log('[WorkflowChat] All runs for this job:', allRunsForJob);
            
            if (run?.result?.output) {
              console.log('[WorkflowChat] Creating result message with output:', run.result.output);
              
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
              
              console.log('[WorkflowChat] Adding result message to chat:', resultMessage);
              setMessages(prev => {
                console.log('[WorkflowChat] Previous messages count:', prev.length);
                return [...prev, resultMessage];
              });
              saveMessageToBackend(resultMessage);
            } else if (run?.result) {
              console.log('[WorkflowChat] Result exists but no output field, using entire result:', run.result);
              
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
              
              console.log('[WorkflowChat] Adding result message to chat (from full result):', resultMessage);
              setMessages(prev => [...prev, resultMessage]);
              saveMessageToBackend(resultMessage);
            } else {
              console.warn('[WorkflowChat] No result found for completed job!');
              console.log('[WorkflowChat] Run object keys:', run ? Object.keys(run) : 'run is null');
              
              // Try to find result in the last trace message
              console.log('[WorkflowChat] Checking last trace messages for result...');
            }
          }).catch(error => {
            console.error('[WorkflowChat] Error fetching job result:', error);
            console.error('[WorkflowChat] Error details:', error.response || error.message || error);
          });
        }, 2000); // Wait 2 seconds for the backend to update
        
        setExecutingJobId(null);
        setExecutionStartTime(null);
        setProcessedTraceIds(new Set());
        window.dispatchEvent(new CustomEvent('forceClearExecution'));
      } else {
        console.log('[WorkflowChat] Job ID does not match, ignoring completion event');
      }
    };

    const handleJobFailed = (event: CustomEvent) => {
      const { jobId, error } = event.detail;
      console.log('[WorkflowChat] Job failed event:', { jobId, executingJobId });
      
      if (executingJobId || jobId === lastExecutionJobId) {
        const failureMessage: ChatMessage = {
          id: `exec-failed-${Date.now()}`,
          type: 'execution',
          content: `❌ Execution failed: ${error}`,
          timestamp: new Date(),
          jobId
        };
        
        setMessages(prev => [...prev, failureMessage]);
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
        const traceMessage: ChatMessage = {
          id: `trace-${trace.id || Date.now()}`,
          type: 'trace',
          content: typeof trace.output === 'string' ? trace.output : JSON.stringify(trace.output, null, 2),
          timestamp: new Date(trace.created_at || Date.now()),
          isIntermediate: true,
          eventSource: trace.event_source,
          eventContext: trace.event_context,
          eventType: trace.event_type,
          jobId
        };
        
        setMessages(prev => [...prev, traceMessage]);
        saveMessageToBackend(traceMessage);
      }
    };

    const handleExecutionError = (event: CustomEvent) => {
      const { message } = event.detail;
      console.log('[WorkflowChat] Received executionError event:', message);
      
      setExecutingJobId(null);
      
      setMessages(prev => {
        const fiveSecondsAgo = Date.now() - 5000;
        const filtered = prev.filter(msg => {
          if (msg.type === 'execution' && 
              (msg.content.includes('🚀 Started execution:') || 
               msg.content.includes('⏳ Preparing to execute crew...'))) {
            return msg.timestamp.getTime() < fiveSecondsAgo;
          }
          return true;
        });
        
        const errorMessage: ChatMessage = {
          id: `exec-error-${Date.now()}`,
          type: 'execution',
          content: `❌ ${message}`,
          timestamp: new Date(),
        };
        
        return [...filtered, errorMessage];
      });
    };

    const handleForceClearExecution = () => {
      console.log('[WorkflowChat] Force clearing execution state');
      setExecutingJobId(null);
      setExecutionStartTime(null);
      setProcessedTraceIds(new Set());
    };

    window.addEventListener('jobCreated', handleJobCreated as EventListener);
    window.addEventListener('jobCompleted', handleJobCompleted as EventListener);
    window.addEventListener('jobFailed', handleJobFailed as EventListener);
    window.addEventListener('traceUpdate', handleTraceUpdate as EventListener);
    window.addEventListener('executionError', handleExecutionError as EventListener);
    window.addEventListener('forceClearExecution', handleForceClearExecution);

    return () => {
      window.removeEventListener('jobCreated', handleJobCreated as EventListener);
      window.removeEventListener('jobCompleted', handleJobCompleted as EventListener);
      window.removeEventListener('jobFailed', handleJobFailed as EventListener);
      window.removeEventListener('traceUpdate', handleTraceUpdate as EventListener);
      window.removeEventListener('executionError', handleExecutionError as EventListener);
      window.removeEventListener('forceClearExecution', handleForceClearExecution);
    };
  }, [executingJobId, lastExecutionJobId, processedTraceIds, executionStartTime, saveMessageToBackend, sessionId, setMessages]);

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
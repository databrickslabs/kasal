import { useRef, useCallback, useEffect } from 'react';
import { streamExecution, StreamEvent } from '../api/streaming';

interface UseExecutionStreamOptions {
  onTrace: (message: string, data?: Record<string, unknown>) => void;
  onTaskOutput?: (taskName: string, output: string) => void;
  onStatusChange: (status: string, data: Record<string, unknown>) => void;
  onComplete: (result: Record<string, unknown>) => void;
  onError: (error: string) => void;
}

export function useExecutionStream(options: UseExecutionStreamOptions) {
  const closeRef = useRef<(() => void) | null>(null);
  const completedRef = useRef(false);
  const optionsRef = useRef(options);
  useEffect(() => {
    optionsRef.current = options;
  });

  const startStream = useCallback(
    (jobId: string) => {
      console.log('[useExecutionStream] startStream called for', jobId);
      if (closeRef.current) {
        closeRef.current();
      }
      completedRef.current = false;

      closeRef.current = streamExecution(
        jobId,
        (event: StreamEvent) => {
          const opts = optionsRef.current;
          // No per-event log here: this fires for EVERY streamed trace and
          // measurably slows trace-heavy runs with DevTools open. Lifecycle
          // transitions below keep their own logs.
          switch (event.event) {
            case 'connected':
              console.log('[useExecutionStream] Connected');
              opts.onStatusChange('connected', event.data);
              break;
            case 'execution_update': {
              const status = (event.data.status as string) || '';
              const statusLower = status.toLowerCase();
              console.log('[useExecutionStream] execution_update status:', status, 'has result:', !!event.data.result);
              opts.onStatusChange(status, event.data);
              if (statusLower === 'completed') {
                console.log('[useExecutionStream] COMPLETED — calling onComplete');
                completedRef.current = true;
                opts.onComplete(event.data);
                stopStream();
              } else if (statusLower === 'failed' || statusLower === 'stopped') {
                console.log('[useExecutionStream] FAILED/STOPPED — calling onError');
                completedRef.current = true;
                opts.onError(
                  (event.data.error as string) || `Execution ${status}`
                );
                stopStream();
              }
              break;
            }
            case 'trace': {
              const msg =
                (event.data.message as string) ||
                (event.data.trace as string) ||
                JSON.stringify(event.data);
              opts.onTrace(msg, event.data);

              const eventType = event.data.event_type as string;
              if (eventType === 'task_completed' && opts.onTaskOutput) {
                const metadata = event.data.trace_metadata as Record<string, unknown> | undefined;
                const taskName = (metadata?.task_name as string) || (event.data.event_context as string) || 'Task';
                const rawOutput = event.data.output || event.data.result || msg;
                const output = typeof rawOutput === 'string' ? rawOutput : JSON.stringify(rawOutput);
                opts.onTaskOutput(taskName, output);
              }
              break;
            }
            case 'error':
              console.log('[useExecutionStream] error event, completedRef:', completedRef.current);
              if (!completedRef.current) {
                opts.onError(
                  (event.data.message as string) || 'Unknown error'
                );
              }
              stopStream();
              break;
            default:
              console.log('[useExecutionStream] Unhandled event type:', event.event);
              break;
          }
        },
        () => {
          console.log('[useExecutionStream] Connection lost callback, completedRef:', completedRef.current);
          if (!completedRef.current) {
            optionsRef.current.onError('Connection lost');
          }
        }
      );
    },
    []
  );

  const stopStream = useCallback(() => {
    console.log('[useExecutionStream] stopStream called');
    if (closeRef.current) {
      closeRef.current();
      closeRef.current = null;
    }
  }, []);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (closeRef.current) {
        closeRef.current();
        closeRef.current = null;
      }
    };
  }, []);

  return { startStream, stopStream };
}

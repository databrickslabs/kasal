import { useRef, useCallback, useEffect } from 'react';
import { streamExecution, StreamEvent } from '../api/streaming';
import { extractA2uiSurface } from '../utils/resultExtraction';

/**
 * How long to keep a finished run's stream open waiting for its A2UI surface.
 *
 * The crew subprocess announces COMPLETED as soon as the crew has its answer;
 * the parent composes the surface afterwards and announces a second time.
 * Measured
 * gaps: 25s and 44s. 2 minutes leaves room without waiting on a run forever.
 */
const LATE_SURFACE_WAIT_MS = 120_000;

interface UseExecutionStreamOptions {
  onTrace: (message: string, data?: Record<string, unknown>) => void;
  onTaskOutput?: (taskName: string, output: string) => void;
  onStatusChange: (status: string, data: Record<string, unknown>) => void;
  onComplete: (result: Record<string, unknown>) => void;
  onError: (error: string) => void;
  /** Coalesced LLM token chunk (`llm_chunk` SSE event) for live typing. */
  onChunk?: (chunk: string, data: Record<string, unknown>) => void;
}

export function useExecutionStream(options: UseExecutionStreamOptions) {
  const closeRef = useRef<(() => void) | null>(null);
  const completedRef = useRef(false);
  // Armed when a run finishes WITHOUT a surface — see awaitLateSurface.
  const lateSurfaceTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
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
      if (lateSurfaceTimerRef.current) {
        clearTimeout(lateSurfaceTimerRef.current);
        lateSurfaceTimerRef.current = null;
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
                // A crew run announces COMPLETED TWICE on purpose: the plain
                // answer the moment the crew has it, then a second one carrying
                // the composed A2UI surface, which the parent can only build
                // after the subprocess exits (25-45s on measured decks).
                //
                // Closing on the first one left nothing for the second to
                // arrive on — the poller stops on the same event — so the deck
                // was composed, stored, and never seen: the chat kept the raw
                // markdown. Hold the stream open until that surface lands, or
                // until it is clearly not coming.
                if (extractA2uiSurface(event.data)) stopStream();
                else awaitLateSurface();
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
            case 'llm_chunk': {
              const chunk = (event.data.chunk as string) || '';
              if (chunk && opts.onChunk) {
                opts.onChunk(chunk, event.data);
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
    if (lateSurfaceTimerRef.current) {
      clearTimeout(lateSurfaceTimerRef.current);
      lateSurfaceTimerRef.current = null;
    }
    if (closeRef.current) {
      closeRef.current();
      closeRef.current = null;
    }
  }, []);

  /**
   * Keep the stream open a while longer, waiting for the composed surface.
   *
   * Bounded rather than open-ended: if composition fails or is skipped there is
   * no second announcement at all, and an un-timed wait would leak one idle
   * connection per run. The window is generous because composition is slow on a
   * local model — 44s measured for a 50-component deck — and an idle SSE
   * connection costs far less than a deck the user never sees.
   */
  const awaitLateSurface = useCallback(() => {
    if (lateSurfaceTimerRef.current) clearTimeout(lateSurfaceTimerRef.current);
    console.log('[useExecutionStream] holding stream open for the composed A2UI surface');
    lateSurfaceTimerRef.current = setTimeout(() => {
      lateSurfaceTimerRef.current = null;
      console.log('[useExecutionStream] no A2UI surface arrived — closing');
      stopStream();
    }, LATE_SURFACE_WAIT_MS);
  }, [stopStream]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (lateSurfaceTimerRef.current) {
        clearTimeout(lateSurfaceTimerRef.current);
        lateSurfaceTimerRef.current = null;
      }
      if (closeRef.current) {
        closeRef.current();
        closeRef.current = null;
      }
    };
  }, []);

  return { startStream, stopStream };
}

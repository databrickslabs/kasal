/**
 * Custom hook for Server-Sent Events (SSE) connections.
 *
 * Provides automatic reconnection, event handling, and cleanup.
 */

import { useEffect, useRef, useCallback, useState } from 'react';
import { config } from '../../config/api/ApiConfig';

export interface SSEOptions {
  /**
   * Maximum consecutive errors before giving up entirely.
   * Native EventSource auto-reconnects with Last-Event-ID on each drop,
   * so this should be high — Databricks Apps proxy drops ~75% of SSE
   * connections (known infra bug) and reconnection is expected.
   * @default 50
   */
  maxReconnectAttempts?: number;

  /**
   * Whether the hook is enabled (useful for conditional connections)
   * @default true
   */
  enabled?: boolean;

  /**
   * Callback when connection is established
   */
  onConnect?: () => void;

  /**
   * Callback when connection is lost
   */
  onDisconnect?: () => void;

  /**
   * Callback for errors
   */
  onError?: (error: Event) => void;
}

export interface SSEEvent<T = any> {
  data: T;
  event?: string;
  id?: string;
}

/**
 * Hook for establishing SSE connection to a specific endpoint
 */
export const useSSE = <T = any>(
  endpoint: string,
  onMessage: (event: SSEEvent<T>) => void,
  options: SSEOptions = {}
) => {
  const {
    maxReconnectAttempts = 50,
    enabled = true,
    onConnect,
    onDisconnect,
    onError,
  } = options;

  const eventSourceRef = useRef<EventSource | null>(null);
  const consecutiveErrorsRef = useRef(0);
  const [connectionState, setConnectionState] = useState<'connecting' | 'connected' | 'disconnected'>('disconnected');

  // CRITICAL: Use refs for callbacks to prevent useEffect from re-running
  // when callback functions change. This is the standard React pattern for
  // event handlers that shouldn't trigger effect re-runs.
  const onMessageRef = useRef(onMessage);
  const onConnectRef = useRef(onConnect);
  const onDisconnectRef = useRef(onDisconnect);
  const onErrorRef = useRef(onError);

  // Keep refs updated with latest callbacks
  useEffect(() => {
    onMessageRef.current = onMessage;
  }, [onMessage]);

  useEffect(() => {
    onConnectRef.current = onConnect;
  }, [onConnect]);

  useEffect(() => {
    onDisconnectRef.current = onDisconnect;
  }, [onDisconnect]);

  useEffect(() => {
    onErrorRef.current = onError;
  }, [onError]);

  const connect = useCallback(() => {
    if (!enabled) return;

    // Clean up existing connection
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
    }

    const url = endpoint.startsWith('http') ? endpoint : `${config.apiUrl}${endpoint}`;
    const t0 = Date.now();

    console.log(`[SSE] ${new Date().toISOString()} | CONNECT  | ${endpoint} | url=${url}`);
    setConnectionState('connecting');
    consecutiveErrorsRef.current = 0;

    const eventSource = new EventSource(url);
    eventSourceRef.current = eventSource;

    // --- onopen ---
    eventSource.onopen = () => {
      const dt = Date.now() - t0;
      console.log(
        `[SSE] ${new Date().toISOString()} | OPEN     | ${endpoint} | ` +
        `readyState=${eventSource.readyState} | took ${dt}ms`
      );
      setConnectionState('connected');
      consecutiveErrorsRef.current = 0;
      onConnectRef.current?.();
    };

    // --- onmessage (unnamed events) ---
    eventSource.onmessage = (event) => {
      console.log(
        `[SSE] ${new Date().toISOString()} | MESSAGE  | ${endpoint} | ` +
        `id=${event.lastEventId} | type=${(event as any).type} | ` +
        `data=${event.data?.substring(0, 120)}`
      );
      try {
        const data = JSON.parse(event.data);
        onMessageRef.current({
          data,
          event: (event as any).type,
          id: event.lastEventId
        });
      } catch (error) {
        console.error('[SSE] Error parsing message:', error);
      }
    };

    // --- onerror ---
    // Do NOT call eventSource.close() here — let the browser's native
    // EventSource reconnection handle retry with Last-Event-ID.
    eventSource.onerror = () => {
      consecutiveErrorsRef.current += 1;
      const attempt = consecutiveErrorsRef.current;
      const dt = Date.now() - t0;

      console.warn(
        `[SSE] ${new Date().toISOString()} | ERROR #${attempt} | ${endpoint} | ` +
        `readyState=${eventSource.readyState} ` +
        `(${eventSource.readyState === 0 ? 'CONNECTING' : eventSource.readyState === 1 ? 'OPEN' : 'CLOSED'}) | ` +
        `elapsed=${dt}ms since connect`
      );

      setConnectionState(
        eventSource.readyState === EventSource.CONNECTING ? 'connecting' : 'disconnected'
      );

      onDisconnectRef.current?.();

      // Only give up after many consecutive failures
      if (attempt >= maxReconnectAttempts) {
        console.error(
          `[SSE] ${new Date().toISOString()} | GIVE UP  | ${endpoint} | ` +
          `${maxReconnectAttempts} consecutive errors`
        );
        eventSource.close();
        eventSourceRef.current = null;
        setConnectionState('disconnected');
      }
    };

    // --- Named event listeners (with logging) ---
    const namedEvents = ['execution_update', 'trace', 'hitl_request', 'connected'] as const;
    for (const eventType of namedEvents) {
      eventSource.addEventListener(eventType, (event: any) => {
        console.log(
          `[SSE] ${new Date().toISOString()} | EVENT    | ${endpoint} | ` +
          `type=${eventType} | id=${event.lastEventId} | ` +
          `data=${event.data?.substring(0, 120)}`
        );
        try {
          const data = JSON.parse(event.data);
          if (eventType === 'connected') {
            // Just log, don't dispatch
            return;
          }
          onMessageRef.current({
            data,
            event: eventType,
            id: event.lastEventId
          });
        } catch (error) {
          console.error(`[SSE] Error parsing ${eventType}:`, error);
        }
      });
    }

  }, [endpoint, enabled, maxReconnectAttempts]);

  const disconnect = useCallback(() => {
    console.log(`[SSE] Disconnecting from: ${endpoint}`);

    // Close connection
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }

    setConnectionState('disconnected');
  }, [endpoint]);

  // Connect on mount or when endpoint/enabled changes
  useEffect(() => {
    if (enabled && endpoint) {
      connect();
    }

    return () => {
      disconnect();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [endpoint, enabled]);

  return {
    connectionState,
    reconnect: connect,
    disconnect,
  };
};

/**
 * Hook for SSE connection to execution updates
 */
export const useExecutionSSE = (
  jobId: string | null,
  onUpdate: (data: any) => void,
  options?: SSEOptions
) => {
  const endpoint = jobId ? `/sse/executions/${jobId}/stream` : '';

  return useSSE(
    endpoint,
    (event) => {
      if (event.event === 'execution_update' || event.event === 'trace') {
        // Pass the full event object so callback can access event.event type
        onUpdate(event);
      }
    },
    {
      ...options,
      enabled: !!jobId && (options?.enabled !== false),
    }
  );
};

/**
 * Hook for SSE connection to all executions (global stream)
 *
 * This stream broadcasts ALL job status updates to ALL browsers,
 * ensuring synchronization across multiple browser windows/tabs.
 */
export const useGlobalExecutionSSE = (
  onUpdate: (data: any) => void,
  options?: SSEOptions
) => {
  const endpoint = '/sse/executions/stream-all';

  return useSSE(
    endpoint,
    (event) => {
      // Pass all events through to the consumer (GlobalSSEConnection).
      // The component is responsible for dispatching window events and
      // routing data to the store — this hook has no side effects.
      onUpdate(event);
    },
    {
      ...options,
      enabled: options?.enabled !== false,
    }
  );
};

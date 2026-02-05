/**
 * Custom hook for Server-Sent Events (SSE) connections.
 *
 * Provides automatic reconnection, event handling, and cleanup.
 */

import { useEffect, useRef, useCallback, useState } from 'react';
import { config } from '../../config/api/ApiConfig';

export interface SSEOptions {
  /**
   * Whether to automatically reconnect on connection loss
   * @default true
   */
  autoReconnect?: boolean;

  /**
   * Maximum number of reconnection attempts
   * @default 5
   */
  maxReconnectAttempts?: number;

  /**
   * Base delay for reconnection (will use exponential backoff)
   * @default 1000
   */
  reconnectDelay?: number;

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
    autoReconnect = true,
    maxReconnectAttempts = 5,
    reconnectDelay = 1000,
    enabled = true,
    onConnect,
    onDisconnect,
    onError,
  } = options;

  const eventSourceRef = useRef<EventSource | null>(null);
  const reconnectAttemptsRef = useRef(0);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null);
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

    console.log(`[SSE] Connecting to: ${url}`);
    setConnectionState('connecting');

    const eventSource = new EventSource(url);
    eventSourceRef.current = eventSource;

    eventSource.onopen = () => {
      console.log(`[SSE] Connected to: ${endpoint}`);
      setConnectionState('connected');
      reconnectAttemptsRef.current = 0; // Reset reconnect counter on successful connection
      onConnectRef.current?.();
    };

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        onMessageRef.current({
          data,
          event: (event as any).type,
          id: event.lastEventId
        });
      } catch (error) {
        console.error('[SSE] Error parsing message:', error);
        // Pass parsing errors to onError callback with context
        const parseError = new Event('error');
        (parseError as any).message = 'Failed to parse SSE message';
        (parseError as any).originalError = error;
        onErrorRef.current?.(parseError);
      }
    };

    eventSource.onerror = (error) => {
      console.error(`[SSE] Connection error for ${endpoint}:`, error);
      setConnectionState('disconnected');

      // Create enhanced error with context
      const enhancedError = error as Event;
      (enhancedError as any).endpoint = endpoint;
      (enhancedError as any).reconnectAttempt = reconnectAttemptsRef.current + 1;
      (enhancedError as any).maxAttempts = maxReconnectAttempts;

      onErrorRef.current?.(enhancedError);
      onDisconnectRef.current?.();

      // Attempt to reconnect
      if (
        autoReconnect &&
        reconnectAttemptsRef.current < maxReconnectAttempts &&
        eventSource.readyState !== EventSource.CONNECTING
      ) {
        reconnectAttemptsRef.current += 1;
        const delay = reconnectDelay * Math.pow(2, reconnectAttemptsRef.current - 1);

        console.log(
          `[SSE] Reconnect attempt ${reconnectAttemptsRef.current}/${maxReconnectAttempts} in ${delay}ms`
        );

        reconnectTimeoutRef.current = setTimeout(() => {
          connect();
        }, delay);
      } else if (reconnectAttemptsRef.current >= maxReconnectAttempts) {
        console.error(`[SSE] Max reconnect attempts (${maxReconnectAttempts}) reached for ${endpoint}`);
        // Notify that max attempts reached
        const maxAttemptsError = new Event('error');
        (maxAttemptsError as any).message = 'Max reconnection attempts reached';
        (maxAttemptsError as any).endpoint = endpoint;
        (maxAttemptsError as any).isFatal = true;
        onErrorRef.current?.(maxAttemptsError);
      }

      // Close the errored connection
      eventSource.close();
    };

    // Add custom event listeners for specific event types
    eventSource.addEventListener('execution_update', (event: any) => {
      try {
        const data = JSON.parse(event.data);
        onMessageRef.current({
          data,
          event: 'execution_update',
          id: event.lastEventId
        });
      } catch (error) {
        console.error('[SSE] Error parsing execution_update:', error);
      }
    });

    eventSource.addEventListener('trace', (event: any) => {
      try {
        const data = JSON.parse(event.data);
        onMessageRef.current({
          data,
          event: 'trace',
          id: event.lastEventId
        });
      } catch (error) {
        console.error('[SSE] Error parsing trace:', error);
      }
    });

    eventSource.addEventListener('hitl_request', (event: any) => {
      try {
        const data = JSON.parse(event.data);
        onMessageRef.current({
          data,
          event: 'hitl_request',
          id: event.lastEventId
        });
      } catch (error) {
        console.error('[SSE] Error parsing hitl_request:', error);
      }
    });

    eventSource.addEventListener('connected', (event: any) => {
      try {
        const data = JSON.parse(event.data);
        console.log('[SSE] Connection confirmed:', data);
      } catch (error) {
        console.error('[SSE] Error parsing connected event:', error);
      }
    });

  // CRITICAL: Only include stable values in dependencies, NOT callbacks
  // Callbacks are accessed via refs to prevent reconnection cycles
  }, [endpoint, enabled, autoReconnect, maxReconnectAttempts, reconnectDelay]);

  const disconnect = useCallback(() => {
    console.log(`[SSE] Disconnecting from: ${endpoint}`);

    // Clear reconnect timeout
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }

    // Close connection
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }

    setConnectionState('disconnected');
  }, [endpoint]);

  // Connect on mount or when endpoint/enabled changes
  // CRITICAL: Only depend on endpoint and enabled, NOT on connect/disconnect
  // since those functions now use refs and don't need to trigger reconnects
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
      // Handle all event types from the global stream
      if (event.event === 'execution_update') {
        onUpdate(event);
      } else if (event.event === 'trace') {
        // CRITICAL: Pass trace events to onUpdate so GlobalSSEConnection can add to store
        onUpdate(event);
        // Also dispatch window event for backwards compatibility with useExecutionMonitoring
        if (event.data?.job_id) {
          window.dispatchEvent(new CustomEvent('traceUpdate', {
            detail: { jobId: event.data.job_id, trace: event.data }
          }));
        }
      } else if (event.event === 'hitl_request') {
        // Dispatch HITL request event
        if (event.data?.job_id) {
          window.dispatchEvent(new CustomEvent('hitlRequest', {
            detail: event.data
          }));
        }
      } else {
        // Handle generic data messages
        onUpdate(event);
      }
    },
    {
      ...options,
      enabled: options?.enabled !== false,
    }
  );
};

import { getBaseUrl } from './client';

export interface StreamEvent {
  event: string;
  data: Record<string, unknown>;
}

type EventCallback = (event: StreamEvent) => void;

/**
 * Build the SSE URL.
 *
 * In development mode, connect directly to the backend (port 8000) to avoid
 * Vite proxy buffering issues with Server-Sent Events. In production, use the
 * relative URL which goes through the app gateway.
 */
function buildSseUrl(path: string): string {
  const baseUrl = getBaseUrl();
  // Strip trailing /api/v1 so we can append /api/v1/sse/...
  const root = baseUrl.replace(/\/api\/v1\/?$/, '');
  const url = `${root}/api/v1${path}`;

  // If the base URL is already absolute, use it directly
  if (url.startsWith('http')) {
    return url;
  }

  // In dev mode, connect directly to the backend to avoid proxy issues with SSE
  if (import.meta.env.DEV) {
    return `http://localhost:8000/api/v1${path}`;
  }

  // In production, resolve against the current origin
  return `${window.location.origin}${url}`;
}

const MAX_RECONNECT_ATTEMPTS = 5;
const RECONNECT_BASE_DELAY = 1000; // 1 second

export function streamExecution(
  jobId: string,
  onEvent: EventCallback,
  onError?: (error: Event) => void
): () => void {
  let closed = false;
  let eventSource: EventSource | null = null;
  let reconnectAttempts = 0;
  let reconnectTimeout: ReturnType<typeof setTimeout> | null = null;

  function connect() {
    if (closed) return;

    const sseUrl = buildSseUrl(`/sse/executions/${jobId}/stream`);
    console.log('[SSE] Connecting to execution stream:', sseUrl, `(attempt ${reconnectAttempts + 1})`);
    eventSource = new EventSource(sseUrl);

    const eventTypes = [
      'execution_update',
      'trace',
      'hitl_request',
      'error',
      'connected',
    ];

    for (const type of eventTypes) {
      eventSource.addEventListener(type, (e: Event) => {
        const me = e as MessageEvent;
        console.log(`[SSE] Event received: "${type}", raw data:`, me.data?.slice?.(0, 300) || me.data);
        try {
          const data = JSON.parse(me.data);
          onEvent({ event: type, data });
        } catch {
          onEvent({ event: type, data: { message: me.data } });
        }
      });
    }

    eventSource.onopen = () => {
      console.log('[SSE] Connection established for', jobId);
      reconnectAttempts = 0; // Reset on successful connection
    };

    eventSource.onmessage = (e: MessageEvent) => {
      console.log('[SSE] Generic message:', e.data?.slice?.(0, 300) || e.data);
      try {
        const data = JSON.parse(e.data);
        onEvent({ event: 'message', data });
      } catch {
        onEvent({ event: 'message', data: { message: e.data } });
      }
    };

    eventSource.onerror = (e) => {
      console.log('[SSE] EventSource error, readyState:', eventSource?.readyState, e);

      // Close the current errored connection
      if (eventSource) {
        eventSource.close();
        eventSource = null;
      }

      if (closed) return;

      // Attempt reconnection with exponential backoff
      if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
        reconnectAttempts++;
        const delay = RECONNECT_BASE_DELAY * Math.pow(2, reconnectAttempts - 1);
        console.log(`[SSE] Reconnect attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS} in ${delay}ms`);
        reconnectTimeout = setTimeout(connect, delay);
      } else {
        console.log('[SSE] Max reconnect attempts reached, giving up SSE');
        if (onError) onError(e);
      }
    };
  }

  connect();

  return () => {
    console.log('[SSE] Closing execution stream for', jobId);
    closed = true;
    if (reconnectTimeout) {
      clearTimeout(reconnectTimeout);
      reconnectTimeout = null;
    }
    if (eventSource) {
      eventSource.close();
      eventSource = null;
    }
  };
}

export function streamGeneration(
  generationId: string,
  onEvent: EventCallback,
  onError?: (error: Event) => void
): () => void {
  let closed = false;
  let eventSource: EventSource | null = null;
  let reconnectAttempts = 0;
  let reconnectTimeout: ReturnType<typeof setTimeout> | null = null;

  function connect() {
    if (closed) return;

    const sseUrl = buildSseUrl(`/sse/generations/${generationId}/stream`);
    console.log('[SSE] Connecting to generation stream:', sseUrl, `(attempt ${reconnectAttempts + 1})`);
    eventSource = new EventSource(sseUrl);

    const eventTypes = [
      'plan_ready',
      'agent_detail',
      'task_detail',
      'entity_error',
      'generation_complete',
      'generation_failed',
      'connected',
    ];

    for (const type of eventTypes) {
      eventSource.addEventListener(type, (e: Event) => {
        const me = e as MessageEvent;
        console.log(`[SSE] Generation event: "${type}", raw data:`, me.data?.slice?.(0, 500) || me.data);
        try {
          const data = JSON.parse(me.data);
          onEvent({ event: type, data });
        } catch (err) {
          console.error(`[SSE] JSON parse failed for "${type}":`, err, 'raw:', me.data?.slice?.(0, 200));
          onEvent({ event: type, data: { message: me.data } });
        }
      });
    }

    eventSource.onopen = () => {
      console.log('[SSE] Generation connection established for', generationId);
      reconnectAttempts = 0;
    };

    eventSource.onmessage = (e: MessageEvent) => {
      console.log('[SSE] Generation generic message:', e.data?.slice?.(0, 300) || e.data);
      try {
        const data = JSON.parse(e.data);
        onEvent({ event: 'message', data });
      } catch {
        onEvent({ event: 'message', data: { message: e.data } });
      }
    };

    eventSource.onerror = (e) => {
      console.log('[SSE] Generation EventSource error, readyState:', eventSource?.readyState, e);

      if (eventSource) {
        eventSource.close();
        eventSource = null;
      }

      if (closed) return;

      if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
        reconnectAttempts++;
        const delay = RECONNECT_BASE_DELAY * Math.pow(2, reconnectAttempts - 1);
        console.log(`[SSE] Generation reconnect attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS} in ${delay}ms`);
        reconnectTimeout = setTimeout(connect, delay);
      } else {
        console.log('[SSE] Generation max reconnect attempts reached');
        if (onError) onError(e);
      }
    };
  }

  connect();

  return () => {
    console.log('[SSE] Closing generation stream for', generationId);
    closed = true;
    if (reconnectTimeout) {
      clearTimeout(reconnectTimeout);
      reconnectTimeout = null;
    }
    if (eventSource) {
      eventSource.close();
      eventSource = null;
    }
  };
}

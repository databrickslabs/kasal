import { getBaseUrl, getClient } from './client';
import { SSE_ENABLED } from '../../../utils/sseTransport';

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
  // SSE is dev-only. On Databricks Apps the HTTP/2 proxy refuses/drops the
  // stream, so we don't open it — the REST polling fallback drives updates.
  if (!SSE_ENABLED) return () => {};
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

    // 'error' is special and NOT in the loop above: EventSource fires the
    // 'error' listener for BOTH a server-sent `event: error` frame AND a native
    // TRANSPORT error (a connection drop), the latter a bare Event with no
    // `.data`. Forwarding that dataless transport error as an application error
    // made a transient SSE blip surface "Execution failed: Unknown error" while
    // the backend job kept running — especially right after a refresh, where
    // reconnecting re-opens the stream. Reconnection is owned by onerror below,
    // so here we forward ONLY a genuine server-sent error frame (one with data).
    eventSource.addEventListener('error', (e: Event) => {
      const me = e as MessageEvent;
      if (me.data == null) return; // transport error — let onerror reconnect
      console.log('[SSE] Event received: "error", raw data:', me.data?.slice?.(0, 300) || me.data);
      try {
        onEvent({ event: 'error', data: JSON.parse(me.data) });
      } catch {
        onEvent({ event: 'error', data: { message: me.data } });
      }
    });

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

// Safety-net polling for the generation terminal event. The chat fast path
// completes in <1s and the Databricks Apps HTTP/2 proxy drops the first SSE
// connect of a page often enough that the client can miss the sole
// `generation_complete` event (which carries the execution_id), orphaning a
// run that actually finished. We poll the non-streaming result endpoint as a
// backstop. It reads the same replay buffer the stream replays from, so the
// terminal outcome is recoverable over plain HTTP even if SSE never connects.
const POLL_START_DELAY = 2000; // give SSE a head start before polling
const POLL_INTERVAL = 2000;
const POLL_MAX_ATTEMPTS = 90; // ~3 min backstop, matches stream timeout budget

export function streamGeneration(
  generationId: string,
  onEvent: EventCallback,
  onError?: (error: Event) => void
): () => void {
  let closed = false;
  let eventSource: EventSource | null = null;
  let reconnectAttempts = 0;
  let reconnectTimeout: ReturnType<typeof setTimeout> | null = null;

  // Polling state
  let terminalReceived = false;
  let pollAttempts = 0;
  let pollTimer: ReturnType<typeof setTimeout> | null = null;
  let pollStartTimer: ReturnType<typeof setTimeout> | null = null;
  let pollInFlight = false;

  function stopPolling() {
    if (pollStartTimer) {
      clearTimeout(pollStartTimer);
      pollStartTimer = null;
    }
    if (pollTimer) {
      clearTimeout(pollTimer);
      pollTimer = null;
    }
  }

  // Forward an event to the consumer, tracking whether the terminal event has
  // been delivered. Once it has, we tear down both the stream and the poll —
  // the consumer's own dedup (st.completed) makes a duplicate harmless if both
  // the stream and a poll race to deliver it.
  function forward(event: StreamEvent) {
    if (event.event === 'generation_complete' || event.event === 'generation_failed') {
      terminalReceived = true;
      stopPolling();
    }
    onEvent(event);
  }

  async function pollOnce() {
    if (closed || terminalReceived || pollInFlight) return;
    pollInFlight = true;
    try {
      const resp = await getClient().get(`/sse/generations/${generationId}/result`);
      const data = (resp.data || {}) as Record<string, unknown>;
      const status = data.status as string | undefined;
      if (status && status !== 'pending') {
        const eventName = status === 'failed' ? 'generation_failed' : 'generation_complete';
        console.log(`[SSE] Generation recovered via result poll: ${eventName}`, generationId);
        forward({ event: eventName, data });
        return;
      }
    } catch (err) {
      // Endpoint unreachable / transient — keep polling until the cap.
      console.log('[SSE] Generation result poll failed (will retry):', err);
    } finally {
      pollInFlight = false;
    }

    if (closed || terminalReceived) return;
    if (pollAttempts < POLL_MAX_ATTEMPTS) {
      pollAttempts++;
      pollTimer = setTimeout(pollOnce, POLL_INTERVAL);
    } else {
      console.log('[SSE] Generation result poll gave up after max attempts', generationId);
    }
  }

  function ensurePolling(immediate = false) {
    if (closed || terminalReceived) return;
    if (immediate) {
      // A transport error means the stream may never deliver — poll now,
      // cancelling the delayed safety-net start. No-op if a poll is already
      // in flight or a poll loop is already scheduled.
      if (pollStartTimer) {
        clearTimeout(pollStartTimer);
        pollStartTimer = null;
      }
      if (pollTimer || pollInFlight) return;
      pollOnce();
      return;
    }
    if (pollTimer || pollStartTimer || pollInFlight) return;
    pollStartTimer = setTimeout(() => {
      pollStartTimer = null;
      pollOnce();
    }, POLL_START_DELAY);
  }

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
      // ChatMode auto-execute folds the backend run's execution_id INTO this
      // single terminal event (no separate execution_started), so the stream
      // closes on generation_complete — the result-endpoint poll backstops a
      // miss.
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
          forward({ event: type, data });
        } catch (err) {
          console.error(`[SSE] JSON parse failed for "${type}":`, err, 'raw:', me.data?.slice?.(0, 200));
          forward({ event: type, data: { message: me.data } });
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
        forward({ event: 'message', data });
      } catch {
        forward({ event: 'message', data: { message: e.data } });
      }
    };

    eventSource.onerror = (e) => {
      console.log('[SSE] Generation EventSource error, readyState:', eventSource?.readyState, e);

      if (eventSource) {
        eventSource.close();
        eventSource = null;
      }

      if (closed || terminalReceived) return;

      // A transport error means the stream may never deliver the terminal
      // event — start the result poll right away rather than waiting.
      ensurePolling(true);

      if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS) {
        reconnectAttempts++;
        const delay = RECONNECT_BASE_DELAY * Math.pow(2, reconnectAttempts - 1);
        console.log(`[SSE] Generation reconnect attempt ${reconnectAttempts}/${MAX_RECONNECT_ATTEMPTS} in ${delay}ms`);
        reconnectTimeout = setTimeout(connect, delay);
      } else {
        console.log('[SSE] Generation max reconnect attempts reached; relying on result poll');
        if (onError) onError(e);
      }
    };
  }

  if (SSE_ENABLED) {
    connect();
    // Backstop even when the stream "succeeds" but stalls (proxy buffering can
    // swallow the terminal frame). No-op if the stream delivers the terminal
    // event before POLL_START_DELAY elapses.
    ensurePolling(false);
  } else {
    // Deployed (Databricks Apps): the HTTP/2 proxy refuses EventSource, so
    // every dispatch used to burn up to 6 doomed connect attempts (1→16s
    // backoff) while this result poll did the real work anyway. Poll-only —
    // and start immediately, since there's no SSE to give a head start to.
    ensurePolling(true);
  }

  return () => {
    console.log('[SSE] Closing generation stream for', generationId);
    closed = true;
    stopPolling();
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

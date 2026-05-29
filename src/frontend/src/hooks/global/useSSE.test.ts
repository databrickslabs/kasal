/**
 * Unit tests for useSSE hook.
 *
 * Tests the SSE connection functionality including connection management,
 * event handling, reconnection logic, and cleanup.
 */
import { renderHook, act, waitFor } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';

import { useSSE, useExecutionSSE, useGlobalExecutionSSE } from './useSSE';

// Mock EventSource
class MockEventSource {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSED = 2;

  url: string;
  readyState: number = MockEventSource.CONNECTING;
  onopen: ((ev: Event) => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  eventListeners: Map<string, ((ev: any) => void)[]> = new Map();

  constructor(url: string) {
    this.url = url;
    // Simulate async connection
    setTimeout(() => {
      this.readyState = MockEventSource.OPEN;
      if (this.onopen) {
        this.onopen(new Event('open'));
      }
    }, 0);
  }

  addEventListener(type: string, callback: (ev: any) => void) {
    if (!this.eventListeners.has(type)) {
      this.eventListeners.set(type, []);
    }
    this.eventListeners.get(type)!.push(callback);
  }

  removeEventListener(type: string, callback: (ev: any) => void) {
    const listeners = this.eventListeners.get(type);
    if (listeners) {
      const index = listeners.indexOf(callback);
      if (index > -1) {
        listeners.splice(index, 1);
      }
    }
  }

  dispatchEvent(event: Event): boolean {
    const listeners = this.eventListeners.get(event.type);
    if (listeners) {
      listeners.forEach((cb) => cb(event));
    }
    return true;
  }

  close() {
    this.readyState = MockEventSource.CLOSED;
  }

  // Helper to simulate receiving a message
  simulateMessage(data: any, eventType?: string) {
    const event = {
      data: JSON.stringify(data),
      type: eventType || 'message',
      lastEventId: 'test-id',
    };

    if (eventType && this.eventListeners.has(eventType)) {
      this.eventListeners.get(eventType)!.forEach((cb) => cb(event));
    } else if (this.onmessage) {
      this.onmessage(event as MessageEvent);
    }
  }

  // Helper to simulate error
  simulateError() {
    this.readyState = MockEventSource.CLOSED;
    if (this.onerror) {
      this.onerror(new Event('error'));
    }
  }
}

// Store instance references for tests
let mockEventSourceInstances: MockEventSource[] = [];

// Create a trackable MockEventSource class that stores instances
class TrackableMockEventSource extends MockEventSource {
  constructor(url: string) {
    super(url);
    mockEventSourceInstances.push(this);
  }
}

// Mock config
vi.mock('../../config/api/ApiConfig', () => ({
  config: {
    apiUrl: 'http://localhost:8000/api/v1',
  },
}));

describe('useSSE', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockEventSourceInstances = [];
    (global as any).EventSource = TrackableMockEventSource;
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
    delete (global as any).EventSource;
  });

  // Helper to get the latest mock instance
  const getLatestMockInstance = () => mockEventSourceInstances[mockEventSourceInstances.length - 1];

  describe('Connection Management', () => {
    it('creates EventSource with correct URL', async () => {
      const onMessage = vi.fn();

      renderHook(() => useSSE('/test/endpoint', onMessage));

      await vi.advanceTimersByTimeAsync(100);

      expect(mockEventSourceInstances.length).toBe(1);
      expect(getLatestMockInstance()?.url).toBe(
        'http://localhost:8000/api/v1/test/endpoint'
      );
    });

    it('uses full URL when endpoint starts with http', async () => {
      const onMessage = vi.fn();

      renderHook(() =>
        useSSE('http://custom.server.com/endpoint', onMessage)
      );

      await vi.advanceTimersByTimeAsync(100);

      expect(mockEventSourceInstances.length).toBe(1);
      expect(getLatestMockInstance()?.url).toBe(
        'http://custom.server.com/endpoint'
      );
    });

    it('does not create connection when disabled', async () => {
      const onMessage = vi.fn();

      renderHook(() =>
        useSSE('/test/endpoint', onMessage, { enabled: false })
      );

      await vi.advanceTimersByTimeAsync(100);

      expect(mockEventSourceInstances.length).toBe(0);
    });

    it('closes connection on unmount', async () => {
      const onMessage = vi.fn();

      const { unmount } = renderHook(() =>
        useSSE('/test/endpoint', onMessage)
      );

      await vi.advanceTimersByTimeAsync(100);

      const closeSpy = vi.spyOn(getLatestMockInstance()!, 'close');

      unmount();

      expect(closeSpy).toHaveBeenCalled();
    });

    it('returns correct connection state', async () => {
      const onMessage = vi.fn();

      const { result } = renderHook(() =>
        useSSE('/test/endpoint', onMessage)
      );

      // Initially connecting
      expect(result.current.connectionState).toBe('connecting');

      // Wait for connection
      await vi.advanceTimersByTimeAsync(100);

      expect(result.current.connectionState).toBe('connected');
    });
  });

  describe('Message Handling', () => {
    it('calls onMessage when message received', async () => {
      const onMessage = vi.fn();

      renderHook(() => useSSE('/test/endpoint', onMessage));

      await vi.advanceTimersByTimeAsync(100);

      // Simulate receiving a message
      act(() => {
        getLatestMockInstance()!.simulateMessage({ status: 'running' });
      });

      expect(onMessage).toHaveBeenCalledWith({
        data: { status: 'running' },
        event: 'message',
        id: 'test-id',
      });
    });

    it('handles execution_update events', async () => {
      const onMessage = vi.fn();

      renderHook(() => useSSE('/test/endpoint', onMessage));

      await vi.advanceTimersByTimeAsync(100);

      // Simulate execution_update event
      act(() => {
        getLatestMockInstance()!.simulateMessage(
          { job_id: 'job-123', status: 'completed' },
          'execution_update'
        );
      });

      expect(onMessage).toHaveBeenCalledWith({
        data: { job_id: 'job-123', status: 'completed' },
        event: 'execution_update',
        id: 'test-id',
      });
    });

    it('handles trace events', async () => {
      const onMessage = vi.fn();

      renderHook(() => useSSE('/test/endpoint', onMessage));

      await vi.advanceTimersByTimeAsync(100);

      // Simulate trace event
      act(() => {
        getLatestMockInstance()!.simulateMessage(
          { id: 1, output: 'Task started' },
          'trace'
        );
      });

      expect(onMessage).toHaveBeenCalledWith({
        data: { id: 1, output: 'Task started' },
        event: 'trace',
        id: 'test-id',
      });
    });

    it('handles hitl_request events', async () => {
      const onMessage = vi.fn();

      renderHook(() => useSSE('/test/endpoint', onMessage));

      await vi.advanceTimersByTimeAsync(100);

      // Simulate HITL request event
      act(() => {
        getLatestMockInstance()!.simulateMessage(
          { approval_id: 1, message: 'Approval needed' },
          'hitl_request'
        );
      });

      expect(onMessage).toHaveBeenCalledWith({
        data: { approval_id: 1, message: 'Approval needed' },
        event: 'hitl_request',
        id: 'test-id',
      });
    });
  });

  describe('Callback Handling', () => {
    it('calls onConnect when connection established', async () => {
      const onMessage = vi.fn();
      const onConnect = vi.fn();

      renderHook(() =>
        useSSE('/test/endpoint', onMessage, { onConnect })
      );

      await vi.advanceTimersByTimeAsync(100);

      expect(onConnect).toHaveBeenCalled();
    });

    it('calls onDisconnect when connection lost', async () => {
      const onMessage = vi.fn();
      const onDisconnect = vi.fn();

      renderHook(() =>
        useSSE('/test/endpoint', onMessage, { onDisconnect })
      );

      await vi.advanceTimersByTimeAsync(100);

      // Simulate error
      act(() => {
        getLatestMockInstance()!.simulateError();
      });

      expect(onDisconnect).toHaveBeenCalled();
    });

    it('calls onError when error occurs', async () => {
      const onMessage = vi.fn();
      const onError = vi.fn();

      renderHook(() =>
        useSSE('/test/endpoint', onMessage, { onError })
      );

      await vi.advanceTimersByTimeAsync(100);

      // Simulate error
      act(() => {
        getLatestMockInstance()!.simulateError();
      });

      expect(onError).toHaveBeenCalled();
    });
  });

  // The hook delegates reconnection to the browser's native EventSource (it does
  // NOT manually create new connections or use timer-based backoff). It only
  // tracks consecutive errors and gives up — closing the connection — once
  // maxReconnectAttempts is exceeded.
  describe('Reconnection Logic', () => {
    it('does not manually recreate the EventSource on a transient error', async () => {
      const onMessage = vi.fn();

      renderHook(() =>
        useSSE('/test/endpoint', onMessage, { maxReconnectAttempts: 3 })
      );

      await vi.advanceTimersByTimeAsync(100);
      expect(mockEventSourceInstances.length).toBe(1);

      // A single transient error must not spin up a new EventSource — native
      // reconnection on the existing instance handles that.
      act(() => {
        getLatestMockInstance()!.simulateError();
      });
      await vi.advanceTimersByTimeAsync(5000);

      expect(mockEventSourceInstances.length).toBe(1);
    });

    it('does not close the connection before max attempts is reached', async () => {
      const onMessage = vi.fn();

      renderHook(() =>
        useSSE('/test/endpoint', onMessage, { maxReconnectAttempts: 3 })
      );

      await vi.advanceTimersByTimeAsync(100);
      const instance = getLatestMockInstance()!;
      const closeSpy = vi.spyOn(instance, 'close');

      // Two consecutive errors (< maxReconnectAttempts of 3) — should keep trying.
      act(() => {
        instance.simulateError();
        instance.simulateError();
      });

      expect(closeSpy).not.toHaveBeenCalled();
    });

    it('gives up and closes the connection after max consecutive errors', async () => {
      const onMessage = vi.fn();
      const onError = vi.fn();

      renderHook(() =>
        useSSE('/test/endpoint', onMessage, {
          maxReconnectAttempts: 2,
          onError,
        })
      );

      await vi.advanceTimersByTimeAsync(100);
      const instance = getLatestMockInstance()!;
      const closeSpy = vi.spyOn(instance, 'close');

      // Two consecutive errors reaches maxReconnectAttempts → give up.
      act(() => {
        instance.simulateError();
        instance.simulateError();
      });

      expect(closeSpy).toHaveBeenCalled();
      // The final, fatal error is reported to the consumer.
      expect(onError).toHaveBeenCalledWith(
        expect.objectContaining({ isFatal: true })
      );
    });

    it('resets the consecutive-error counter on successful (re)connection', async () => {
      const onMessage = vi.fn();
      const onError = vi.fn();

      renderHook(() =>
        useSSE('/test/endpoint', onMessage, {
          maxReconnectAttempts: 2,
          onError,
        })
      );

      await vi.advanceTimersByTimeAsync(100);
      const instance = getLatestMockInstance()!;

      // One error (counter = 1, below the limit of 2).
      act(() => {
        instance.simulateError();
      });

      // Native EventSource recovers: fire onopen again to reset the counter.
      act(() => {
        instance.readyState = MockEventSource.OPEN;
        instance.onopen?.(new Event('open'));
      });

      // A single subsequent error must not be treated as fatal, because the
      // counter was reset on the successful reconnection.
      act(() => {
        instance.simulateError();
      });

      expect(onError).not.toHaveBeenCalledWith(
        expect.objectContaining({ isFatal: true })
      );
    });
  });

  describe('Manual Connection Control', () => {
    it('provides reconnect function', async () => {
      const onMessage = vi.fn();

      const { result } = renderHook(() =>
        useSSE('/test/endpoint', onMessage)
      );

      expect(result.current.reconnect).toBeDefined();
      expect(typeof result.current.reconnect).toBe('function');
    });

    it('provides disconnect function', async () => {
      const onMessage = vi.fn();

      const { result } = renderHook(() =>
        useSSE('/test/endpoint', onMessage)
      );

      expect(result.current.disconnect).toBeDefined();
      expect(typeof result.current.disconnect).toBe('function');
    });

    it('disconnect closes connection', async () => {
      const onMessage = vi.fn();

      const { result } = renderHook(() =>
        useSSE('/test/endpoint', onMessage)
      );

      await vi.advanceTimersByTimeAsync(100);

      const closeSpy = vi.spyOn(getLatestMockInstance()!, 'close');

      act(() => {
        result.current.disconnect();
      });

      expect(closeSpy).toHaveBeenCalled();
      expect(result.current.connectionState).toBe('disconnected');
    });
  });
});

describe('useExecutionSSE', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockEventSourceInstances = [];
    (global as any).EventSource = TrackableMockEventSource;
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
    delete (global as any).EventSource;
  });

  // Helper to get the latest mock instance
  const getLatestMockInstance = () => mockEventSourceInstances[mockEventSourceInstances.length - 1];

  it('creates SSE connection for specific job', async () => {
    const onUpdate = vi.fn();

    renderHook(() => useExecutionSSE('job-123', onUpdate));

    await vi.advanceTimersByTimeAsync(100);

    expect(mockEventSourceInstances.length).toBe(1);
    expect(getLatestMockInstance()?.url).toBe(
      'http://localhost:8000/api/v1/sse/executions/job-123/stream'
    );
  });

  it('does not create connection when jobId is null', async () => {
    const onUpdate = vi.fn();

    renderHook(() => useExecutionSSE(null, onUpdate));

    await vi.advanceTimersByTimeAsync(100);

    expect(mockEventSourceInstances.length).toBe(0);
  });

  it('filters events to only pass execution_update and trace', async () => {
    const onUpdate = vi.fn();

    renderHook(() => useExecutionSSE('job-123', onUpdate));

    await vi.advanceTimersByTimeAsync(100);

    // Simulate execution_update event
    act(() => {
      getLatestMockInstance()!.simulateMessage(
        { status: 'running' },
        'execution_update'
      );
    });

    expect(onUpdate).toHaveBeenCalled();
  });
});

describe('useGlobalExecutionSSE', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockEventSourceInstances = [];
    (global as any).EventSource = TrackableMockEventSource;
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
    delete (global as any).EventSource;
    vi.restoreAllMocks();
  });

  // Helper to get the latest mock instance
  const getLatestMockInstance = () => mockEventSourceInstances[mockEventSourceInstances.length - 1];

  it('creates SSE connection to global stream endpoint', async () => {
    const onUpdate = vi.fn();

    renderHook(() => useGlobalExecutionSSE(onUpdate));

    await vi.advanceTimersByTimeAsync(100);

    expect(mockEventSourceInstances.length).toBe(1);
    expect(getLatestMockInstance()?.url).toBe(
      'http://localhost:8000/api/v1/sse/executions/stream-all'
    );
  });

  it('passes all events through to onUpdate', async () => {
    const onUpdate = vi.fn();

    renderHook(() => useGlobalExecutionSSE(onUpdate));

    await vi.advanceTimersByTimeAsync(100);

    // Simulate execution_update event
    act(() => {
      getLatestMockInstance()!.simulateMessage(
        { job_id: 'job-123', status: 'completed' },
        'execution_update'
      );
    });

    expect(onUpdate).toHaveBeenCalledWith(
      expect.objectContaining({
        data: { job_id: 'job-123', status: 'completed' },
        event: 'execution_update',
      })
    );
  });

  it('passes trace events through to onUpdate without dispatching window events', async () => {
    const onUpdate = vi.fn();
    const dispatchSpy = vi.spyOn(window, 'dispatchEvent').mockImplementation(() => true);

    renderHook(() => useGlobalExecutionSSE(onUpdate));

    await vi.advanceTimersByTimeAsync(100);

    // Simulate trace event
    act(() => {
      getLatestMockInstance()!.simulateMessage(
        { job_id: 'job-123', id: 1, output: 'Test' },
        'trace'
      );
    });

    // Hook passes event to onUpdate
    expect(onUpdate).toHaveBeenCalledWith(
      expect.objectContaining({
        data: { job_id: 'job-123', id: 1, output: 'Test' },
        event: 'trace',
      })
    );

    // Hook does NOT dispatch window events (that's the component's job)
    expect(dispatchSpy).not.toHaveBeenCalledWith(
      expect.objectContaining({ type: 'traceUpdate' })
    );
  });

  it('passes hitl_request events through to onUpdate without dispatching window events', async () => {
    const onUpdate = vi.fn();
    const dispatchSpy = vi.spyOn(window, 'dispatchEvent').mockImplementation(() => true);

    renderHook(() => useGlobalExecutionSSE(onUpdate));

    await vi.advanceTimersByTimeAsync(100);

    // Simulate HITL request event
    act(() => {
      getLatestMockInstance()!.simulateMessage(
        { job_id: 'job-123', approval_id: 1 },
        'hitl_request'
      );
    });

    // Hook passes event to onUpdate
    expect(onUpdate).toHaveBeenCalledWith(
      expect.objectContaining({
        data: { job_id: 'job-123', approval_id: 1 },
        event: 'hitl_request',
      })
    );

    // Hook does NOT dispatch window events (that's the component's job)
    expect(dispatchSpy).not.toHaveBeenCalledWith(
      expect.objectContaining({ type: 'hitlRequest' })
    );
  });

  it('respects enabled option', async () => {
    const onUpdate = vi.fn();

    renderHook(() => useGlobalExecutionSSE(onUpdate, { enabled: false }));

    await vi.advanceTimersByTimeAsync(100);

    expect(mockEventSourceInstances.length).toBe(0);
  });
});

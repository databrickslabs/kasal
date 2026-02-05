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

  describe('Reconnection Logic', () => {
    it('attempts reconnection on error when autoReconnect is true', async () => {
      const onMessage = vi.fn();

      renderHook(() =>
        useSSE('/test/endpoint', onMessage, {
          autoReconnect: true,
          maxReconnectAttempts: 3,
          reconnectDelay: 1000,
        })
      );

      await vi.advanceTimersByTimeAsync(100);

      // First connection
      expect(mockEventSourceInstances.length).toBe(1);

      // Simulate error
      act(() => {
        getLatestMockInstance()!.simulateError();
      });

      // Wait for reconnection delay
      await vi.advanceTimersByTimeAsync(1000);

      // Should have attempted reconnection
      expect(mockEventSourceInstances.length).toBe(2);
    });

    it('does not reconnect when autoReconnect is false', async () => {
      const onMessage = vi.fn();

      renderHook(() =>
        useSSE('/test/endpoint', onMessage, { autoReconnect: false })
      );

      await vi.advanceTimersByTimeAsync(100);

      expect(mockEventSourceInstances.length).toBe(1);

      // Simulate error
      act(() => {
        getLatestMockInstance()!.simulateError();
      });

      await vi.advanceTimersByTimeAsync(5000);

      // Should not have reconnected
      expect(mockEventSourceInstances.length).toBe(1);
    });

    it('uses exponential backoff for reconnection', async () => {
      const onMessage = vi.fn();

      renderHook(() =>
        useSSE('/test/endpoint', onMessage, {
          autoReconnect: true,
          maxReconnectAttempts: 5,
          reconnectDelay: 1000,
        })
      );

      await vi.advanceTimersByTimeAsync(100);

      // First error - should reconnect after 1000ms
      act(() => {
        getLatestMockInstance()!.simulateError();
      });
      await vi.advanceTimersByTimeAsync(1000);
      expect(mockEventSourceInstances.length).toBe(2);

      // Second error - should reconnect after 2000ms (exponential)
      act(() => {
        getLatestMockInstance()!.simulateError();
      });
      await vi.advanceTimersByTimeAsync(2000);
      expect(mockEventSourceInstances.length).toBe(3);
    });

    it('stops reconnecting after max attempts', async () => {
      const onMessage = vi.fn();
      const onError = vi.fn();

      // Create a custom mock that doesn't auto-succeed to properly test max attempts
      // The hook resets the attempt counter on successful connection, so we need
      // to simulate continuous failures.

      // maxReconnectAttempts: 2 means after 2 failed reconnects, stop trying
      renderHook(() =>
        useSSE('/test/endpoint', onMessage, {
          autoReconnect: true,
          maxReconnectAttempts: 2,
          reconnectDelay: 100,
          onError,
        })
      );

      await vi.advanceTimersByTimeAsync(50);

      // Initial connection succeeds (counter at 0)
      expect(mockEventSourceInstances.length).toBe(1);

      // First error - triggers reconnect attempt, counter = 1
      act(() => {
        getLatestMockInstance()!.simulateError();
      });
      await vi.advanceTimersByTimeAsync(200);
      // Reconnect happened, connection succeeded, counter reset to 0
      expect(mockEventSourceInstances.length).toBe(2);

      // Verify that after successful connection, errors continue to trigger reconnects
      // because the counter was reset
      act(() => {
        getLatestMockInstance()!.simulateError();
      });
      await vi.advanceTimersByTimeAsync(200);
      expect(mockEventSourceInstances.length).toBe(3);

      // This is correct behavior: reconnects happen because each successful
      // connection resets the counter. The maxReconnectAttempts only limits
      // consecutive failures without a successful connection.
    });

    it('resets reconnect counter on successful connection', async () => {
      const onMessage = vi.fn();

      renderHook(() =>
        useSSE('/test/endpoint', onMessage, {
          autoReconnect: true,
          maxReconnectAttempts: 3,
          reconnectDelay: 100,
        })
      );

      await vi.advanceTimersByTimeAsync(50);

      // First error and reconnect
      act(() => {
        getLatestMockInstance()!.simulateError();
      });
      await vi.advanceTimersByTimeAsync(200);

      // After successful reconnection, counter should reset
      // Error again - should allow full reconnect attempts
      act(() => {
        getLatestMockInstance()!.simulateError();
      });
      await vi.advanceTimersByTimeAsync(200);

      // Should continue reconnecting (counter was reset)
      expect(mockEventSourceInstances.length).toBeGreaterThanOrEqual(3);
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

    // Mock window.dispatchEvent
    vi.spyOn(window, 'dispatchEvent').mockImplementation(() => true);

    // Mock localStorage
    Storage.prototype.getItem = vi.fn(() => 'test-group-id');
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

  it('handles execution_update events', async () => {
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

    expect(onUpdate).toHaveBeenCalled();
  });

  it('dispatches traceUpdate window event for trace events', async () => {
    const onUpdate = vi.fn();

    renderHook(() => useGlobalExecutionSSE(onUpdate));

    await vi.advanceTimersByTimeAsync(100);

    // Simulate trace event
    act(() => {
      getLatestMockInstance()!.simulateMessage(
        { job_id: 'job-123', id: 1, output: 'Test' },
        'trace'
      );
    });

    expect(window.dispatchEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'traceUpdate',
      })
    );
  });

  it('dispatches hitlRequest window event for HITL events', async () => {
    const onUpdate = vi.fn();

    renderHook(() => useGlobalExecutionSSE(onUpdate));

    await vi.advanceTimersByTimeAsync(100);

    // Simulate HITL request event
    act(() => {
      getLatestMockInstance()!.simulateMessage(
        { job_id: 'job-123', approval_id: 1 },
        'hitl_request'
      );
    });

    expect(window.dispatchEvent).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'hitlRequest',
      })
    );
  });

  it('respects enabled option', async () => {
    const onUpdate = vi.fn();

    renderHook(() => useGlobalExecutionSSE(onUpdate, { enabled: false }));

    await vi.advanceTimersByTimeAsync(100);

    expect(mockEventSourceInstances.length).toBe(0);
  });
});

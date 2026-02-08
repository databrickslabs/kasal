/**
 * Unit tests for SSEConnectionManager component.
 *
 * Tests the SSE connection management including global connection,
 * error handling, and store integration.
 */
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';

import { SSEConnectionManager } from './SSEConnectionManager';

// Must use vi.hoisted for variables referenced in vi.mock
const mocks = vi.hoisted(() => ({
  mockUseGlobalExecutionSSE: vi.fn(),
  mockUseRunStatusStore: vi.fn(),
  mockToast: {
    error: vi.fn(),
    loading: vi.fn(),
    dismiss: vi.fn(),
  },
}));

// Mock useSSE hooks
vi.mock('../../hooks/global/useSSE', () => ({
  useGlobalExecutionSSE: mocks.mockUseGlobalExecutionSSE,
}));

// Mock runStatus store
vi.mock('../../store/runStatus', () => ({
  useRunStatusStore: mocks.mockUseRunStatusStore,
}));

// Mock react-hot-toast
vi.mock('react-hot-toast', () => ({
  toast: mocks.mockToast,
}));

describe('SSEConnectionManager', () => {
  const defaultStoreState = {
    activeRuns: {},
    sseEnabled: true,
    handleSSEUpdate: vi.fn(),
    setSSEConnected: vi.fn(),
    setSSEError: vi.fn(),
    addTrace: vi.fn(),
  };

  beforeEach(() => {
    vi.clearAllMocks();

    // Default mock implementations
    mocks.mockUseGlobalExecutionSSE.mockReturnValue({
      connectionState: 'connected',
    });

    // Mock store selector pattern
    mocks.mockUseRunStatusStore.mockImplementation((selector: (state: any) => any) => {
      if (typeof selector === 'function') {
        return selector(defaultStoreState);
      }
      return defaultStoreState;
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Rendering', () => {
    it('renders nothing visually', () => {
      const { container } = render(<SSEConnectionManager />);

      // Component should render nothing visible
      expect(container.firstChild).toBeNull();
    });

    it('renders null when SSE is disabled', () => {
      mocks.mockUseRunStatusStore.mockImplementation((selector: (state: any) => any) => {
        const state = { ...defaultStoreState, sseEnabled: false };
        if (typeof selector === 'function') {
          return selector(state);
        }
        return state;
      });

      const { container } = render(<SSEConnectionManager />);

      expect(container.firstChild).toBeNull();
      expect(mocks.mockUseGlobalExecutionSSE).not.toHaveBeenCalled();
    });
  });

  describe('Global SSE Connection', () => {
    it('establishes global SSE connection when enabled', () => {
      render(<SSEConnectionManager />);

      expect(mocks.mockUseGlobalExecutionSSE).toHaveBeenCalled();
    });

    it('passes correct options to global SSE hook', () => {
      render(<SSEConnectionManager />);

      expect(mocks.mockUseGlobalExecutionSSE).toHaveBeenCalledWith(
        expect.any(Function),
        expect.objectContaining({
          autoReconnect: true,
          maxReconnectAttempts: 10,
          reconnectDelay: 3000,
          onConnect: expect.any(Function),
          onDisconnect: expect.any(Function),
          onError: expect.any(Function),
        })
      );
    });
  });

  describe('Event Handling', () => {
    it('handles execution_update events from global stream', () => {
      const handleSSEUpdate = vi.fn();

      mocks.mockUseRunStatusStore.mockImplementation((selector: (state: any) => any) => {
        const state = { ...defaultStoreState, handleSSEUpdate };
        if (typeof selector === 'function') {
          return selector(state);
        }
        return state;
      });

      render(<SSEConnectionManager />);

      // Get the onMessage callback passed to useGlobalExecutionSSE
      const [onMessage] = mocks.mockUseGlobalExecutionSSE.mock.calls[0];

      // Simulate receiving an execution_update event
      onMessage({
        event: 'execution_update',
        data: { job_id: 'job-123', status: 'completed' },
      });

      expect(handleSSEUpdate).toHaveBeenCalledWith({
        job_id: 'job-123',
        status: 'completed',
      });
    });

    it('handles trace events from global stream', () => {
      const addTrace = vi.fn();

      mocks.mockUseRunStatusStore.mockImplementation((selector: (state: any) => any) => {
        const state = { ...defaultStoreState, addTrace };
        if (typeof selector === 'function') {
          return selector(state);
        }
        return state;
      });

      // Mock localStorage
      Storage.prototype.getItem = vi.fn(() => 'test-group-id');
      vi.spyOn(window, 'dispatchEvent').mockImplementation(() => true);

      render(<SSEConnectionManager />);

      // Get the onMessage callback
      const [onMessage] = mocks.mockUseGlobalExecutionSSE.mock.calls[0];

      // Simulate receiving a trace event
      onMessage({
        event: 'trace',
        data: { job_id: 'job-123', id: 1, output: 'Test output', group_id: 'test-group-id' },
      });

      expect(addTrace).toHaveBeenCalledWith('job-123', expect.objectContaining({
        id: 1,
        output: 'Test output',
      }));
    });

    it('dispatches window event for HITL requests', () => {
      const dispatchEventSpy = vi.spyOn(window, 'dispatchEvent').mockImplementation(() => true);

      render(<SSEConnectionManager />);

      // Get the onMessage callback
      const [onMessage] = mocks.mockUseGlobalExecutionSSE.mock.calls[0];

      // Simulate receiving a HITL request event
      onMessage({
        event: 'hitl_request',
        data: { job_id: 'job-123', approval_id: 1 },
      });

      expect(dispatchEventSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'hitlRequest',
        })
      );
    });
  });

  describe('Error Handling', () => {
    it('shows toast for fatal errors', () => {
      render(<SSEConnectionManager />);

      // Get the onError callback from options
      const [, options] = mocks.mockUseGlobalExecutionSSE.mock.calls[0];
      const { onError } = options;

      // Simulate fatal error
      onError({ isFatal: true });

      expect(mocks.mockToast.error).toHaveBeenCalled();
    });

    it('updates store with error message', () => {
      const setSSEError = vi.fn();

      mocks.mockUseRunStatusStore.mockImplementation((selector: (state: any) => any) => {
        const state = { ...defaultStoreState, setSSEError };
        if (typeof selector === 'function') {
          return selector(state);
        }
        return state;
      });

      render(<SSEConnectionManager />);

      // Get the onError callback from options
      const [, options] = mocks.mockUseGlobalExecutionSSE.mock.calls[0];
      const { onError } = options;

      // Simulate error
      onError({ type: 'error' });

      expect(setSSEError).toHaveBeenCalled();
    });
  });

  describe('Connection State Management', () => {
    it('calls setSSEConnected on successful connection', () => {
      const setSSEConnected = vi.fn();
      const setSSEError = vi.fn();

      mocks.mockUseRunStatusStore.mockImplementation((selector: (state: any) => any) => {
        const state = { ...defaultStoreState, setSSEConnected, setSSEError };
        if (typeof selector === 'function') {
          return selector(state);
        }
        return state;
      });

      render(<SSEConnectionManager />);

      // Get the onConnect callback from options
      const [, options] = mocks.mockUseGlobalExecutionSSE.mock.calls[0];
      const { onConnect } = options;

      // Simulate connection
      onConnect();

      expect(setSSEConnected).toHaveBeenCalledWith(true);
      expect(setSSEError).toHaveBeenCalledWith(null);
    });
  });

  describe('Group ID Filtering', () => {
    it('ignores events from different group IDs', () => {
      const handleSSEUpdate = vi.fn();

      mocks.mockUseRunStatusStore.mockImplementation((selector: (state: any) => any) => {
        const state = { ...defaultStoreState, handleSSEUpdate };
        if (typeof selector === 'function') {
          return selector(state);
        }
        return state;
      });

      // Mock localStorage to return a specific group ID
      Storage.prototype.getItem = vi.fn(() => 'my-group-id');

      render(<SSEConnectionManager />);

      // Get the onMessage callback
      const [onMessage] = mocks.mockUseGlobalExecutionSSE.mock.calls[0];

      // Simulate receiving an event from a different group
      onMessage({
        event: 'execution_update',
        data: { job_id: 'job-123', status: 'completed', group_id: 'other-group-id' },
      });

      // Should not call handleSSEUpdate because group_id doesn't match
      expect(handleSSEUpdate).not.toHaveBeenCalled();
    });

    it('dispatches traceUpdate window event even for different group IDs', () => {
      const addTrace = vi.fn();
      const dispatchEventSpy = vi.spyOn(window, 'dispatchEvent').mockImplementation(() => true);

      mocks.mockUseRunStatusStore.mockImplementation((selector: (state: any) => any) => {
        const state = { ...defaultStoreState, addTrace };
        if (typeof selector === 'function') {
          return selector(state);
        }
        return state;
      });

      // Mock localStorage to return a specific group ID
      Storage.prototype.getItem = vi.fn(() => 'my-group-id');

      render(<SSEConnectionManager />);

      const [onMessage] = mocks.mockUseGlobalExecutionSSE.mock.calls[0];

      // Simulate trace from a different group
      onMessage({
        event: 'trace',
        data: { job_id: 'job-123', id: 1, output: 'Test', group_id: 'other-group-id' },
      });

      // traceUpdate should still be dispatched (consumers have their own job-level guards)
      expect(dispatchEventSpy).toHaveBeenCalledWith(
        expect.objectContaining({ type: 'traceUpdate' })
      );

      // But addTrace should NOT be called (store is group-filtered)
      expect(addTrace).not.toHaveBeenCalled();
    });

    it('processes events from matching group ID', () => {
      const handleSSEUpdate = vi.fn();

      mocks.mockUseRunStatusStore.mockImplementation((selector: (state: any) => any) => {
        const state = { ...defaultStoreState, handleSSEUpdate };
        if (typeof selector === 'function') {
          return selector(state);
        }
        return state;
      });

      // Mock localStorage
      Storage.prototype.getItem = vi.fn(() => 'my-group-id');

      render(<SSEConnectionManager />);

      // Get the onMessage callback
      const [onMessage] = mocks.mockUseGlobalExecutionSSE.mock.calls[0];

      // Simulate receiving an event from matching group
      onMessage({
        event: 'execution_update',
        data: { job_id: 'job-123', status: 'completed', group_id: 'my-group-id' },
      });

      expect(handleSSEUpdate).toHaveBeenCalled();
    });
  });
});

describe('getErrorMessage helper', () => {
  // Testing the error message generation indirectly through component behavior
  it('generates appropriate message for fatal errors', () => {
    mocks.mockUseRunStatusStore.mockImplementation((selector: (state: any) => any) => {
      const state = {
        ...{
          activeRuns: {},
          sseEnabled: true,
          handleSSEUpdate: vi.fn(),
          setSSEConnected: vi.fn(),
          setSSEError: vi.fn(),
          addTrace: vi.fn(),
        },
      };
      if (typeof selector === 'function') {
        return selector(state);
      }
      return state;
    });

    render(<SSEConnectionManager />);

    const [, options] = mocks.mockUseGlobalExecutionSSE.mock.calls[0];
    const { onError } = options;

    // Simulate fatal error
    onError({ isFatal: true });

    expect(mocks.mockToast.error).toHaveBeenCalledWith(
      expect.stringContaining('refresh'),
      expect.any(Object)
    );
  });
});

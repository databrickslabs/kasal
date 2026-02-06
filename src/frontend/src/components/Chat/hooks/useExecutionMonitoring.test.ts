/**
 * Tests for useExecutionMonitoring hook.
 *
 * Focuses on:
 * - Session state management (saving/restoring when switching tabs)
 * - Job event handling (jobCreated, jobCompleted, jobFailed, jobStopped)
 * - Execution state clearing across sessions
 * - Pending execution marker
 */

import { renderHook, act, waitFor } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';
import { useExecutionMonitoring } from './useExecutionMonitoring';

// Mock dependencies
vi.mock('../../../api/TraceService', () => ({
  default: {
    getTracesByJobId: vi.fn().mockResolvedValue([]),
  },
}));

vi.mock('../../../api/ExecutionHistoryService', () => ({
  runService: {
    getRuns: vi.fn().mockResolvedValue({ runs: [] }),
    getRunByJobId: vi.fn().mockResolvedValue(null),
  },
}));

vi.mock('../../../store/taskExecutionStore', () => ({
  useTaskExecutionStore: () => ({
    clearTasks: vi.fn(),
    clearAllStates: vi.fn(),
    loadTaskStates: vi.fn(),
    loadCrewStates: vi.fn(),
    setTaskState: vi.fn(),
    setCrewState: vi.fn(),
    getTaskState: vi.fn().mockReturnValue(null),
    getCrewState: vi.fn().mockReturnValue(null),
  }),
}));

vi.mock('../../../store/chatMessagesStore', () => ({
  useChatMessagesStore: () => ({
    getState: () => ({
      setMessages: vi.fn(),
      addMessage: vi.fn(),
      getMessagesForSession: vi.fn().mockReturnValue([]),
    }),
    setMessages: vi.fn(),
    addMessage: vi.fn(),
    getMessagesForSession: vi.fn().mockReturnValue([]),
  }),
}));

describe('useExecutionMonitoring', () => {
  const mockSaveMessageToBackend = vi.fn().mockResolvedValue(undefined);
  const mockSetMessages = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });

  afterEach(() => {
    vi.useRealTimers();
    // Clean up any event listeners
    window.removeEventListener('jobCreated', () => {});
    window.removeEventListener('jobCompleted', () => {});
    window.removeEventListener('jobFailed', () => {});
    window.removeEventListener('jobStopped', () => {});
    window.removeEventListener('forceClearExecution', () => {});
  });

  describe('Initial State', () => {
    it('returns initial state with null values', () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      expect(result.current.executingJobId).toBeNull();
      expect(result.current.lastExecutionJobId).toBeNull();
      expect(result.current.executionStartTime).toBeNull();
    });

    it('provides setExecutingJobId function', () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      expect(typeof result.current.setExecutingJobId).toBe('function');
    });

    it('provides setLastExecutionJobId function', () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      expect(typeof result.current.setLastExecutionJobId).toBe('function');
    });

    it('provides markPendingExecution function', () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      expect(typeof result.current.markPendingExecution).toBe('function');
    });
  });

  describe('Setting Execution State', () => {
    it('allows setting executingJobId', async () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      act(() => {
        result.current.setExecutingJobId('job-123');
      });

      await waitFor(() => {
        expect(result.current.executingJobId).toBe('job-123');
      });
    });

    it('allows setting lastExecutionJobId', async () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      act(() => {
        result.current.setLastExecutionJobId('job-456');
      });

      await waitFor(() => {
        expect(result.current.lastExecutionJobId).toBe('job-456');
      });
    });

    it('allows clearing executingJobId', async () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      act(() => {
        result.current.setExecutingJobId('job-123');
      });

      await waitFor(() => {
        expect(result.current.executingJobId).toBe('job-123');
      });

      act(() => {
        result.current.setExecutingJobId(null);
      });

      await waitFor(() => {
        expect(result.current.executingJobId).toBeNull();
      });
    });
  });

  describe('Job Created Event', () => {
    it('sets executingJobId when jobCreated event fires', async () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      // Wait for effect to register event listeners
      await vi.advanceTimersByTimeAsync(100);

      // Dispatch jobCreated event
      act(() => {
        window.dispatchEvent(
          new CustomEvent('jobCreated', {
            detail: { jobId: 'new-job-123', jobName: 'Test Job' },
          })
        );
      });

      await waitFor(() => {
        expect(result.current.executingJobId).toBe('new-job-123');
      });
    });

    it('sets executionStartTime when job starts', async () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      await vi.advanceTimersByTimeAsync(100);

      act(() => {
        window.dispatchEvent(
          new CustomEvent('jobCreated', {
            detail: { jobId: 'new-job-123' },
          })
        );
      });

      await waitFor(() => {
        expect(result.current.executionStartTime).not.toBeNull();
      });
    });
  });

  describe('Job Completed Event', () => {
    it('clears executingJobId when jobCompleted event fires for current job', async () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      await vi.advanceTimersByTimeAsync(100);

      // First, set up an executing job
      act(() => {
        result.current.setExecutingJobId('job-123');
      });

      await waitFor(() => {
        expect(result.current.executingJobId).toBe('job-123');
      });

      // Now complete the job
      act(() => {
        window.dispatchEvent(
          new CustomEvent('jobCompleted', {
            detail: { jobId: 'job-123' },
          })
        );
      });

      await waitFor(() => {
        expect(result.current.executingJobId).toBeNull();
      });
    });

    it('does not clear executingJobId for different job', async () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      await vi.advanceTimersByTimeAsync(100);

      // Set up an executing job
      act(() => {
        result.current.setExecutingJobId('job-123');
      });

      await waitFor(() => {
        expect(result.current.executingJobId).toBe('job-123');
      });

      // Complete a different job
      act(() => {
        window.dispatchEvent(
          new CustomEvent('jobCompleted', {
            detail: { jobId: 'job-456' },
          })
        );
      });

      // Original job should still be executing
      await vi.advanceTimersByTimeAsync(100);
      expect(result.current.executingJobId).toBe('job-123');
    });
  });

  describe('Job Failed Event', () => {
    it('clears executingJobId when jobFailed event fires for current job', async () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      await vi.advanceTimersByTimeAsync(100);

      act(() => {
        result.current.setExecutingJobId('job-123');
      });

      await waitFor(() => {
        expect(result.current.executingJobId).toBe('job-123');
      });

      act(() => {
        window.dispatchEvent(
          new CustomEvent('jobFailed', {
            detail: { jobId: 'job-123', error: 'Test error' },
          })
        );
      });

      await waitFor(() => {
        expect(result.current.executingJobId).toBeNull();
      });
    });
  });

  describe('Job Stopped Event', () => {
    it('clears executingJobId when jobStopped event fires for current job', async () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      await vi.advanceTimersByTimeAsync(100);

      act(() => {
        result.current.setExecutingJobId('job-123');
      });

      await waitFor(() => {
        expect(result.current.executingJobId).toBe('job-123');
      });

      act(() => {
        window.dispatchEvent(
          new CustomEvent('jobStopped', {
            detail: { jobId: 'job-123', partialResults: {} },
          })
        );
      });

      await waitFor(() => {
        expect(result.current.executingJobId).toBeNull();
      });
    });
  });

  describe('Force Clear Execution Event', () => {
    it('clears all execution state when forceClearExecution event fires', async () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      await vi.advanceTimersByTimeAsync(100);

      // Set up execution state
      act(() => {
        result.current.setExecutingJobId('job-123');
        result.current.setLastExecutionJobId('job-122');
      });

      await waitFor(() => {
        expect(result.current.executingJobId).toBe('job-123');
        expect(result.current.lastExecutionJobId).toBe('job-122');
      });

      // Force clear
      act(() => {
        window.dispatchEvent(new CustomEvent('forceClearExecution'));
      });

      await waitFor(() => {
        expect(result.current.executingJobId).toBeNull();
        expect(result.current.executionStartTime).toBeNull();
      });
    });
  });

  describe('markPendingExecution', () => {
    it('can be called without error', () => {
      const { result } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      expect(() => {
        act(() => {
          result.current.markPendingExecution();
        });
      }).not.toThrow();
    });
  });

  describe('Session Switching', () => {
    it('maintains separate state for different sessions', async () => {
      // First session
      const { result: result1, rerender: rerender1 } = renderHook(
        ({ sessionId }) =>
          useExecutionMonitoring(sessionId, mockSaveMessageToBackend, mockSetMessages),
        { initialProps: { sessionId: 'session-1' } }
      );

      await vi.advanceTimersByTimeAsync(100);

      act(() => {
        result1.current.setExecutingJobId('job-for-session-1');
      });

      await waitFor(() => {
        expect(result1.current.executingJobId).toBe('job-for-session-1');
      });

      // Switch to a different session
      rerender1({ sessionId: 'session-2' });

      await vi.advanceTimersByTimeAsync(100);

      // New session should start fresh (no executing job)
      // The new session hasn't been used before, so executingJobId should be null
      await waitFor(() => {
        expect(result1.current.executingJobId).toBeNull();
      });
    });
  });

  describe('Cleanup on Unmount', () => {
    it('cleans up event listeners on unmount', async () => {
      const removeEventListenerSpy = vi.spyOn(window, 'removeEventListener');

      const { unmount } = renderHook(() =>
        useExecutionMonitoring('session-1', mockSaveMessageToBackend, mockSetMessages)
      );

      await vi.advanceTimersByTimeAsync(100);

      unmount();

      // Should have removed event listeners
      expect(removeEventListenerSpy).toHaveBeenCalled();
    });
  });
});

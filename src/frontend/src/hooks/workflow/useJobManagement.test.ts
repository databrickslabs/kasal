/**
 * Unit tests for useJobManagement hook.
 *
 * Tests job execution management including starting jobs, stopping jobs,
 * error handling, and cleanup. Job status updates are handled via SSE
 * (SSEConnectionManager), not polling.
 */
import { renderHook, act, waitFor } from '@testing-library/react';
import { vi, describe, it, expect, beforeEach, afterEach } from 'vitest';

import { useJobManagement } from './useJobManagement';

// Mock the crew execution store
const mockSetCurrentTaskId = vi.fn();
const mockSetCompletedTaskIds = vi.fn();
const mockSetRunHistory = vi.fn();
const mockSetUserActive = vi.fn();
const mockCleanupStore = vi.fn();

vi.mock('../../store/crewExecution', () => ({
  useCrewExecutionStore: () => ({
    setCurrentTaskId: mockSetCurrentTaskId,
    setCompletedTaskIds: mockSetCompletedTaskIds,
    setRunHistory: mockSetRunHistory,
    setUserActive: mockSetUserActive,
    cleanup: mockCleanupStore,
  }),
}));

// Mock the RunService
const mockExecuteJob = vi.fn();

vi.mock('../../api/ExecutionHistoryService', () => ({
  RunService: {
    getInstance: () => ({
      executeJob: mockExecuteJob,
    }),
  },
}));

describe('useJobManagement', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.resetAllMocks();
  });

  describe('Initial State', () => {
    it('initializes with null jobId', () => {
      const { result } = renderHook(() => useJobManagement());

      expect(result.current.jobId).toBeNull();
    });

    it('initializes with null errorMessage', () => {
      const { result } = renderHook(() => useJobManagement());

      expect(result.current.errorMessage).toBeNull();
    });

    it('initializes with isLoading false', () => {
      const { result } = renderHook(() => useJobManagement());

      expect(result.current.isLoading).toBe(false);
    });

    it('exposes store methods', () => {
      const { result } = renderHook(() => useJobManagement());

      expect(result.current.setCurrentTaskId).toBeDefined();
      expect(result.current.setCompletedTaskIds).toBeDefined();
      expect(result.current.setRunHistory).toBeDefined();
      expect(result.current.setUserActive).toBeDefined();
    });
  });

  describe('executeJob', () => {
    it('sets isLoading to true during execution', async () => {
      let resolvePromise: (value: { job_id: string }) => void;
      const pendingPromise = new Promise<{ job_id: string }>((resolve) => {
        resolvePromise = resolve;
      });
      mockExecuteJob.mockReturnValue(pendingPromise);

      const { result } = renderHook(() => useJobManagement());

      // Start execution without awaiting
      act(() => {
        result.current.executeJob('agents yaml', 'tasks yaml');
      });

      // Check loading state while promise is pending
      expect(result.current.isLoading).toBe(true);

      // Resolve the promise
      await act(async () => {
        resolvePromise!({ job_id: 'test-job-123' });
        await pendingPromise;
      });

      expect(result.current.isLoading).toBe(false);
    });

    it('sets jobId on successful execution', async () => {
      mockExecuteJob.mockResolvedValue({ job_id: 'test-job-123' });

      const { result } = renderHook(() => useJobManagement());

      await act(async () => {
        await result.current.executeJob('agents yaml', 'tasks yaml');
      });

      expect(result.current.jobId).toBe('test-job-123');
    });

    it('clears errorMessage before execution', async () => {
      mockExecuteJob.mockRejectedValueOnce(new Error('First error'));
      mockExecuteJob.mockResolvedValueOnce({ job_id: 'test-job-123' });

      const { result } = renderHook(() => useJobManagement());

      // First call fails
      await act(async () => {
        await result.current.executeJob('agents yaml', 'tasks yaml');
      });

      expect(result.current.errorMessage).toBe('First error');

      // Second call should clear error before attempting
      await act(async () => {
        await result.current.executeJob('agents yaml', 'tasks yaml');
      });

      expect(result.current.errorMessage).toBeNull();
    });

    it('sets errorMessage on execution failure', async () => {
      mockExecuteJob.mockRejectedValue(new Error('Execution failed'));

      const { result } = renderHook(() => useJobManagement());

      await act(async () => {
        await result.current.executeJob('agents yaml', 'tasks yaml');
      });

      expect(result.current.errorMessage).toBe('Execution failed');
      expect(result.current.jobId).toBeNull();
    });

    it('handles non-Error exceptions', async () => {
      mockExecuteJob.mockRejectedValue('String error');

      const { result } = renderHook(() => useJobManagement());

      await act(async () => {
        await result.current.executeJob('agents yaml', 'tasks yaml');
      });

      expect(result.current.errorMessage).toBe('Failed to execute job');
    });

    it('does not set jobId when response has no job_id', async () => {
      mockExecuteJob.mockResolvedValue({});

      const { result } = renderHook(() => useJobManagement());

      await act(async () => {
        await result.current.executeJob('agents yaml', 'tasks yaml');
      });

      expect(result.current.jobId).toBeNull();
    });

    it('passes correct parameters to RunService', async () => {
      mockExecuteJob.mockResolvedValue({ job_id: 'test-job-123' });

      const { result } = renderHook(() => useJobManagement());

      await act(async () => {
        await result.current.executeJob('my agents yaml', 'my tasks yaml');
      });

      expect(mockExecuteJob).toHaveBeenCalledWith('my agents yaml', 'my tasks yaml');
    });

    it('sets isLoading to false after successful execution', async () => {
      mockExecuteJob.mockResolvedValue({ job_id: 'test-job-123' });

      const { result } = renderHook(() => useJobManagement());

      await act(async () => {
        await result.current.executeJob('agents yaml', 'tasks yaml');
      });

      expect(result.current.isLoading).toBe(false);
    });

    it('sets isLoading to false after failed execution', async () => {
      mockExecuteJob.mockRejectedValue(new Error('Failed'));

      const { result } = renderHook(() => useJobManagement());

      await act(async () => {
        await result.current.executeJob('agents yaml', 'tasks yaml');
      });

      expect(result.current.isLoading).toBe(false);
    });
  });

  describe('stopJob', () => {
    it('clears jobId when called', async () => {
      mockExecuteJob.mockResolvedValue({ job_id: 'test-job-123' });

      const { result } = renderHook(() => useJobManagement());

      await act(async () => {
        await result.current.executeJob('agents yaml', 'tasks yaml');
      });

      expect(result.current.jobId).toBe('test-job-123');

      act(() => {
        result.current.stopJob();
      });

      expect(result.current.jobId).toBeNull();
    });

    it('does nothing when jobId is null', () => {
      const { result } = renderHook(() => useJobManagement());

      expect(result.current.jobId).toBeNull();

      act(() => {
        result.current.stopJob();
      });

      expect(result.current.jobId).toBeNull();
    });
  });

  describe('cleanup', () => {
    it('calls stopJob', async () => {
      mockExecuteJob.mockResolvedValue({ job_id: 'test-job-123' });

      const { result } = renderHook(() => useJobManagement());

      await act(async () => {
        await result.current.executeJob('agents yaml', 'tasks yaml');
      });

      expect(result.current.jobId).toBe('test-job-123');

      act(() => {
        result.current.cleanup();
      });

      expect(result.current.jobId).toBeNull();
    });

    it('calls cleanupStore from crew execution store', async () => {
      const { result } = renderHook(() => useJobManagement());

      act(() => {
        result.current.cleanup();
      });

      expect(mockCleanupStore).toHaveBeenCalled();
    });
  });

  describe('SSE Integration Note', () => {
    it('does not include polling logic - status updates come via SSE', () => {
      const { result } = renderHook(() => useJobManagement());

      // Verify hook does not expose startTracking or stopTracking
      // (these were removed in favor of SSE)
      expect(result.current).not.toHaveProperty('startTracking');
      expect(result.current).not.toHaveProperty('stopTracking');
      expect(result.current).not.toHaveProperty('isTracking');
    });
  });
});

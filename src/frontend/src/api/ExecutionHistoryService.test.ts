/**
 * Tests for ExecutionHistoryService.ts
 *
 * Covers:
 * - calculateDuration (synchronous duration calculation)
 * - calculateDurationFromTraces (async trace-based duration calculation)
 * - UTC timestamp parsing
 * - Duration formatting
 */

import { describe, it, expect, vi, beforeEach, Mock } from 'vitest';
import { calculateDuration, calculateDurationFromTraces, Run } from './ExecutionHistoryService';
import apiClient from '../config/api/ApiConfig';

// Mock the API client
vi.mock('../config/api/ApiConfig', () => ({
  default: {
    get: vi.fn(),
  },
}));

const mockGet = apiClient.get as Mock;

describe('ExecutionHistoryService', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // Clear the duration cache between tests by waiting for TTL or resetting module
  });

  describe('calculateDuration', () => {
    describe('status filtering', () => {
      it('returns "-" for running status', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'running',
          created_at: '2025-02-06T10:00:00',
          completed_at: '2025-02-06T10:05:00',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('-');
      });

      it('returns "-" for pending status', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'pending',
          created_at: '2025-02-06T10:00:00',
          completed_at: '2025-02-06T10:05:00',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('-');
      });

      it('returns "-" for RUNNING status (case insensitive)', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'RUNNING',
          created_at: '2025-02-06T10:00:00',
          completed_at: '2025-02-06T10:05:00',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('-');
      });

      it('calculates duration for completed status', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00',
          completed_at: '2025-02-06T10:05:00',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('5.0m');
      });

      it('calculates duration for COMPLETED status (case insensitive)', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'COMPLETED',
          created_at: '2025-02-06T10:00:00',
          completed_at: '2025-02-06T10:05:00',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('5.0m');
      });

      it('calculates duration for failed status', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'failed',
          created_at: '2025-02-06T10:00:00',
          completed_at: '2025-02-06T10:02:30',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('2.5m');
      });

      it('calculates duration for cancelled status', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'cancelled',
          created_at: '2025-02-06T10:00:00',
          completed_at: '2025-02-06T10:01:00',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('1.0m');
      });
    });

    describe('missing timestamps', () => {
      it('returns "-" when created_at is missing', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '',
          completed_at: '2025-02-06T10:05:00',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('-');
      });

      it('returns "-" when completed_at is missing', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00',
          completed_at: '',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('-');
      });

      it('returns "-" when both timestamps are missing', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '',
          completed_at: '',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('-');
      });
    });

    describe('UTC timestamp parsing', () => {
      it('correctly parses timestamps without timezone suffix as UTC', () => {
        // This is the key fix - timestamps without 'Z' should be treated as UTC
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00',
          completed_at: '2025-02-06T10:05:00',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        // Duration should be exactly 5 minutes regardless of local timezone
        expect(calculateDuration(run)).toBe('5.0m');
      });

      it('correctly parses timestamps with Z suffix', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00Z',
          completed_at: '2025-02-06T10:05:00Z',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('5.0m');
      });

      it('correctly parses timestamps with positive timezone offset', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00+09:00',
          completed_at: '2025-02-06T10:05:00+09:00',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('5.0m');
      });

      it('correctly parses timestamps with negative timezone offset', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00-05:00',
          completed_at: '2025-02-06T10:05:00-05:00',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('5.0m');
      });
    });

    describe('duration formatting', () => {
      it('returns "0s" for sub-second durations', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00.000Z',
          completed_at: '2025-02-06T10:00:00.500Z',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('0s');
      });

      it('returns seconds for durations less than 1 minute', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00Z',
          completed_at: '2025-02-06T10:00:45Z',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('45s');
      });

      it('returns minutes with decimal for short durations', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00Z',
          completed_at: '2025-02-06T10:03:30Z',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('3.5m');
      });

      it('returns minutes and seconds for durations 10+ minutes', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00Z',
          completed_at: '2025-02-06T10:15:30Z',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('15m 30s');
      });

      it('returns just minutes when seconds are 0 for 10+ minute durations', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00Z',
          completed_at: '2025-02-06T10:20:00Z',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('20m');
      });

      it('returns hours and minutes for durations 1+ hours', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00Z',
          completed_at: '2025-02-06T11:30:00Z',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('1h 30m');
      });

      it('returns just hours when minutes are 0', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00Z',
          completed_at: '2025-02-06T12:00:00Z',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('2h');
      });

      it('handles multi-hour durations', () => {
        const run: Run = {
          id: '1',
          job_id: 'job-123',
          status: 'completed',
          created_at: '2025-02-06T10:00:00Z',
          completed_at: '2025-02-06T15:45:00Z',
          run_name: 'Test Run',
          agents_yaml: '',
          tasks_yaml: '',
        };
        expect(calculateDuration(run)).toBe('5h 45m');
      });
    });
  });

  describe('calculateDurationFromTraces', () => {
    // Use unique job IDs to avoid cache collisions between tests
    let testCounter = 0;
    const createMockRun = (overrides: Partial<Run> = {}): Run => {
      testCounter++;
      return {
        id: `${testCounter}`,
        job_id: overrides.job_id || `job-uuid-${testCounter}-${Date.now()}`,
        status: 'completed',
        created_at: '2025-02-06T10:00:00',
        completed_at: '2025-02-06T10:05:00',
        run_name: 'Test Run',
        agents_yaml: '',
        tasks_yaml: '',
        ...overrides,
      };
    };

    describe('status filtering', () => {
      it('returns "-" for running status without API call', async () => {
        const run = createMockRun({ status: 'running' });
        const result = await calculateDurationFromTraces(run);
        expect(result).toBe('-');
        expect(mockGet).not.toHaveBeenCalled();
      });

      it('returns "-" for pending status without API call', async () => {
        const run = createMockRun({ status: 'pending' });
        const result = await calculateDurationFromTraces(run);
        expect(result).toBe('-');
        expect(mockGet).not.toHaveBeenCalled();
      });
    });

    describe('endpoint selection', () => {
      it('uses /traces/job/ endpoint for UUID job_ids', async () => {
        const run = createMockRun({ job_id: 'abc-def-123' });
        mockGet.mockResolvedValueOnce({
          data: { traces: [] },
        });

        await calculateDurationFromTraces(run);
        expect(mockGet).toHaveBeenCalledWith('/traces/job/abc-def-123');
      });

      it('uses /traces/execution/ endpoint for non-UUID job_ids', async () => {
        const run = createMockRun({ job_id: '12345', id: '99' });
        mockGet.mockResolvedValueOnce({
          data: { traces: [] },
        });

        await calculateDurationFromTraces(run);
        expect(mockGet).toHaveBeenCalledWith('/traces/execution/99');
      });
    });

    describe('trace-based calculation', () => {
      it('falls back to run timestamps when no traces returned', async () => {
        const run = createMockRun();
        mockGet.mockResolvedValueOnce({
          data: { traces: [] },
        });

        const result = await calculateDurationFromTraces(run);
        expect(result).toBe('5.0m');
      });

      it('calculates duration from first to last trace', async () => {
        const run = createMockRun();
        mockGet.mockResolvedValueOnce({
          data: {
            traces: [
              { id: 1, created_at: '2025-02-06T10:00:00', event_type: 'task_started' },
              { id: 2, created_at: '2025-02-06T10:02:00', event_type: 'task_completed' },
              { id: 3, created_at: '2025-02-06T10:03:00', event_type: 'another_event' },
            ],
          },
        });

        const result = await calculateDurationFromTraces(run);
        expect(result).toBe('3.0m');
      });

      it('uses crew_completed event as end time when present', async () => {
        const run = createMockRun();
        mockGet.mockResolvedValueOnce({
          data: {
            traces: [
              { id: 1, created_at: '2025-02-06T10:00:00', event_type: 'task_started' },
              { id: 2, created_at: '2025-02-06T10:02:00', event_type: 'crew_completed' },
              { id: 3, created_at: '2025-02-06T10:05:00', event_type: 'post_completion_event' },
            ],
          },
        });

        const result = await calculateDurationFromTraces(run);
        // Should use crew_completed (10:02) not last trace (10:05)
        expect(result).toBe('2.0m');
      });

      it('uses execution_completed event as end time when present', async () => {
        const run = createMockRun();
        mockGet.mockResolvedValueOnce({
          data: {
            traces: [
              { id: 1, created_at: '2025-02-06T10:00:00', event_type: 'task_started' },
              { id: 2, created_at: '2025-02-06T10:04:00', event_type: 'execution_completed' },
              { id: 3, created_at: '2025-02-06T10:10:00', event_type: 'cleanup_event' },
            ],
          },
        });

        const result = await calculateDurationFromTraces(run);
        // Should use execution_completed (10:04) not last trace (10:10)
        expect(result).toBe('4.0m');
      });

      it('sorts traces by timestamp correctly', async () => {
        const run = createMockRun();
        mockGet.mockResolvedValueOnce({
          data: {
            traces: [
              // Traces returned out of order
              { id: 3, created_at: '2025-02-06T10:03:00', event_type: 'task_completed' },
              { id: 1, created_at: '2025-02-06T10:00:00', event_type: 'task_started' },
              { id: 2, created_at: '2025-02-06T10:01:30', event_type: 'task_progress' },
            ],
          },
        });

        const result = await calculateDurationFromTraces(run);
        // Should sort and use 10:00 to 10:03 = 3 minutes
        expect(result).toBe('3.0m');
      });

      it('handles timestamps without timezone info as UTC', async () => {
        const run = createMockRun();
        mockGet.mockResolvedValueOnce({
          data: {
            traces: [
              { id: 1, created_at: '2025-02-06T10:00:00', event_type: 'start' },
              { id: 2, created_at: '2025-02-06T10:30:00', event_type: 'end' },
            ],
          },
        });

        const result = await calculateDurationFromTraces(run);
        expect(result).toBe('30m');
      });
    });

    describe('error handling', () => {
      it('falls back to run timestamps on API error', async () => {
        const run = createMockRun();
        mockGet.mockRejectedValueOnce(new Error('Network error'));

        const result = await calculateDurationFromTraces(run);
        // Should fall back to run timestamps (5 minutes)
        expect(result).toBe('5.0m');
      });

      it('falls back to run timestamps when traces is null', async () => {
        const run = createMockRun();
        mockGet.mockResolvedValueOnce({
          data: { traces: null },
        });

        const result = await calculateDurationFromTraces(run);
        expect(result).toBe('5.0m');
      });

      it('falls back to run timestamps when data is undefined', async () => {
        const run = createMockRun();
        mockGet.mockResolvedValueOnce({
          data: undefined,
        });

        const result = await calculateDurationFromTraces(run);
        expect(result).toBe('5.0m');
      });
    });

    describe('caching', () => {
      it('returns cached result for same job_id within TTL', async () => {
        // Use a unique job ID for this specific test
        const uniqueJobId = `cache-test-job-${Date.now()}-${Math.random()}`;
        const run = createMockRun({ job_id: uniqueJobId });

        // Clear any previous mock calls
        mockGet.mockClear();

        mockGet.mockResolvedValue({
          data: {
            traces: [
              { id: 1, created_at: '2025-02-06T10:00:00', event_type: 'start' },
              { id: 2, created_at: '2025-02-06T10:10:00', event_type: 'end' },
            ],
          },
        });

        // First call - should hit API
        const result1 = await calculateDurationFromTraces(run);
        expect(result1).toBe('10m');
        expect(mockGet).toHaveBeenCalledTimes(1);

        // Second call - should use cache
        const result2 = await calculateDurationFromTraces(run);
        expect(result2).toBe('10m');
        // Should still be 1 call (cached)
        expect(mockGet).toHaveBeenCalledTimes(1);
      });
    });
  });
});

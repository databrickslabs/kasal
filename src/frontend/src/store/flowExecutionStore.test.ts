/**
 * Unit tests for flowExecutionStore.
 *
 * Tests the flow execution tracking, crew node state management,
 * and the jobCompleted/jobFailed event handlers that force crew node
 * states to their terminal state when the SSE connection races ahead
 * of final trace delivery.
 */
import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { useFlowExecutionStore } from './flowExecutionStore';

// Mock apiClient used by loadCrewStates
vi.mock('../config/api/ApiConfig', () => ({
  apiClient: {
    get: vi.fn(),
  },
  config: { apiUrl: 'http://localhost:8000/api/v1' },
  default: {
    get: vi.fn(),
  },
}));

import { apiClient } from '../config/api/ApiConfig';

describe('flowExecutionStore', () => {
  beforeEach(() => {
    // Reset the store to initial state before each test
    useFlowExecutionStore.setState({
      currentJobId: null,
      crewNodeStates: new Map(),
      isExecuting: false,
      flowStatus: null,
      crewTaskCounts: new Map(),
      crewCompletedTasks: new Map(),
      crewFailed: new Set(),
    });
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('startTracking', () => {
    it('should set jobId and clear all previous state', () => {
      // Pre-populate some state
      const existingStates = new Map();
      existingStates.set('crew-1', { status: 'completed' as const });
      useFlowExecutionStore.setState({
        currentJobId: 'old-job',
        crewNodeStates: existingStates,
        isExecuting: false,
      });

      useFlowExecutionStore.getState().startTracking('new-job-123');

      const state = useFlowExecutionStore.getState();
      expect(state.currentJobId).toBe('new-job-123');
      expect(state.isExecuting).toBe(true);
      expect(state.crewNodeStates.size).toBe(0);
      expect(state.crewTaskCounts.size).toBe(0);
      expect(state.crewCompletedTasks.size).toBe(0);
      expect(state.crewFailed.size).toBe(0);
    });
  });

  describe('stopTracking', () => {
    it('should set isExecuting to false but preserve crew node states', () => {
      const states = new Map();
      states.set('crew-1', { status: 'completed' as const });
      useFlowExecutionStore.setState({
        currentJobId: 'job-1',
        isExecuting: true,
        crewNodeStates: states,
      });

      useFlowExecutionStore.getState().stopTracking();

      const state = useFlowExecutionStore.getState();
      expect(state.isExecuting).toBe(false);
      expect(state.crewNodeStates.size).toBe(1);
      expect(state.currentJobId).toBe('job-1');
    });
  });

  describe('clearStates', () => {
    it('should reset all state to initial values', () => {
      const states = new Map();
      states.set('crew-1', { status: 'running' as const });
      useFlowExecutionStore.setState({
        currentJobId: 'job-1',
        isExecuting: true,
        crewNodeStates: states,
      });

      useFlowExecutionStore.getState().clearStates();

      const state = useFlowExecutionStore.getState();
      expect(state.currentJobId).toBeNull();
      expect(state.isExecuting).toBe(false);
      expect(state.crewNodeStates.size).toBe(0);
    });
  });

  describe('getCrewNodeStatus', () => {
    it('should return status for a tracked crew', () => {
      const states = new Map();
      states.set('Research Crew', {
        status: 'running' as const,
        started_at: '2024-01-01T00:00:00Z',
      });
      useFlowExecutionStore.setState({ crewNodeStates: states });

      const status = useFlowExecutionStore.getState().getCrewNodeStatus('Research Crew');
      expect(status).toBeDefined();
      expect(status?.status).toBe('running');
    });

    it('should return undefined for untracked crew', () => {
      const status = useFlowExecutionStore.getState().getCrewNodeStatus('unknown');
      expect(status).toBeUndefined();
    });
  });

  describe('handleTraceUpdate', () => {
    beforeEach(() => {
      useFlowExecutionStore.getState().startTracking('test-job-1');
    });

    it('should ignore traces for a different job', () => {
      useFlowExecutionStore.getState().handleTraceUpdate({
        id: 1,
        job_id: 'other-job',
        event_type: 'TASK_STARTED',
        trace_metadata: { crew_name: 'Crew A' },
      });

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.size).toBe(0);
    });

    it('should ignore non-task events', () => {
      useFlowExecutionStore.getState().handleTraceUpdate({
        id: 1,
        job_id: 'test-job-1',
        event_type: 'llm_call',
        trace_metadata: { crew_name: 'Crew A' },
      });

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.size).toBe(0);
    });

    it('should ignore traces without crew_name', () => {
      useFlowExecutionStore.getState().handleTraceUpdate({
        id: 1,
        job_id: 'test-job-1',
        event_type: 'TASK_STARTED',
        trace_metadata: {},
      });

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.size).toBe(0);
    });

    it('should set crew to running on TASK_STARTED', () => {
      useFlowExecutionStore.getState().handleTraceUpdate({
        id: 1,
        job_id: 'test-job-1',
        event_type: 'TASK_STARTED',
        trace_metadata: { crew_name: 'Research Crew' },
        created_at: '2024-01-01T00:00:00Z',
      });

      const state = useFlowExecutionStore.getState();
      const crewState = state.crewNodeStates.get('Research Crew');
      expect(crewState).toBeDefined();
      expect(crewState?.status).toBe('running');
      expect(crewState?.started_at).toBe('2024-01-01T00:00:00Z');
      expect(state.crewTaskCounts.get('Research Crew')).toBe(1);
    });

    it('should set crew to completed when all tasks finish', () => {
      const store = useFlowExecutionStore.getState();

      // Start a task
      store.handleTraceUpdate({
        id: 1,
        job_id: 'test-job-1',
        event_type: 'TASK_STARTED',
        trace_metadata: { crew_name: 'Crew A' },
        created_at: '2024-01-01T00:00:00Z',
      });

      // Complete the task
      store.handleTraceUpdate({
        id: 2,
        job_id: 'test-job-1',
        event_type: 'TASK_COMPLETED',
        trace_metadata: { crew_name: 'Crew A' },
        created_at: '2024-01-01T00:01:00Z',
      });

      const state = useFlowExecutionStore.getState();
      const crewState = state.crewNodeStates.get('Crew A');
      expect(crewState?.status).toBe('completed');
      expect(crewState?.completed_at).toBe('2024-01-01T00:01:00Z');
    });

    it('should not set crew to completed if tasks remain', () => {
      const store = useFlowExecutionStore.getState();

      // Start two tasks
      store.handleTraceUpdate({
        id: 1,
        job_id: 'test-job-1',
        event_type: 'TASK_STARTED',
        trace_metadata: { crew_name: 'Crew A' },
      });
      store.handleTraceUpdate({
        id: 2,
        job_id: 'test-job-1',
        event_type: 'TASK_STARTED',
        trace_metadata: { crew_name: 'Crew A' },
      });

      // Complete only one task
      store.handleTraceUpdate({
        id: 3,
        job_id: 'test-job-1',
        event_type: 'TASK_COMPLETED',
        trace_metadata: { crew_name: 'Crew A' },
      });

      const state = useFlowExecutionStore.getState();
      const crewState = state.crewNodeStates.get('Crew A');
      expect(crewState?.status).toBe('running');
    });

    it('should set crew to failed on TASK_FAILED', () => {
      const store = useFlowExecutionStore.getState();

      store.handleTraceUpdate({
        id: 1,
        job_id: 'test-job-1',
        event_type: 'TASK_STARTED',
        trace_metadata: { crew_name: 'Crew A' },
      });

      store.handleTraceUpdate({
        id: 2,
        job_id: 'test-job-1',
        event_type: 'TASK_FAILED',
        trace_metadata: { crew_name: 'Crew A' },
        created_at: '2024-01-01T00:00:30Z',
      });

      const state = useFlowExecutionStore.getState();
      const crewState = state.crewNodeStates.get('Crew A');
      expect(crewState?.status).toBe('failed');
      expect(crewState?.failed_at).toBe('2024-01-01T00:00:30Z');
    });

    it('should detect task_completion from event_context', () => {
      const store = useFlowExecutionStore.getState();

      store.handleTraceUpdate({
        id: 1,
        job_id: 'test-job-1',
        event_type: 'TASK_STARTED',
        trace_metadata: { crew_name: 'Crew A' },
      });

      // Backend sends event_context = 'task_completion' with a different event_type
      store.handleTraceUpdate({
        id: 2,
        job_id: 'test-job-1',
        event_type: 'some_event',
        event_context: 'task_completion',
        trace_metadata: { crew_name: 'Crew A' },
      });

      const state = useFlowExecutionStore.getState();
      const crewState = state.crewNodeStates.get('Crew A');
      expect(crewState?.status).toBe('completed');
    });

    it('should extract crew_name from extra_data as fallback', () => {
      useFlowExecutionStore.getState().handleTraceUpdate({
        id: 1,
        job_id: 'test-job-1',
        event_type: 'TASK_STARTED',
        trace_metadata: { extra_data: { crew_name: 'Legacy Crew' } },
      });

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.has('Legacy Crew')).toBe(true);
    });

    it('should fall back to agent_role if crew_name missing', () => {
      useFlowExecutionStore.getState().handleTraceUpdate({
        id: 1,
        job_id: 'test-job-1',
        event_type: 'TASK_STARTED',
        trace_metadata: { agent_role: 'News Gatherer' },
      });

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.has('News Gatherer')).toBe(true);
    });

    it('should track multiple crews independently', () => {
      const store = useFlowExecutionStore.getState();

      store.handleTraceUpdate({
        id: 1,
        job_id: 'test-job-1',
        event_type: 'TASK_STARTED',
        trace_metadata: { crew_name: 'Crew A' },
      });

      store.handleTraceUpdate({
        id: 2,
        job_id: 'test-job-1',
        event_type: 'TASK_STARTED',
        trace_metadata: { crew_name: 'Crew B' },
      });

      store.handleTraceUpdate({
        id: 3,
        job_id: 'test-job-1',
        event_type: 'TASK_COMPLETED',
        trace_metadata: { crew_name: 'Crew A' },
      });

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.get('Crew A')?.status).toBe('completed');
      expect(state.crewNodeStates.get('Crew B')?.status).toBe('running');
    });
  });

  describe('jobCompleted event handler - force crew completion', () => {
    it('should mark all running crews as completed when job completes', () => {
      // Set up store with running crews
      const crewNodeStates = new Map();
      crewNodeStates.set('Crew A', { status: 'running' as const, started_at: '2024-01-01T00:00:00Z' });
      crewNodeStates.set('Crew B', { status: 'running' as const, started_at: '2024-01-01T00:01:00Z' });
      crewNodeStates.set('Crew C', { status: 'completed' as const, completed_at: '2024-01-01T00:02:00Z' });

      useFlowExecutionStore.setState({
        currentJobId: 'job-123',
        isExecuting: true,
        crewNodeStates,
      });

      // Dispatch the jobCompleted event
      const event = new CustomEvent('jobCompleted', {
        detail: { jobId: 'job-123' },
      });
      window.dispatchEvent(event);

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.get('Crew A')?.status).toBe('completed');
      expect(state.crewNodeStates.get('Crew A')?.completed_at).toBeDefined();
      expect(state.crewNodeStates.get('Crew B')?.status).toBe('completed');
      expect(state.crewNodeStates.get('Crew B')?.completed_at).toBeDefined();
      // Already completed crew should remain completed
      expect(state.crewNodeStates.get('Crew C')?.status).toBe('completed');
      expect(state.crewNodeStates.get('Crew C')?.completed_at).toBe('2024-01-01T00:02:00Z');
    });

    it('should mark pending crews as completed when job completes', () => {
      const crewNodeStates = new Map();
      crewNodeStates.set('Crew A', { status: 'pending' as const });

      useFlowExecutionStore.setState({
        currentJobId: 'job-123',
        isExecuting: true,
        crewNodeStates,
      });

      window.dispatchEvent(new CustomEvent('jobCompleted', {
        detail: { jobId: 'job-123' },
      }));

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.get('Crew A')?.status).toBe('completed');
    });

    it('should not modify state for a different jobId', () => {
      const crewNodeStates = new Map();
      crewNodeStates.set('Crew A', { status: 'running' as const });

      useFlowExecutionStore.setState({
        currentJobId: 'job-123',
        isExecuting: true,
        crewNodeStates,
      });

      window.dispatchEvent(new CustomEvent('jobCompleted', {
        detail: { jobId: 'different-job' },
      }));

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.get('Crew A')?.status).toBe('running');
    });

    it('should not update state when no crews have running/pending status', () => {
      const crewNodeStates = new Map();
      crewNodeStates.set('Crew A', { status: 'completed' as const, completed_at: '2024-01-01T00:00:00Z' });
      crewNodeStates.set('Crew B', { status: 'failed' as const, failed_at: '2024-01-01T00:01:00Z' });

      useFlowExecutionStore.setState({
        currentJobId: 'job-123',
        isExecuting: true,
        crewNodeStates,
      });

      window.dispatchEvent(new CustomEvent('jobCompleted', {
        detail: { jobId: 'job-123' },
      }));

      const state = useFlowExecutionStore.getState();
      // Should remain unchanged
      expect(state.crewNodeStates.get('Crew A')?.status).toBe('completed');
      expect(state.crewNodeStates.get('Crew A')?.completed_at).toBe('2024-01-01T00:00:00Z');
      expect(state.crewNodeStates.get('Crew B')?.status).toBe('failed');
    });
  });

  describe('jobFailed event handler - force crew failure', () => {
    it('should mark all running crews as failed when job fails', () => {
      const crewNodeStates = new Map();
      crewNodeStates.set('Crew A', { status: 'running' as const, started_at: '2024-01-01T00:00:00Z' });
      crewNodeStates.set('Crew B', { status: 'running' as const, started_at: '2024-01-01T00:01:00Z' });
      crewNodeStates.set('Crew C', { status: 'completed' as const, completed_at: '2024-01-01T00:02:00Z' });

      useFlowExecutionStore.setState({
        currentJobId: 'job-456',
        isExecuting: true,
        crewNodeStates,
      });

      window.dispatchEvent(new CustomEvent('jobFailed', {
        detail: { jobId: 'job-456' },
      }));

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.get('Crew A')?.status).toBe('failed');
      expect(state.crewNodeStates.get('Crew A')?.failed_at).toBeDefined();
      expect(state.crewNodeStates.get('Crew B')?.status).toBe('failed');
      expect(state.crewNodeStates.get('Crew B')?.failed_at).toBeDefined();
      // Already completed crew should remain completed
      expect(state.crewNodeStates.get('Crew C')?.status).toBe('completed');
    });

    it('should mark pending crews as failed when job fails', () => {
      const crewNodeStates = new Map();
      crewNodeStates.set('Crew A', { status: 'pending' as const });

      useFlowExecutionStore.setState({
        currentJobId: 'job-456',
        isExecuting: true,
        crewNodeStates,
      });

      window.dispatchEvent(new CustomEvent('jobFailed', {
        detail: { jobId: 'job-456' },
      }));

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.get('Crew A')?.status).toBe('failed');
      expect(state.crewNodeStates.get('Crew A')?.failed_at).toBeDefined();
    });

    it('should not modify state for a different jobId', () => {
      const crewNodeStates = new Map();
      crewNodeStates.set('Crew A', { status: 'running' as const });

      useFlowExecutionStore.setState({
        currentJobId: 'job-456',
        isExecuting: true,
        crewNodeStates,
      });

      window.dispatchEvent(new CustomEvent('jobFailed', {
        detail: { jobId: 'different-job' },
      }));

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.get('Crew A')?.status).toBe('running');
    });

    it('should call stopTracking on job failure', () => {
      useFlowExecutionStore.setState({
        currentJobId: 'job-456',
        isExecuting: true,
        crewNodeStates: new Map(),
      });

      window.dispatchEvent(new CustomEvent('jobFailed', {
        detail: { jobId: 'job-456' },
      }));

      const state = useFlowExecutionStore.getState();
      expect(state.isExecuting).toBe(false);
    });
  });

  describe('jobCreated event handler - clear previous state', () => {
    it('should clear all crew node states when a new job is created', () => {
      const crewNodeStates = new Map();
      crewNodeStates.set('Crew A', { status: 'completed' as const });
      useFlowExecutionStore.setState({
        currentJobId: 'old-job',
        isExecuting: false,
        crewNodeStates,
      });

      window.dispatchEvent(new CustomEvent('jobCreated', {
        detail: { jobId: 'new-job-789' },
      }));

      const state = useFlowExecutionStore.getState();
      // clearStates should have been called
      expect(state.crewNodeStates.size).toBe(0);
      expect(state.currentJobId).toBeNull();
    });

    it('should start tracking if job is a flow execution', () => {
      window.dispatchEvent(new CustomEvent('jobCreated', {
        detail: { jobId: 'flow-job-1', isFlow: true },
      }));

      const state = useFlowExecutionStore.getState();
      expect(state.currentJobId).toBe('flow-job-1');
      expect(state.isExecuting).toBe(true);
    });

    it('should not start tracking for non-flow jobs', () => {
      window.dispatchEvent(new CustomEvent('jobCreated', {
        detail: { jobId: 'crew-job-1', isFlow: false },
      }));

      const state = useFlowExecutionStore.getState();
      // clearStates was called, so currentJobId is null
      expect(state.currentJobId).toBeNull();
      expect(state.isExecuting).toBe(false);
    });
  });

  describe('traceUpdate event handler', () => {
    it('should process trace updates from SSE via window event', () => {
      useFlowExecutionStore.getState().startTracking('sse-job-1');

      window.dispatchEvent(new CustomEvent('traceUpdate', {
        detail: {
          trace: {
            id: 1,
            job_id: 'sse-job-1',
            event_type: 'TASK_STARTED',
            trace_metadata: { crew_name: 'SSE Crew' },
          },
        },
      }));

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.has('SSE Crew')).toBe(true);
      expect(state.crewNodeStates.get('SSE Crew')?.status).toBe('running');
    });
  });

  describe('loadCrewStates', () => {
    const mockGet = apiClient.get as ReturnType<typeof vi.fn>;

    it('should fetch traces from API and process them into crew states', async () => {
      mockGet.mockResolvedValue({
        data: {
          traces: [
            {
              id: 1,
              job_id: 'load-job-1',
              event_type: 'TASK_STARTED',
              trace_metadata: { crew_name: 'Alpha Crew' },
              created_at: '2024-06-01T10:00:00Z',
            },
            {
              id: 2,
              job_id: 'load-job-1',
              event_type: 'TASK_COMPLETED',
              trace_metadata: { crew_name: 'Alpha Crew' },
              created_at: '2024-06-01T10:01:00Z',
            },
            {
              id: 3,
              job_id: 'load-job-1',
              event_type: 'TASK_STARTED',
              trace_metadata: { crew_name: 'Beta Crew' },
              created_at: '2024-06-01T10:00:30Z',
            },
          ],
        },
      });

      await useFlowExecutionStore.getState().loadCrewStates('load-job-1');

      expect(mockGet).toHaveBeenCalledWith('/traces/job/load-job-1', {
        params: { limit: 500, offset: 0 },
      });

      const state = useFlowExecutionStore.getState();
      expect(state.currentJobId).toBe('load-job-1');
      expect(state.isExecuting).toBe(true);

      // Alpha Crew: 1 started + 1 completed = completed
      const alpha = state.crewNodeStates.get('Alpha Crew');
      expect(alpha).toBeDefined();
      expect(alpha?.status).toBe('completed');
      expect(alpha?.completed_at).toBe('2024-06-01T10:01:00Z');

      // Beta Crew: 1 started, 0 completed = running
      const beta = state.crewNodeStates.get('Beta Crew');
      expect(beta).toBeDefined();
      expect(beta?.status).toBe('running');
      expect(beta?.started_at).toBe('2024-06-01T10:00:30Z');
    });

    it('should handle API errors gracefully without crashing', async () => {
      mockGet.mockRejectedValue(new Error('Network failure'));

      // Should not throw
      await useFlowExecutionStore.getState().loadCrewStates('err-job');

      // State should remain at initial (no update applied)
      const state = useFlowExecutionStore.getState();
      expect(state.currentJobId).toBeNull();
      expect(state.crewNodeStates.size).toBe(0);
    });

    it('should handle empty traces response', async () => {
      mockGet.mockResolvedValue({
        data: { traces: [] },
      });

      await useFlowExecutionStore.getState().loadCrewStates('empty-job');

      // loadCrewStates still sets currentJobId and isExecuting even with no traces
      const state = useFlowExecutionStore.getState();
      expect(state.currentJobId).toBe('empty-job');
      expect(state.isExecuting).toBe(true);
      expect(state.crewNodeStates.size).toBe(0);
    });

    it('should handle response with no traces key', async () => {
      mockGet.mockResolvedValue({
        data: {},
      });

      await useFlowExecutionStore.getState().loadCrewStates('no-traces-job');

      // Should early-return without updating state
      const state = useFlowExecutionStore.getState();
      expect(state.currentJobId).toBeNull();
    });

    it('should handle null data response', async () => {
      mockGet.mockResolvedValue({
        data: null,
      });

      await useFlowExecutionStore.getState().loadCrewStates('null-data-job');

      const state = useFlowExecutionStore.getState();
      expect(state.currentJobId).toBeNull();
    });

    it('should skip non-task events in loaded traces', async () => {
      mockGet.mockResolvedValue({
        data: {
          traces: [
            {
              id: 1,
              job_id: 'skip-job',
              event_type: 'LLM_CALL',
              trace_metadata: { crew_name: 'Crew X' },
              created_at: '2024-06-01T10:00:00Z',
            },
            {
              id: 2,
              job_id: 'skip-job',
              event_type: 'TASK_STARTED',
              trace_metadata: { crew_name: 'Crew X' },
              created_at: '2024-06-01T10:00:01Z',
            },
          ],
        },
      });

      await useFlowExecutionStore.getState().loadCrewStates('skip-job');

      const state = useFlowExecutionStore.getState();
      const crewX = state.crewNodeStates.get('Crew X');
      expect(crewX).toBeDefined();
      expect(crewX?.status).toBe('running');
      expect(state.crewTaskCounts.get('Crew X')).toBe(1);
    });

    it('should skip traces without crew_name or agent_role', async () => {
      mockGet.mockResolvedValue({
        data: {
          traces: [
            {
              id: 1,
              job_id: 'no-crew-job',
              event_type: 'TASK_STARTED',
              trace_metadata: {},
              created_at: '2024-06-01T10:00:00Z',
            },
            {
              id: 2,
              job_id: 'no-crew-job',
              event_type: 'TASK_STARTED',
              trace_metadata: { crew_name: 'Valid Crew' },
              created_at: '2024-06-01T10:00:01Z',
            },
          ],
        },
      });

      await useFlowExecutionStore.getState().loadCrewStates('no-crew-job');

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.size).toBe(1);
      expect(state.crewNodeStates.has('Valid Crew')).toBe(true);
    });

    it('should extract crew_name from extra_data fallback', async () => {
      mockGet.mockResolvedValue({
        data: {
          traces: [
            {
              id: 1,
              job_id: 'legacy-job',
              event_type: 'TASK_STARTED',
              trace_metadata: { extra_data: { crew_name: 'Legacy Crew' } },
              created_at: '2024-06-01T10:00:00Z',
            },
          ],
        },
      });

      await useFlowExecutionStore.getState().loadCrewStates('legacy-job');

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.has('Legacy Crew')).toBe(true);
    });

    it('should fall back to agent_role when crew_name is missing', async () => {
      mockGet.mockResolvedValue({
        data: {
          traces: [
            {
              id: 1,
              job_id: 'agent-role-job',
              event_type: 'TASK_STARTED',
              trace_metadata: { agent_role: 'Data Analyst' },
              created_at: '2024-06-01T10:00:00Z',
            },
          ],
        },
      });

      await useFlowExecutionStore.getState().loadCrewStates('agent-role-job');

      const state = useFlowExecutionStore.getState();
      expect(state.crewNodeStates.has('Data Analyst')).toBe(true);
    });

    it('should correctly handle TASK_FAILED traces', async () => {
      mockGet.mockResolvedValue({
        data: {
          traces: [
            {
              id: 1,
              job_id: 'fail-job',
              event_type: 'TASK_STARTED',
              trace_metadata: { crew_name: 'Failing Crew' },
              created_at: '2024-06-01T10:00:00Z',
            },
            {
              id: 2,
              job_id: 'fail-job',
              event_type: 'TASK_FAILED',
              trace_metadata: { crew_name: 'Failing Crew' },
              created_at: '2024-06-01T10:00:30Z',
            },
          ],
        },
      });

      await useFlowExecutionStore.getState().loadCrewStates('fail-job');

      const state = useFlowExecutionStore.getState();
      const crew = state.crewNodeStates.get('Failing Crew');
      expect(crew?.status).toBe('failed');
      expect(crew?.failed_at).toBe('2024-06-01T10:00:30Z');
      expect(state.crewFailed.has('Failing Crew')).toBe(true);
    });

    it('should track multiple crews from loaded traces', async () => {
      mockGet.mockResolvedValue({
        data: {
          traces: [
            {
              id: 1,
              job_id: 'multi-job',
              event_type: 'TASK_STARTED',
              trace_metadata: { crew_name: 'Crew 1' },
              created_at: '2024-06-01T10:00:00Z',
            },
            {
              id: 2,
              job_id: 'multi-job',
              event_type: 'TASK_COMPLETED',
              trace_metadata: { crew_name: 'Crew 1' },
              created_at: '2024-06-01T10:01:00Z',
            },
            {
              id: 3,
              job_id: 'multi-job',
              event_type: 'TASK_STARTED',
              trace_metadata: { crew_name: 'Crew 2' },
              created_at: '2024-06-01T10:00:30Z',
            },
            {
              id: 4,
              job_id: 'multi-job',
              event_type: 'TASK_STARTED',
              trace_metadata: { crew_name: 'Crew 2' },
              created_at: '2024-06-01T10:01:30Z',
            },
            {
              id: 5,
              job_id: 'multi-job',
              event_type: 'TASK_COMPLETED',
              trace_metadata: { crew_name: 'Crew 2' },
              created_at: '2024-06-01T10:02:00Z',
            },
          ],
        },
      });

      await useFlowExecutionStore.getState().loadCrewStates('multi-job');

      const state = useFlowExecutionStore.getState();

      // Crew 1: 1 started, 1 completed -> completed
      expect(state.crewNodeStates.get('Crew 1')?.status).toBe('completed');

      // Crew 2: 2 started, 1 completed -> still running (1 remaining)
      expect(state.crewNodeStates.get('Crew 2')?.status).toBe('running');
      expect(state.crewTaskCounts.get('Crew 2')).toBe(2);
      expect(state.crewCompletedTasks.get('Crew 2')).toBe(1);
    });
  });
});

/**
 * Unit tests for flowExecutionStore.
 *
 * Tests the flow execution tracking, crew node state management,
 * and the jobCompleted/jobFailed event handlers that force crew node
 * states to their terminal state when the SSE connection races ahead
 * of final trace delivery.
 */
import { describe, it, expect, beforeEach } from 'vitest';
import { useFlowExecutionStore } from './flowExecutionStore';

describe('flowExecutionStore', () => {
  beforeEach(() => {
    // Reset the store to initial state before each test
    useFlowExecutionStore.setState({
      currentJobId: null,
      crewNodeStates: new Map(),
      isExecuting: false,
      crewTaskCounts: new Map(),
      crewCompletedTasks: new Map(),
      crewFailed: new Set(),
    });
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
});

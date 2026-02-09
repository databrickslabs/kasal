/**
 * Unit tests for runStatus store.
 *
 * Tests the SSE-driven state management: handleSSEUpdate, addTrace,
 * SSE connection state, fetchInitialRunHistory, and processedCompletions
 * deduplication logic.
 */
import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { useRunStatusStore } from './runStatus';
import { Trace } from '../types/trace';
import { ExtendedRun } from '../types/run';

// Mock the runService used by fetchInitialRunHistory
vi.mock('../api/ExecutionHistoryService', () => ({
  runService: {
    getRuns: vi.fn(),
    invalidateRunsCache: vi.fn(),
  },
}));

import { runService } from '../api/ExecutionHistoryService';

describe('runStatus store', () => {
  beforeEach(() => {
    // Reset the store to a clean initial state before each test
    useRunStatusStore.setState({
      currentRun: null,
      isTracking: false,
      error: null,
      isLoading: false,
      runHistory: [],
      activeRuns: {},
      lastFetchTime: Date.now(),
      hasRunningJobs: false,
      processedCompletions: new Set<string>(),
      sseEnabled: true,
      sseConnected: false,
      sseError: null,
      traces: new Map<string, Trace[]>(),
    });

    // Clear any mocked function state
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // -------------------------------------------------------
  // addTrace
  // -------------------------------------------------------
  describe('addTrace', () => {
    it('should add a trace to the correct job', () => {
      const trace: Trace = {
        id: 1,
        event_source: 'crewai',
        event_context: 'task_execution',
        event_type: 'TASK_STARTED',
        output: null,
        created_at: '2024-06-01T10:00:00Z',
      };

      useRunStatusStore.getState().addTrace('job-1', trace);

      const traces = useRunStatusStore.getState().traces.get('job-1');
      expect(traces).toBeDefined();
      expect(traces).toHaveLength(1);
      expect(traces![0]).toEqual(trace);
    });

    it('should append traces to an existing job', () => {
      const trace1: Trace = {
        id: 1,
        event_source: 'crewai',
        event_context: 'task_execution',
        event_type: 'TASK_STARTED',
        output: null,
        created_at: '2024-06-01T10:00:00Z',
      };
      const trace2: Trace = {
        id: 2,
        event_source: 'crewai',
        event_context: 'task_completion',
        event_type: 'TASK_COMPLETED',
        output: 'result',
        created_at: '2024-06-01T10:01:00Z',
      };

      const store = useRunStatusStore.getState();
      store.addTrace('job-1', trace1);
      store.addTrace('job-1', trace2);

      const traces = useRunStatusStore.getState().traces.get('job-1');
      expect(traces).toHaveLength(2);
      expect(traces![0].event_type).toBe('TASK_STARTED');
      expect(traces![1].event_type).toBe('TASK_COMPLETED');
    });

    it('should not add duplicate traces (same created_at, event_type, event_source)', () => {
      const trace: Trace = {
        id: 1,
        event_source: 'crewai',
        event_context: 'task_execution',
        event_type: 'TASK_STARTED',
        output: null,
        created_at: '2024-06-01T10:00:00Z',
      };

      const store = useRunStatusStore.getState();
      store.addTrace('job-1', trace);
      store.addTrace('job-1', trace);
      store.addTrace('job-1', { ...trace, id: 99 }); // same signature, different id

      const traces = useRunStatusStore.getState().traces.get('job-1');
      expect(traces).toHaveLength(1);
    });

    it('should keep traces for different jobs separate', () => {
      const traceA: Trace = {
        id: 1,
        event_source: 'crewai',
        event_context: 'ctx',
        event_type: 'TASK_STARTED',
        output: null,
        created_at: '2024-06-01T10:00:00Z',
      };
      const traceB: Trace = {
        id: 2,
        event_source: 'crewai',
        event_context: 'ctx',
        event_type: 'TASK_COMPLETED',
        output: null,
        created_at: '2024-06-01T10:01:00Z',
      };

      const store = useRunStatusStore.getState();
      store.addTrace('job-a', traceA);
      store.addTrace('job-b', traceB);

      expect(useRunStatusStore.getState().traces.get('job-a')).toHaveLength(1);
      expect(useRunStatusStore.getState().traces.get('job-b')).toHaveLength(1);
    });
  });

  // -------------------------------------------------------
  // setTracesForJob / getTracesForJob / clearTracesForJob
  // -------------------------------------------------------
  describe('trace management helpers', () => {
    it('setTracesForJob should replace all traces for a job', () => {
      const traces: Trace[] = [
        { id: 1, event_source: 'a', event_context: 'b', event_type: 'c', output: null, created_at: '2024-01-01T00:00:00Z' },
        { id: 2, event_source: 'a', event_context: 'b', event_type: 'd', output: null, created_at: '2024-01-01T00:01:00Z' },
      ];

      useRunStatusStore.getState().setTracesForJob('job-x', traces);

      expect(useRunStatusStore.getState().traces.get('job-x')).toHaveLength(2);
    });

    it('getTracesForJob should return empty array for unknown job', () => {
      const traces = useRunStatusStore.getState().getTracesForJob('nonexistent');
      expect(traces).toEqual([]);
    });

    it('clearTracesForJob should remove traces for specified job only', () => {
      const store = useRunStatusStore.getState();
      store.setTracesForJob('job-1', [
        { id: 1, event_source: 'a', event_context: 'b', event_type: 'c', output: null, created_at: '2024-01-01T00:00:00Z' },
      ]);
      store.setTracesForJob('job-2', [
        { id: 2, event_source: 'a', event_context: 'b', event_type: 'd', output: null, created_at: '2024-01-01T00:01:00Z' },
      ]);

      useRunStatusStore.getState().clearTracesForJob('job-1');

      expect(useRunStatusStore.getState().traces.get('job-1')).toBeUndefined();
      expect(useRunStatusStore.getState().traces.get('job-2')).toHaveLength(1);
    });
  });

  // -------------------------------------------------------
  // handleSSEUpdate
  // -------------------------------------------------------
  describe('handleSSEUpdate', () => {
    it('should update an existing job in runHistory when status changes', () => {
      const existingRun: ExtendedRun = {
        id: 'job-100',
        job_id: 'job-100',
        status: 'running',
        created_at: '2024-06-01T10:00:00Z',
        updated_at: '2024-06-01T10:00:00Z',
        run_name: 'My Run',
        agents_yaml: '',
        tasks_yaml: '',
      };

      useRunStatusStore.setState({
        runHistory: [existingRun],
        activeRuns: { 'job-100': existingRun },
      });

      useRunStatusStore.getState().handleSSEUpdate({
        job_id: 'job-100',
        status: 'completed',
        result: { output: 'done' },
        completed_at: '2024-06-01T10:05:00Z',
      });

      const state = useRunStatusStore.getState();
      const updatedRun = state.runHistory.find(r => r.job_id === 'job-100');
      expect(updatedRun).toBeDefined();
      expect(updatedRun!.status).toBe('completed');
      expect(updatedRun!.result).toEqual({ output: 'done' });
    });

    it('should add a new run to history if job_id is not found', () => {
      useRunStatusStore.getState().handleSSEUpdate({
        job_id: 'new-job-999',
        status: 'running',
        run_name: 'Brand New Run',
        created_at: '2024-06-01T12:00:00Z',
      });

      const state = useRunStatusStore.getState();
      expect(state.runHistory).toHaveLength(1);
      expect(state.runHistory[0].job_id).toBe('new-job-999');
      expect(state.runHistory[0].status).toBe('running');
      expect(state.runHistory[0].run_name).toBe('Brand New Run');
    });

    it('should add running jobs to activeRuns', () => {
      useRunStatusStore.getState().handleSSEUpdate({
        job_id: 'job-active',
        status: 'running',
      });

      const state = useRunStatusStore.getState();
      expect(state.activeRuns['job-active']).toBeDefined();
      expect(state.activeRuns['job-active'].status).toBe('running');
      expect(state.hasRunningJobs).toBe(true);
    });

    it('should add queued jobs to activeRuns', () => {
      useRunStatusStore.getState().handleSSEUpdate({
        job_id: 'job-queued',
        status: 'queued',
      });

      const state = useRunStatusStore.getState();
      expect(state.activeRuns['job-queued']).toBeDefined();
      expect(state.hasRunningJobs).toBe(true);
    });

    it('should remove completed jobs from activeRuns', () => {
      // Set up an active running job
      const run: ExtendedRun = {
        id: 'job-done',
        job_id: 'job-done',
        status: 'running',
        created_at: '2024-06-01T10:00:00Z',
        updated_at: '2024-06-01T10:00:00Z',
        run_name: 'Run',
        agents_yaml: '',
        tasks_yaml: '',
      };
      useRunStatusStore.setState({
        runHistory: [run],
        activeRuns: { 'job-done': run },
        hasRunningJobs: true,
      });

      useRunStatusStore.getState().handleSSEUpdate({
        job_id: 'job-done',
        status: 'completed',
      });

      const state = useRunStatusStore.getState();
      expect(state.activeRuns['job-done']).toBeUndefined();
      expect(state.hasRunningJobs).toBe(false);
    });

    it('should remove failed jobs from activeRuns', () => {
      const run: ExtendedRun = {
        id: 'job-fail',
        job_id: 'job-fail',
        status: 'running',
        created_at: '2024-06-01T10:00:00Z',
        updated_at: '2024-06-01T10:00:00Z',
        run_name: 'Run',
        agents_yaml: '',
        tasks_yaml: '',
      };
      useRunStatusStore.setState({
        runHistory: [run],
        activeRuns: { 'job-fail': run },
      });

      useRunStatusStore.getState().handleSSEUpdate({
        job_id: 'job-fail',
        status: 'failed',
        error: 'something went wrong',
      });

      const state = useRunStatusStore.getState();
      expect(state.activeRuns['job-fail']).toBeUndefined();
    });

    it('should dispatch jobCompleted event for completed jobs', () => {
      const eventSpy = vi.fn();
      window.addEventListener('jobCompleted', eventSpy);

      useRunStatusStore.getState().handleSSEUpdate({
        job_id: 'job-evt-comp',
        status: 'completed',
        result: { output: 'success' },
      });

      expect(eventSpy).toHaveBeenCalledTimes(1);
      const detail = (eventSpy.mock.calls[0][0] as CustomEvent).detail;
      expect(detail.jobId).toBe('job-evt-comp');
      expect(detail.result).toEqual({ output: 'success' });

      window.removeEventListener('jobCompleted', eventSpy);
    });

    it('should dispatch jobFailed event for failed jobs', () => {
      const eventSpy = vi.fn();
      window.addEventListener('jobFailed', eventSpy);

      useRunStatusStore.getState().handleSSEUpdate({
        job_id: 'job-evt-fail',
        status: 'failed',
        error: 'crash',
      });

      expect(eventSpy).toHaveBeenCalledTimes(1);
      const detail = (eventSpy.mock.calls[0][0] as CustomEvent).detail;
      expect(detail.jobId).toBe('job-evt-fail');
      expect(detail.error).toBe('crash');

      window.removeEventListener('jobFailed', eventSpy);
    });

    it('should dispatch jobStopped event for stopped jobs', () => {
      const eventSpy = vi.fn();
      window.addEventListener('jobStopped', eventSpy);

      useRunStatusStore.getState().handleSSEUpdate({
        job_id: 'job-evt-stop',
        status: 'stopped',
      });

      expect(eventSpy).toHaveBeenCalledTimes(1);
      const detail = (eventSpy.mock.calls[0][0] as CustomEvent).detail;
      expect(detail.jobId).toBe('job-evt-stop');
      expect(detail.status).toBe('stopped');

      window.removeEventListener('jobStopped', eventSpy);
    });

    it('should not dispatch duplicate completion events (processedCompletions)', () => {
      const eventSpy = vi.fn();
      window.addEventListener('jobCompleted', eventSpy);

      const store = useRunStatusStore.getState();
      store.handleSSEUpdate({
        job_id: 'job-dup',
        status: 'completed',
      });

      // Send the same completion again
      useRunStatusStore.getState().handleSSEUpdate({
        job_id: 'job-dup',
        status: 'completed',
      });

      // Should only have been dispatched once
      expect(eventSpy).toHaveBeenCalledTimes(1);

      // processedCompletions should contain the key
      const state = useRunStatusStore.getState();
      expect(state.processedCompletions.has('job-dup-completed')).toBe(true);

      window.removeEventListener('jobCompleted', eventSpy);
    });

    it('should ignore updates without job_id or status', () => {
      const stateBefore = useRunStatusStore.getState().runHistory.length;

      useRunStatusStore.getState().handleSSEUpdate({ message: 'heartbeat' });
      useRunStatusStore.getState().handleSSEUpdate({ job_id: 'x' }); // no status
      useRunStatusStore.getState().handleSSEUpdate({ status: 'running' }); // no job_id

      expect(useRunStatusStore.getState().runHistory.length).toBe(stateBefore);
    });

    it('should use message field from SSE data as error', () => {
      useRunStatusStore.getState().handleSSEUpdate({
        job_id: 'job-msg',
        status: 'failed',
        message: 'custom error message',
      });

      const run = useRunStatusStore.getState().runHistory.find(r => r.job_id === 'job-msg');
      expect(run).toBeDefined();
      expect(run!.error).toBe('custom error message');
    });

    it('should generate a truncated run_name if none is provided', () => {
      useRunStatusStore.getState().handleSSEUpdate({
        job_id: 'abcdefghij-long-id',
        status: 'running',
      });

      const run = useRunStatusStore.getState().runHistory.find(r => r.job_id === 'abcdefghij-long-id');
      expect(run).toBeDefined();
      expect(run!.run_name).toBe('Run abcdefgh');
    });
  });

  // -------------------------------------------------------
  // setSSEConnected / setSSEError
  // -------------------------------------------------------
  describe('SSE connection state management', () => {
    it('setSSEConnected(true) should set connected and clear error', () => {
      useRunStatusStore.setState({ sseError: 'previous error' });

      useRunStatusStore.getState().setSSEConnected(true);

      const state = useRunStatusStore.getState();
      expect(state.sseConnected).toBe(true);
      expect(state.sseError).toBeNull();
    });

    it('setSSEConnected(false) should set disconnected but keep existing error', () => {
      useRunStatusStore.setState({ sseConnected: true, sseError: 'connection lost' });

      useRunStatusStore.getState().setSSEConnected(false);

      const state = useRunStatusStore.getState();
      expect(state.sseConnected).toBe(false);
      expect(state.sseError).toBe('connection lost');
    });

    it('setSSEConnected(false) should not introduce error if none existed', () => {
      useRunStatusStore.setState({ sseConnected: true, sseError: null });

      useRunStatusStore.getState().setSSEConnected(false);

      const state = useRunStatusStore.getState();
      expect(state.sseConnected).toBe(false);
      expect(state.sseError).toBeNull();
    });

    it('setSSEError should set error message', () => {
      useRunStatusStore.getState().setSSEError('timeout');

      expect(useRunStatusStore.getState().sseError).toBe('timeout');
    });

    it('setSSEError(null) should clear error', () => {
      useRunStatusStore.setState({ sseError: 'old error' });

      useRunStatusStore.getState().setSSEError(null);

      expect(useRunStatusStore.getState().sseError).toBeNull();
    });
  });

  // -------------------------------------------------------
  // fetchInitialRunHistory
  // -------------------------------------------------------
  describe('fetchInitialRunHistory', () => {
    const mockGetRuns = runService.getRuns as ReturnType<typeof vi.fn>;

    it('should populate runHistory from API response', async () => {
      // Set up localStorage for group filtering
      const getItemSpy = vi.spyOn(Storage.prototype, 'getItem').mockReturnValue('group-1');

      mockGetRuns.mockResolvedValue({
        runs: [
          {
            id: '1',
            job_id: 'job-a',
            status: 'completed',
            created_at: '2024-06-01T10:00:00Z',
            updated_at: '2024-06-01T10:05:00Z',
            completed_at: '2024-06-01T10:05:00Z',
            run_name: 'Run A',
            agents_yaml: '',
            tasks_yaml: '',
            group_id: 'group-1',
          },
          {
            id: '2',
            job_id: 'job-b',
            status: 'running',
            created_at: '2024-06-01T11:00:00Z',
            updated_at: '2024-06-01T11:01:00Z',
            run_name: 'Run B',
            agents_yaml: '',
            tasks_yaml: '',
            group_id: 'group-1',
          },
        ],
        total: 2,
        limit: 50,
        offset: 0,
      });

      await useRunStatusStore.getState().fetchInitialRunHistory();

      const state = useRunStatusStore.getState();
      expect(state.isLoading).toBe(false);
      expect(state.error).toBeNull();
      expect(state.runHistory).toHaveLength(2);
      expect(state.hasRunningJobs).toBe(true);

      // Running job should be in activeRuns
      expect(state.activeRuns['job-b']).toBeDefined();
      // Completed job should not be in activeRuns
      expect(state.activeRuns['job-a']).toBeUndefined();

      getItemSpy.mockRestore();
    });

    it('should filter out runs from other groups (security)', async () => {
      const getItemSpy = vi.spyOn(Storage.prototype, 'getItem').mockReturnValue('group-1');

      mockGetRuns.mockResolvedValue({
        runs: [
          {
            id: '1',
            job_id: 'job-mine',
            status: 'completed',
            created_at: '2024-06-01T10:00:00Z',
            updated_at: '2024-06-01T10:05:00Z',
            completed_at: '2024-06-01T10:05:00Z',
            run_name: 'My Run',
            agents_yaml: '',
            tasks_yaml: '',
            group_id: 'group-1',
          },
          {
            id: '2',
            job_id: 'job-other',
            status: 'completed',
            created_at: '2024-06-01T10:00:00Z',
            updated_at: '2024-06-01T10:05:00Z',
            completed_at: '2024-06-01T10:05:00Z',
            run_name: 'Other Run',
            agents_yaml: '',
            tasks_yaml: '',
            group_id: 'group-2', // different group
          },
        ],
        total: 2,
        limit: 50,
        offset: 0,
      });

      await useRunStatusStore.getState().fetchInitialRunHistory();

      const state = useRunStatusStore.getState();
      expect(state.runHistory).toHaveLength(1);
      expect(state.runHistory[0].job_id).toBe('job-mine');

      getItemSpy.mockRestore();
    });

    it('should handle API errors gracefully', async () => {
      mockGetRuns.mockRejectedValue(new Error('Network error'));

      await useRunStatusStore.getState().fetchInitialRunHistory();

      const state = useRunStatusStore.getState();
      expect(state.isLoading).toBe(false);
      expect(state.error).toBe('Failed to fetch run history: Network error');
      expect(state.runHistory).toHaveLength(0);
    });

    it('should set isLoading true during fetch', async () => {
      let resolvePromise: (value: unknown) => void;
      const pendingPromise = new Promise(resolve => {
        resolvePromise = resolve;
      });
      mockGetRuns.mockReturnValue(pendingPromise);

      const fetchPromise = useRunStatusStore.getState().fetchInitialRunHistory();

      // While fetching, isLoading should be true
      expect(useRunStatusStore.getState().isLoading).toBe(true);

      // Resolve the promise
      resolvePromise!({ runs: [], total: 0, limit: 50, offset: 0 });
      await fetchPromise;

      expect(useRunStatusStore.getState().isLoading).toBe(false);
    });

    it('should preserve placeholder running runs not yet in API response', async () => {
      const getItemSpy = vi.spyOn(Storage.prototype, 'getItem').mockReturnValue('group-1');

      // Pre-populate store with a placeholder running run (just created via SSE)
      const placeholderRun: ExtendedRun = {
        id: 'job-placeholder',
        job_id: 'job-placeholder',
        status: 'running',
        created_at: '2024-06-01T12:00:00Z',
        updated_at: '2024-06-01T12:00:00Z',
        run_name: 'Placeholder Run',
        agents_yaml: '',
        tasks_yaml: '',
      };
      useRunStatusStore.setState({
        runHistory: [placeholderRun],
        activeRuns: { 'job-placeholder': placeholderRun },
        hasRunningJobs: true,
      });

      // API returns only an older completed run (placeholder not yet visible)
      mockGetRuns.mockResolvedValue({
        runs: [
          {
            id: '1',
            job_id: 'job-old',
            status: 'completed',
            created_at: '2024-06-01T09:00:00Z',
            updated_at: '2024-06-01T09:05:00Z',
            completed_at: '2024-06-01T09:05:00Z',
            run_name: 'Old Completed Run',
            agents_yaml: '',
            tasks_yaml: '',
            group_id: 'group-1',
          },
        ],
        total: 1,
        limit: 50,
        offset: 0,
      });

      await useRunStatusStore.getState().fetchInitialRunHistory();

      const state = useRunStatusStore.getState();
      // Should contain both the API run AND the placeholder
      expect(state.runHistory).toHaveLength(2);
      expect(state.runHistory.some(r => r.job_id === 'job-placeholder')).toBe(true);
      expect(state.runHistory.some(r => r.job_id === 'job-old')).toBe(true);
      expect(state.hasRunningJobs).toBe(true);
      expect(state.activeRuns['job-placeholder']).toBeDefined();

      getItemSpy.mockRestore();
    });

    it('should not duplicate placeholder run when API includes it', async () => {
      const getItemSpy = vi.spyOn(Storage.prototype, 'getItem').mockReturnValue('group-1');

      // Pre-populate store with a placeholder running run
      const placeholderRun: ExtendedRun = {
        id: 'job-now-visible',
        job_id: 'job-now-visible',
        status: 'running',
        created_at: '2024-06-01T12:00:00Z',
        updated_at: '2024-06-01T12:00:00Z',
        run_name: 'Placeholder',
        agents_yaml: '',
        tasks_yaml: '',
      };
      useRunStatusStore.setState({
        runHistory: [placeholderRun],
        activeRuns: { 'job-now-visible': placeholderRun },
      });

      // API now includes this run (no longer a timing gap)
      mockGetRuns.mockResolvedValue({
        runs: [
          {
            id: 'job-now-visible',
            job_id: 'job-now-visible',
            status: 'running',
            created_at: '2024-06-01T12:00:00Z',
            updated_at: '2024-06-01T12:01:00Z',
            run_name: 'Visible Run',
            agents_yaml: '',
            tasks_yaml: '',
            group_id: 'group-1',
          },
        ],
        total: 1,
        limit: 50,
        offset: 0,
      });

      await useRunStatusStore.getState().fetchInitialRunHistory();

      const state = useRunStatusStore.getState();
      // Should NOT duplicate - only 1 entry
      expect(state.runHistory).toHaveLength(1);
      expect(state.runHistory[0].job_id).toBe('job-now-visible');

      getItemSpy.mockRestore();
    });

    it('should drop completed placeholder runs not in API response', async () => {
      const getItemSpy = vi.spyOn(Storage.prototype, 'getItem').mockReturnValue('group-1');

      // Pre-populate with a completed placeholder (should NOT be preserved)
      const completedPlaceholder: ExtendedRun = {
        id: 'job-done',
        job_id: 'job-done',
        status: 'completed',
        created_at: '2024-06-01T11:00:00Z',
        updated_at: '2024-06-01T11:05:00Z',
        run_name: 'Done Placeholder',
        agents_yaml: '',
        tasks_yaml: '',
      };
      useRunStatusStore.setState({ runHistory: [completedPlaceholder] });

      mockGetRuns.mockResolvedValue({
        runs: [],
        total: 0,
        limit: 50,
        offset: 0,
      });

      await useRunStatusStore.getState().fetchInitialRunHistory();

      const state = useRunStatusStore.getState();
      // Completed placeholder should NOT be preserved
      expect(state.runHistory).toHaveLength(0);

      getItemSpy.mockRestore();
    });

    it('should ensure completed_at is set for completed runs without it', async () => {
      const getItemSpy = vi.spyOn(Storage.prototype, 'getItem').mockReturnValue('group-1');

      mockGetRuns.mockResolvedValue({
        runs: [
          {
            id: '1',
            job_id: 'job-notime',
            status: 'completed',
            created_at: '2024-06-01T10:00:00Z',
            updated_at: '2024-06-01T10:05:00Z',
            // completed_at is intentionally missing
            run_name: 'No Time Run',
            agents_yaml: '',
            tasks_yaml: '',
            group_id: 'group-1',
          },
        ],
        total: 1,
        limit: 50,
        offset: 0,
      });

      await useRunStatusStore.getState().fetchInitialRunHistory();

      const state = useRunStatusStore.getState();
      const run = state.runHistory[0];
      expect(run.completed_at).toBeDefined();
      expect(run.completed_at).toBe('2024-06-01T10:05:00Z'); // should use updated_at

      getItemSpy.mockRestore();
    });
  });

  // -------------------------------------------------------
  // processedCompletions deduplication
  // -------------------------------------------------------
  describe('processedCompletions deduplication', () => {
    it('should track completion keys across multiple handleSSEUpdate calls', () => {
      useRunStatusStore.getState().handleSSEUpdate({ job_id: 'j1', status: 'completed' });
      useRunStatusStore.getState().handleSSEUpdate({ job_id: 'j2', status: 'failed' });

      const state = useRunStatusStore.getState();
      expect(state.processedCompletions.has('j1-completed')).toBe(true);
      expect(state.processedCompletions.has('j2-failed')).toBe(true);
    });

    it('should be cleared when clearRunHistory is called', () => {
      useRunStatusStore.getState().handleSSEUpdate({ job_id: 'j1', status: 'completed' });
      expect(useRunStatusStore.getState().processedCompletions.size).toBeGreaterThan(0);

      useRunStatusStore.getState().clearRunHistory();

      const state = useRunStatusStore.getState();
      expect(state.processedCompletions.size).toBe(0);
      expect(state.runHistory).toHaveLength(0);
      expect(state.hasRunningJobs).toBe(false);
    });

    it('should not dispatch events for stopped/cancelled if already processed', () => {
      const eventSpy = vi.fn();
      window.addEventListener('jobStopped', eventSpy);

      useRunStatusStore.getState().handleSSEUpdate({ job_id: 'j-stop', status: 'stopped' });
      useRunStatusStore.getState().handleSSEUpdate({ job_id: 'j-stop', status: 'stopped' });

      expect(eventSpy).toHaveBeenCalledTimes(1);

      window.removeEventListener('jobStopped', eventSpy);
    });
  });

  // -------------------------------------------------------
  // addRun
  // -------------------------------------------------------
  describe('addRun', () => {
    it('should add a run to the beginning of runHistory', () => {
      const existingRun: ExtendedRun = {
        id: 'old',
        job_id: 'old',
        status: 'completed',
        created_at: '2024-01-01T00:00:00Z',
        updated_at: '2024-01-01T00:00:00Z',
        run_name: 'Old Run',
        agents_yaml: '',
        tasks_yaml: '',
      };
      useRunStatusStore.setState({ runHistory: [existingRun] });

      const newRun: ExtendedRun = {
        id: 'new',
        job_id: 'new',
        status: 'running',
        created_at: '2024-06-01T00:00:00Z',
        updated_at: '2024-06-01T00:00:00Z',
        run_name: 'New Run',
        agents_yaml: '',
        tasks_yaml: '',
      };
      useRunStatusStore.getState().addRun(newRun);

      const state = useRunStatusStore.getState();
      expect(state.runHistory).toHaveLength(2);
      expect(state.runHistory[0].job_id).toBe('new');
    });

    it('should replace existing run with same job_id (avoid duplicates)', () => {
      const run: ExtendedRun = {
        id: 'dup',
        job_id: 'dup',
        status: 'running',
        created_at: '2024-01-01T00:00:00Z',
        updated_at: '2024-01-01T00:00:00Z',
        run_name: 'Dup Run',
        agents_yaml: '',
        tasks_yaml: '',
      };
      useRunStatusStore.setState({ runHistory: [run] });

      const updatedRun: ExtendedRun = {
        ...run,
        status: 'completed',
      };
      useRunStatusStore.getState().addRun(updatedRun);

      const state = useRunStatusStore.getState();
      expect(state.runHistory).toHaveLength(1);
      expect(state.runHistory[0].status).toBe('completed');
    });
  });

  // -------------------------------------------------------
  // updateRunStatus
  // -------------------------------------------------------
  describe('updateRunStatus', () => {
    it('should update the status of an active run', () => {
      const run: ExtendedRun = {
        id: 'upd',
        job_id: 'upd',
        status: 'running',
        created_at: '2024-06-01T10:00:00Z',
        updated_at: '2024-06-01T10:00:00Z',
        run_name: 'Update Test',
        agents_yaml: '',
        tasks_yaml: '',
      };
      useRunStatusStore.setState({
        activeRuns: { upd: run },
        runHistory: [run],
      });

      useRunStatusStore.getState().updateRunStatus('upd', 'completed');

      const state = useRunStatusStore.getState();
      expect(state.activeRuns['upd'].status).toBe('completed');
      expect(state.activeRuns['upd'].completed_at).toBeDefined();
      // Also check runHistory was updated
      expect(state.runHistory[0].status).toBe('completed');
    });

    it('should be a no-op if the jobId is not in activeRuns', () => {
      useRunStatusStore.getState().updateRunStatus('nonexistent', 'completed');
      // Should not throw or change state
      expect(useRunStatusStore.getState().runHistory).toHaveLength(0);
    });
  });
});

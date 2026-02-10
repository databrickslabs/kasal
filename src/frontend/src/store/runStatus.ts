import { create } from 'zustand';
import { ExtendedRun } from '../types/run';
import { runService } from '../api/ExecutionHistoryService';
import { Trace } from '../types/trace';

// Re-export Trace type to ensure consistency across the app
export type { Trace };

interface RunStatusState {
  currentRun: ExtendedRun | null;
  isTracking: boolean;
  error: string | null;
  isLoading: boolean;
  runHistory: ExtendedRun[];
  activeRuns: Record<string, ExtendedRun>;
  lastFetchTime: number;
  hasRunningJobs: boolean;
  processedCompletions: Set<string>; // Track which jobs we've already sent completion events for

  // SSE state (primary and only communication method)
  sseEnabled: boolean; // Whether SSE is available (always true unless manually disabled)
  sseConnected: boolean; // Whether SSE is currently connected
  sseError: string | null; // Track SSE connection errors

  // Traces state - accumulates traces from SSE
  traces: Map<string, Trace[]>; // jobId -> traces array

  // Actions
  addRun: (run: ExtendedRun) => void;
  setCurrentRun: (run: ExtendedRun | null) => void;
  setIsTracking: (isTracking: boolean) => void;
  setError: (error: string | null) => void;
  setIsLoading: (isLoading: boolean) => void;
  updateRunStatus: (jobId: string, status: string, error?: string) => void;
  addActiveRun: (run: ExtendedRun) => void;
  removeActiveRun: (jobId: string) => void;
  fetchInitialRunHistory: () => Promise<void>; // One-time fetch on app load
  clearRunHistory: () => void;
  handleSSEUpdate: (data: any) => void; // Handle SSE events
  setSSEConnected: (connected: boolean) => void; // Update SSE connection state
  setSSEError: (error: string | null) => void; // Update SSE error state
  addTrace: (jobId: string, trace: Trace) => void; // Add trace from SSE
  setTracesForJob: (jobId: string, traces: Trace[]) => void; // Set all traces for a job (initial load)
  getTracesForJob: (jobId: string) => Trace[]; // Get traces for a specific job
  clearTracesForJob: (jobId: string) => void; // Clear traces when job is removed
}

export const useRunStatusStore = create<RunStatusState>((set, get) => {
  // Set up event listeners for the store
  const setupEventListeners = () => {
    // Create a function that will be called when a new job is created
    const jobCreatedHandler = (event: CustomEvent) => {
      const { jobId, jobName, status, groupId } = event.detail || {};
      if (jobId) {
        // SECURITY FIX: Only add the run if it belongs to the current user's selected workspace
        const selectedGroupId = localStorage.getItem('selectedGroupId');

        // If the event includes a groupId, check if it matches the selected workspace
        if (groupId && groupId !== selectedGroupId) {
          console.log(`[RunStatusStore] Ignoring jobCreated for different group: ${groupId} (selected: ${selectedGroupId})`);
          return;
        }

        // If no groupId in event, skip for safety (can't verify ownership)
        if (!groupId) {
          console.log(`[RunStatusStore] Ignoring jobCreated without groupId for security`);
          return;
        }

        // Create a placeholder run for immediate display
        const newRun: ExtendedRun = {
          id: jobId,
          job_id: jobId,
          status: status || 'running', // Use the provided status or default to running
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
          run_name: jobName || `Run ${jobId}`,
          group_id: groupId,
          agents_yaml: '',
          tasks_yaml: ''
        };

        // Add the placeholder run and start tracking it
        const store = get();
        store.addRun(newRun);
        store.addActiveRun(newRun);

        // SSE will handle updates automatically - no polling needed
      }
    };

    // Listen for job created events
    window.addEventListener('jobCreated', jobCreatedHandler as EventListener);

    // Return cleanup function
    return () => {
      window.removeEventListener('jobCreated', jobCreatedHandler as EventListener);
    };
  };

  // Set up event listeners immediately
  // Note: Event listeners persist for the lifetime of the store (entire app lifecycle)
  setupEventListeners();

  // Return the actual store
  return {
    currentRun: null,
    isTracking: false,
    error: null,
    isLoading: false,
    runHistory: [],
    activeRuns: {},
    lastFetchTime: Date.now(),
    hasRunningJobs: false,
    processedCompletions: new Set<string>(),

    // SSE state (primary and only communication method)
    sseEnabled: true, // Always enabled - SSE is the only approach
    sseConnected: false,
    sseError: null,

    // Traces state
    traces: new Map<string, Trace[]>(),

    addRun: (run) => {
      set((state) => {
        // Filter out any existing run with the same job_id to avoid duplicates
        const filteredHistory = state.runHistory.filter(r => r.job_id !== run.job_id);

        return {
          runHistory: [run, ...filteredHistory], // Add new run at the beginning for visibility
          hasRunningJobs: state.hasRunningJobs || run.status.toLowerCase() === 'running' || run.status.toLowerCase() === 'queued'
        };
      });
    },

    setCurrentRun: (run) => {
      set({ currentRun: run });
    },

    setIsTracking: (isTracking) => {
      set({ isTracking });
    },

    setError: (error) => {
      set({ error });
    },

    setIsLoading: (isLoading) => {
      set({ isLoading });
    },

    updateRunStatus: (jobId, status, error) => {
      set((state) => {
        const currentRun = state.activeRuns[jobId];
        if (!currentRun) return state;

        const now = new Date().toISOString();

        // For completed or failed jobs, make sure we set the completed_at time
        let completedAt = currentRun.completed_at;
        if ((status.toLowerCase() === 'completed' || status.toLowerCase() === 'failed') && !completedAt) {
          completedAt = now;
        }

        const updatedRun: ExtendedRun = {
          ...currentRun,
          status,
          error,
          id: jobId,
          job_id: jobId,
          created_at: currentRun.created_at || now,
          run_name: currentRun.run_name || '',
          updated_at: now,
          completed_at: completedAt
        };

        // Also update the run in runHistory if it exists
        const updatedHistory = state.runHistory.map(run =>
          run.job_id === jobId ? {
            ...run,
            status,
            error,
            updated_at: now,
            completed_at: completedAt || run.completed_at
          } : run
        );

        return {
          activeRuns: {
            ...state.activeRuns,
            [jobId]: updatedRun
          },
          runHistory: updatedHistory,
          currentRun: state.currentRun?.job_id === jobId ? updatedRun : state.currentRun,
          hasRunningJobs: state.hasRunningJobs || status.toLowerCase() === 'running' || status.toLowerCase() === 'queued' || status.toLowerCase() === 'pending'
        };
      });
    },

    addActiveRun: (run) => {
      set((state) => ({
        activeRuns: { ...state.activeRuns, [run.job_id]: run }
      }));
    },

    removeActiveRun: (jobId) => {
      set((state) => {
        const { [jobId]: removed, ...remainingRuns } = state.activeRuns;
        return {
          activeRuns: remainingRuns,
          currentRun: state.currentRun?.job_id === jobId ? null : state.currentRun
        };
      });
    },

    fetchInitialRunHistory: async () => {
      // One-time fetch on app initialization
      // After this, all updates come via SSE only
      set({ isLoading: true, error: null });

      try {
        // Invalidate cache to ensure we get fresh data from the API.
        // This is critical when called after creating a new execution,
        // otherwise the 5-second cache returns stale data that overwrites
        // the jobCreated placeholder and the new execution disappears.
        runService.invalidateRunsCache();

        // Fetch recent runs from the backend
        const response = await runService.getRuns(50, 0);

        // SECURITY: Filter runs by group_id before processing
        const selectedGroupId = localStorage.getItem('selectedGroupId');
        const securityFilteredRuns = response.runs.filter(run => {
          // Only include runs that belong to the selected group
          if (!selectedGroupId || !run.group_id) {
            return false;
          }
          if (run.group_id !== selectedGroupId) {
            return false;
          }
          return true;
        });

        // Process the response data to ensure we have proper status information
        const currentProcessedCompletions = new Set(get().processedCompletions);

        const processedRuns = securityFilteredRuns.map(run => {
          // Check if this run's status has changed from running to completed/failed
          const currentState = get();
          const currentActiveRun = currentState.activeRuns[run.job_id];

          // Also check runHistory for the previous status
          const previousRun = currentState.runHistory.find((r: ExtendedRun) => r.job_id === run.job_id);
          const wasRunning = currentActiveRun?.status?.toLowerCase() === 'running' ||
                            currentActiveRun?.status?.toLowerCase() === 'queued' ||
                            previousRun?.status?.toLowerCase() === 'running' ||
                            previousRun?.status?.toLowerCase() === 'queued';

          if (wasRunning && (run.status.toLowerCase() === 'completed' || run.status.toLowerCase() === 'failed')) {
            // Check if we've already processed this completion to avoid duplicate events
            const completionKey = `${run.job_id}-${run.status.toLowerCase()}`;
            const alreadyProcessed = currentProcessedCompletions.has(completionKey);

            // Only dispatch events if we haven't already processed this completion
            if (!alreadyProcessed) {
              // Only dispatch ONE event based on status, ignoring error field if status is completed
              if (run.status.toLowerCase() === 'completed') {
                // Mark as processed before dispatching
                currentProcessedCompletions.add(completionKey);

                window.dispatchEvent(new CustomEvent('jobCompleted', {
                  detail: {
                    jobId: run.job_id,
                    result: run.result
                  }
                }));
              } else if (run.status.toLowerCase() === 'failed') {
                // Mark as processed before dispatching
                currentProcessedCompletions.add(completionKey);

                window.dispatchEvent(new CustomEvent('jobFailed', {
                  detail: {
                    jobId: run.job_id,
                    error: run.error || 'Job execution failed'
                  }
                }));
              }
            }
          }

          // Ensure status is properly set from the database
          // If it's completed or failed, make sure completed_at is set
          if ((run.status.toLowerCase() === 'completed' || run.status.toLowerCase() === 'failed')) {
            // Ensure the completed job has a completed_at timestamp
            if (!run.completed_at) {
              return {
                ...run,
                completed_at: run.updated_at || new Date().toISOString()
              };
            }
            // If completed_at is equal to created_at, adjust it to ensure positive duration
            const createdTime = new Date(run.created_at).getTime();
            const completedTime = new Date(run.completed_at).getTime();

            if (completedTime <= createdTime) {
              // Add at least 1 second for duration
              const adjustedCompletedTime = new Date(createdTime + 1000).toISOString();
              return {
                ...run,
                completed_at: adjustedCompletedTime
              };
            }
          }
          return run;
        });

        // Merge with existing store runs to preserve jobCreated placeholders
        // that may not be in the API response yet (timing edge case).
        const existingHistory = get().runHistory;
        const apiJobIds = new Set(processedRuns.map(r => r.job_id));
        const placeholderRuns = existingHistory.filter(existing => {
          // Keep existing placeholder runs that are still running/queued and
          // not yet present in the API response.
          if (apiJobIds.has(existing.job_id)) return false;
          const s = existing.status?.toLowerCase();
          return s === 'running' || s === 'queued' || s === 'pending';
        });
        const mergedRuns = [...placeholderRuns, ...processedRuns];

        // Update the running jobs flag
        const hasActiveJobs = mergedRuns.some(run =>
          run.status.toLowerCase() === 'running' || run.status.toLowerCase() === 'queued' || run.status.toLowerCase() === 'pending'
        );

        // Process runs into active runs
        const updatedActiveRuns: Record<string, ExtendedRun> = {};

        // Only keep truly active runs (running, queued, or pending)
        mergedRuns.forEach(run => {
          // Add to active runs if it's running, queued, or pending
          if (run.status.toLowerCase() === 'running' || run.status.toLowerCase() === 'queued' || run.status.toLowerCase() === 'pending') {
            updatedActiveRuns[run.job_id] = run;
          }
        });

        // Update store with all runs information
        set({
          runHistory: mergedRuns,
          activeRuns: updatedActiveRuns,
          isLoading: false,
          error: null,
          lastFetchTime: Date.now(),
          hasRunningJobs: hasActiveJobs,
          processedCompletions: currentProcessedCompletions
        });

      } catch (error) {
        // Capture and format the error message
        const errorMessage = error instanceof Error ? error.message : String(error);
        set({
          error: `Failed to fetch run history: ${errorMessage}`,
          isLoading: false
        });
      }
    },

    clearRunHistory: () => {
      set({
        runHistory: [],
        activeRuns: {},
        currentRun: null,
        processedCompletions: new Set(),
        error: null,
        isLoading: false,
        lastFetchTime: Date.now(),
        hasRunningJobs: false
      });
    },

    handleSSEUpdate: (data: any) => {
      const state = get();

      // Handle execution status update
      if (data.job_id && data.status) {
        const jobId = data.job_id;
        const status = data.status.toLowerCase();
        const message = data.message || data.error;
        const result = data.result;

        // Update the run in history, or add it if not present
        const existsInHistory = state.runHistory.some(run => run.job_id === jobId);
        let updatedHistory: ExtendedRun[];

        if (existsInHistory) {
          updatedHistory = state.runHistory.map(run => {
            if (run.job_id === jobId) {
              return {
                ...run,
                status,
                error: message,
                result: result || run.result,
                updated_at: data.updated_at || new Date().toISOString(),
                completed_at: data.completed_at || run.completed_at
              };
            }
            return run;
          });
        } else {
          // New job — add it to the beginning of history
          const newRun: ExtendedRun = {
            id: jobId,
            job_id: jobId,
            status,
            run_name: data.run_name || `Run ${jobId.substring(0, 8)}`,
            execution_type: data.execution_type || 'crew',
            created_at: data.created_at || new Date().toISOString(),
            updated_at: data.updated_at || new Date().toISOString(),
            completed_at: data.completed_at,
            group_id: data.group_id,
            error: message,
            result,
            agents_yaml: '',
            tasks_yaml: '',
          };
          updatedHistory = [newRun, ...state.runHistory];
        }

        // Update active runs
        const updatedActiveRuns = { ...state.activeRuns };
        if (status === 'running' || status === 'queued' || status === 'pending') {
          updatedActiveRuns[jobId] = {
            ...state.activeRuns[jobId],
            job_id: jobId,
            id: jobId,
            status,
            error: message,
            result,
            updated_at: data.updated_at || new Date().toISOString(),
            created_at: state.activeRuns[jobId]?.created_at || new Date().toISOString(),
            run_name: state.activeRuns[jobId]?.run_name || `Run ${jobId}`,
            agents_yaml: state.activeRuns[jobId]?.agents_yaml || '',
            tasks_yaml: state.activeRuns[jobId]?.tasks_yaml || ''
          };
        } else {
          // Remove from active runs if completed/failed
          delete updatedActiveRuns[jobId];
        }

        // Check if this is a completion event we haven't processed yet
        const completionKey = `${jobId}-${status}`;
        const alreadyProcessed = state.processedCompletions.has(completionKey);

        if (!alreadyProcessed && (status === 'completed' || status === 'failed' || status === 'stopped' || status === 'cancelled')) {
          // Mark as processed
          const newProcessedCompletions = new Set(state.processedCompletions);
          newProcessedCompletions.add(completionKey);

          // Dispatch appropriate event
          if (status === 'completed') {
            window.dispatchEvent(new CustomEvent('jobCompleted', {
              detail: { jobId, result }
            }));
          } else if (status === 'failed') {
            window.dispatchEvent(new CustomEvent('jobFailed', {
              detail: { jobId, error: message || 'Job execution failed' }
            }));
          } else if (status === 'stopped' || status === 'cancelled') {
            window.dispatchEvent(new CustomEvent('jobStopped', {
              detail: { jobId, status }
            }));
          }

          set({
            runHistory: updatedHistory,
            activeRuns: updatedActiveRuns,
            processedCompletions: newProcessedCompletions,
            hasRunningJobs: Object.keys(updatedActiveRuns).length > 0
          });
        } else {
          set({
            runHistory: updatedHistory,
            activeRuns: updatedActiveRuns,
            hasRunningJobs: Object.keys(updatedActiveRuns).length > 0
          });
        }
      }
    },

    setSSEConnected: (connected: boolean) => {
      set({
        sseConnected: connected,
        // Clear error on connection, keep existing error on disconnect (will be set by SSEConnectionManager)
        sseError: connected ? null : get().sseError
      });
    },

    setSSEError: (error: string | null) => {
      set({ sseError: error });
    },

    // Trace management methods
    addTrace: (jobId: string, trace: Trace) => {
      set((state) => {
        const newTraces = new Map(state.traces);
        const existingTraces = newTraces.get(jobId) || [];

        // Check if trace already exists (avoid duplicates)
        const exists = existingTraces.some(
          t => t.created_at === trace.created_at &&
               t.event_type === trace.event_type &&
               t.event_source === trace.event_source
        );

        if (!exists) {
          newTraces.set(jobId, [...existingTraces, trace]);
        }

        return { traces: newTraces };
      });
    },

    setTracesForJob: (jobId: string, traces: Trace[]) => {
      set((state) => {
        const newTraces = new Map(state.traces);
        newTraces.set(jobId, traces);
        return { traces: newTraces };
      });
    },

    getTracesForJob: (jobId: string) => {
      const state = get();
      return state.traces.get(jobId) || [];
    },

    clearTracesForJob: (jobId: string) => {
      set((state) => {
        const newTraces = new Map(state.traces);
        newTraces.delete(jobId);
        return { traces: newTraces };
      });
    }
  };
});

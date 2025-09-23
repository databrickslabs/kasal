import { create } from 'zustand';
import { ExtendedRun } from '../types/run';
import { runService } from '../api/ExecutionHistoryService';

// Constants for polling intervals
const INTERVALS = {
  CHECK_INTERVAL: 5000,        // Base check interval
  RUNNING_JOBS: 5000,          // Poll every 5 seconds for running jobs
  USER_INACTIVE: 10 * 60 * 1000, // Consider user inactive after 10 minutes
  NEW_JOB_POLLING: 3000,       // Poll every 3 seconds for the first minute after job creation
  DEBOUNCE_THRESHOLD: 1000     // Minimum time between API calls (1 second)
} as const;

// Exponential backoff steps used when no jobs are running
const BACKOFF_STEPS = [5000, 10000, 30000, 60000] as const;

interface RunStatusState {
  currentRun: ExtendedRun | null;
  isTracking: boolean;
  error: string | null;
  isLoading: boolean;
  runHistory: ExtendedRun[];
  activeRuns: Record<string, ExtendedRun>;
  lastFetchTime: number;
  lastFetchAttempt: number;
  lastNewJobTimestamp: number;
  hasRunningJobs: boolean;
  consecutiveEmptyFetches: number;
  backoffInterval: number;
  backoffStep: number;
  isUserActive: boolean;
  pollingInterval: NodeJS.Timeout | null;
  processedCompletions: Set<string>; // Track which jobs we've already sent completion events for

  // Actions
  addRun: (run: ExtendedRun) => void;
  setCurrentRun: (run: ExtendedRun | null) => void;
  setIsTracking: (isTracking: boolean) => void;
  setError: (error: string | null) => void;
  setIsLoading: (isLoading: boolean) => void;
  updateRunStatus: (jobId: string, status: string, error?: string) => void;
  addActiveRun: (run: ExtendedRun) => void;
  removeActiveRun: (jobId: string) => void;
  fetchRunHistory: () => Promise<void>;
  refreshRunHistory: () => Promise<void>;
  startPolling: () => void;
  stopPolling: () => void;
  setUserActive: (active: boolean) => void;
  cleanup: () => void;
  clearRunHistory: () => void;
}

export const useRunStatusStore = create<RunStatusState>((set, get) => {
  // Set up refreshRunHistory event listener for the store
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
          agents_yaml: '',
          tasks_yaml: ''
        };


        // Add the placeholder run and start tracking it
        const store = get();
        store.addRun(newRun);
        store.addActiveRun(newRun);
        
        // Create timestamp for this new job
        const newJobTimestamp = Date.now();
        set({ lastNewJobTimestamp: newJobTimestamp });
        
        // Force an immediate poll to get latest status
        store.fetchRunHistory();
        
        // Start more aggressive polling for new jobs
        store.startPolling();
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
  const cleanupListeners = setupEventListeners();

  // Return the actual store
  return {
    currentRun: null,
    isTracking: false,
    error: null,
    isLoading: false,
    runHistory: [],
    activeRuns: {},
    lastFetchTime: Date.now(),
    lastFetchAttempt: 0,
    lastNewJobTimestamp: 0,
    hasRunningJobs: false,
    consecutiveEmptyFetches: 0,
    backoffInterval: BACKOFF_STEPS[0],
    backoffStep: 0,
    isUserActive: true,
    pollingInterval: null,
    processedCompletions: new Set<string>(),

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

    fetchRunHistory: async () => {
      const state = get();
      const now = Date.now();
      
      // Add debounce to prevent too many API calls in quick succession
      if (now - state.lastFetchAttempt < INTERVALS.DEBOUNCE_THRESHOLD) {
        return;
      }
      
      // Mark attempt time immediately to prevent parallel calls
      set({ lastFetchAttempt: now, isLoading: true, error: null });
      
      try {
        // Simplified approach: always fetch all recent runs from scratch
        const response = await runService.getRuns(50, 0);
        
        // Get current processedCompletions set
        const currentProcessedCompletions = new Set(state.processedCompletions);

        // SECURITY: Filter runs by group_id before processing
        const selectedGroupId = localStorage.getItem('selectedGroupId');
        const securityFilteredRuns = response.runs.filter(run => {
          // Only include runs that belong to the selected group
          if (!selectedGroupId) {

            return false;
          }
          if (!run.group_id) {

            return false;
          }
          if (run.group_id !== selectedGroupId) {

            return false;
          }
          return true;
        });



        // Process the response data to ensure we have proper status information
        const processedRuns = securityFilteredRuns.map(run => {
          // Check if this run's status has changed from running to completed/failed
          const currentActiveRun = state.activeRuns[run.job_id];
          
          // Also check runHistory for the previous status
          const previousRun = state.runHistory.find(r => r.job_id === run.job_id);
          const wasRunning = currentActiveRun?.status?.toLowerCase() === 'running' || 
                            currentActiveRun?.status?.toLowerCase() === 'queued' ||
                            previousRun?.status?.toLowerCase() === 'running' ||
                            previousRun?.status?.toLowerCase() === 'queued';
          
          if (wasRunning && (run.status.toLowerCase() === 'completed' || run.status.toLowerCase() === 'failed')) {
            // Check if we've already processed this completion to avoid duplicate events
            const completionKey = `${run.job_id}-${run.status.toLowerCase()}`;
            const alreadyProcessed = currentProcessedCompletions.has(completionKey);
            
            if (alreadyProcessed) {

              // Don't return here! We still need to update the run data in the store
              // Just skip the event dispatch below
            }
            
            // Dispatch appropriate event for status change


            
            // Check if status is COMPLETED but there's an error field
            if (run.status.toLowerCase() === 'completed' && run.error) {

              // If status is COMPLETED, ignore the error field and treat as success
            }
            
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
          // If it's completed or failed, make sure updated_at is set
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

        // Update the running jobs flag and reset counters if we got data
        const hasActiveJobs = processedRuns.some(run =>
          run.status.toLowerCase() === 'running' || run.status.toLowerCase() === 'queued' || run.status.toLowerCase() === 'pending'
        );

        // Reset counters if we got data with active jobs
        if (hasActiveJobs) {
          set({
            consecutiveEmptyFetches: 0,
            backoffInterval: BACKOFF_STEPS[0],
            backoffStep: 0,
            hasRunningJobs: true
          });
        } else {
          set((state) => {
            const nextStep = Math.min(state.backoffStep + 1, BACKOFF_STEPS.length - 1);
            return {
              hasRunningJobs: false,
              consecutiveEmptyFetches: state.consecutiveEmptyFetches + 1,
              backoffStep: nextStep,
              backoffInterval: BACKOFF_STEPS[nextStep]
            };
          });
        }

        // Always update the fetch time and processed completions
        set({ 
          lastFetchTime: Date.now(),
          processedCompletions: currentProcessedCompletions,
          lastFetchAttempt: Date.now()
        });

        // Process runs into active runs
        const updatedActiveRuns: Record<string, ExtendedRun> = {};
        
        // Only keep truly active runs (running, queued, or pending)
        processedRuns.forEach(run => {
          // Add to active runs if it's running, queued, or pending
          if (run.status.toLowerCase() === 'running' || run.status.toLowerCase() === 'queued' || run.status.toLowerCase() === 'pending') {
            updatedActiveRuns[run.job_id] = run;
          }
        });

        // Update store with all runs information
        set({
          runHistory: processedRuns,
          activeRuns: updatedActiveRuns,
          isLoading: false,
          error: null
        });

      } catch (error) {

        // Capture and format the error message
        const errorMessage = error instanceof Error ? error.message : String(error);
        set({ 
          error: `Failed to fetch run history: ${errorMessage}`, 
          isLoading: false,
          lastFetchAttempt: Date.now()
        });
      }
    },

    refreshRunHistory: async () => {
      const { fetchRunHistory } = get();
      await fetchRunHistory();
    },

    startPolling: () => {
      const state = get();

      // Clear any existing timer first
      if (state.pollingInterval) {
        clearTimeout(state.pollingInterval as unknown as number);
      }

      const poll = async () => {
        const currentState = get();

        // Decide whether to fetch now
        const shouldFetch =
          currentState.hasRunningJobs ||
          (Date.now() - currentState.lastNewJobTimestamp < 60000) ||
          currentState.isUserActive;

        if (shouldFetch) {
          try {
            await currentState.fetchRunHistory();
          } catch (_err) {
            // fetchRunHistory handles error state
          }
        }

        // Compute next delay based on state AFTER fetch
        const afterState = get();
        let nextDelay: number;

        if (Date.now() - afterState.lastNewJobTimestamp < 60000) {
          nextDelay = Math.max(INTERVALS.NEW_JOB_POLLING, INTERVALS.DEBOUNCE_THRESHOLD);
        } else if (afterState.hasRunningJobs) {
          nextDelay = Math.max(INTERVALS.RUNNING_JOBS, INTERVALS.DEBOUNCE_THRESHOLD);
        } else {
          nextDelay = afterState.backoffInterval; // 5s -> 10s -> 30s -> 60s
        }

        const t = setTimeout(poll, nextDelay);
        set({ pollingInterval: t });
      };

      // Kick off the loop immediately
      const t = setTimeout(poll, 0);
      set({ pollingInterval: t });
    },

    stopPolling: () => {
      const { pollingInterval } = get();
      if (pollingInterval) {
        clearTimeout(pollingInterval as unknown as number);
        set({ pollingInterval: null });
      }
    },

    setUserActive: (active) => {
      set({ isUserActive: active });
      
      // If user became active, force a refresh
      if (active) {
        const { refreshRunHistory, startPolling } = get();
        void refreshRunHistory();

        // Reset backoff and restart polling
        set({ backoffInterval: BACKOFF_STEPS[0], backoffStep: 0 });
        startPolling();
      }
    },

    cleanup: () => {
      const { stopPolling } = get();
      stopPolling();
      cleanupListeners();
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
        lastFetchAttempt: Date.now(),
        consecutiveEmptyFetches: 0
      });
    }
  };
}); 
import { create } from 'zustand';
import { apiClient } from '../config/api/ApiConfig';

interface CrewNodeState {
  status: 'pending' | 'running' | 'completed' | 'failed';
  started_at?: string;
  completed_at?: string;
  failed_at?: string;
  task_count?: number;
  completed_count?: number;
}

interface TraceData {
  id: number;
  job_id: string;
  event_type: string;
  event_context?: string;
  trace_metadata?: {
    crew_name?: string;
    agent_role?: string;
    task_id?: string;
    [key: string]: unknown;
  };
  created_at?: string;
}

interface FlowExecutionState {
  // Current flow execution job ID
  currentJobId: string | null;
  // Map of crew name to their execution state
  crewNodeStates: Map<string, CrewNodeState>;
  // Is flow execution in progress
  isExecuting: boolean;
  // Track task counts per crew for completion detection
  crewTaskCounts: Map<string, number>;
  crewCompletedTasks: Map<string, number>;
  crewFailed: Set<string>;

  // Actions
  setCurrentJobId: (jobId: string | null) => void;
  setIsExecuting: (isExecuting: boolean) => void;
  handleTraceUpdate: (trace: TraceData) => void;
  startTracking: (jobId: string) => void;
  stopTracking: () => void;
  getCrewNodeStatus: (crewName: string) => CrewNodeState | undefined;
  clearStates: () => void;
  loadCrewStates: (jobId: string) => Promise<void>;
}

export const useFlowExecutionStore = create<FlowExecutionState>((set, get) => ({
  currentJobId: null,
  crewNodeStates: new Map(),
  isExecuting: false,
  crewTaskCounts: new Map(),
  crewCompletedTasks: new Map(),
  crewFailed: new Set(),

  setCurrentJobId: (jobId: string | null) => {
    set({ currentJobId: jobId });
  },

  setIsExecuting: (isExecuting: boolean) => {
    set({ isExecuting });
  },

  handleTraceUpdate: (trace: TraceData) => {
    const state = get();

    // Only process traces for the current job
    if (!state.currentJobId || trace.job_id !== state.currentJobId) {
      return;
    }

    const eventType = trace.event_type?.toUpperCase() || '';
    const eventContext = trace.event_context?.toLowerCase() || '';

    // Check if this is a completion event based on event_context (backend sends 'task_completion')
    const isCompletionFromContext = eventContext === 'task_completion' || eventContext === 'completing_task';
    const isStartFromContext = eventContext === 'starting_task';

    // Determine effective event type considering both event_type and event_context
    let effectiveEventType = eventType;
    if (isCompletionFromContext && eventType !== 'TASK_COMPLETED') {
      effectiveEventType = 'TASK_COMPLETED';
      console.log('[FlowExecutionStore] Detected completion from event_context:', eventContext);
    } else if (isStartFromContext && eventType !== 'TASK_STARTED') {
      effectiveEventType = 'TASK_STARTED';
    }

    // Only process task-related events
    if (!['TASK_STARTED', 'TASK_COMPLETED', 'TASK_FAILED'].includes(effectiveEventType)) {
      return;
    }

    // Extract crew name from metadata
    // Priority: crew_name > extra_data.crew_name > agent_role
    let crewName: string | null = null;
    if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
      crewName = trace.trace_metadata.crew_name || null;

      // Try extra_data.crew_name (legacy format)
      if (!crewName) {
        const extraData = trace.trace_metadata.extra_data as Record<string, unknown> | undefined;
        if (extraData && typeof extraData === 'object') {
          crewName = (extraData.crew_name as string) || null;
        }
      }

      // Fall back to agent role if crew name not available
      if (!crewName) {
        crewName = trace.trace_metadata.agent_role || null;
      }
    }

    if (!crewName) {
      console.log('[FlowExecutionStore] No crew_name or agent_role found in trace, skipping');
      return;
    }

    console.log('[FlowExecutionStore] Processing trace - crew:', crewName, 'event:', effectiveEventType);

    set((store) => {
      const newCrewNodeStates = new Map(store.crewNodeStates);
      const newCrewTaskCounts = new Map(store.crewTaskCounts);
      const newCrewCompletedTasks = new Map(store.crewCompletedTasks);
      const newCrewFailed = new Set(store.crewFailed);

      // Initialize crew state if not exists
      if (!newCrewNodeStates.has(crewName!)) {
        newCrewNodeStates.set(crewName!, {
          status: 'pending',
          started_at: undefined,
          completed_at: undefined,
          task_count: 0,
          completed_count: 0,
        });
        newCrewTaskCounts.set(crewName!, 0);
        newCrewCompletedTasks.set(crewName!, 0);
      }

      const currentState = newCrewNodeStates.get(crewName!)!;
      const timestamp = trace.created_at || new Date().toISOString();

      if (effectiveEventType === 'TASK_STARTED') {
        const taskCount = (newCrewTaskCounts.get(crewName!) || 0) + 1;
        newCrewTaskCounts.set(crewName!, taskCount);

        // Update to running if not already failed
        if (currentState.status === 'pending') {
          newCrewNodeStates.set(crewName!, {
            ...currentState,
            status: 'running',
            started_at: timestamp,
            task_count: taskCount,
          });
        } else {
          newCrewNodeStates.set(crewName!, {
            ...currentState,
            task_count: taskCount,
          });
        }
      } else if (effectiveEventType === 'TASK_COMPLETED') {
        const completedCount = (newCrewCompletedTasks.get(crewName!) || 0) + 1;
        newCrewCompletedTasks.set(crewName!, completedCount);
        const taskCount = newCrewTaskCounts.get(crewName!) || 0;

        // Check if all tasks are completed and crew hasn't failed
        if (completedCount >= taskCount && taskCount > 0 && !newCrewFailed.has(crewName!)) {
          newCrewNodeStates.set(crewName!, {
            ...currentState,
            status: 'completed',
            completed_at: timestamp,
            completed_count: completedCount,
          });
        } else {
          newCrewNodeStates.set(crewName!, {
            ...currentState,
            completed_count: completedCount,
          });
        }
      } else if (effectiveEventType === 'TASK_FAILED') {
        newCrewFailed.add(crewName!);
        newCrewNodeStates.set(crewName!, {
          ...currentState,
          status: 'failed',
          failed_at: timestamp,
        });
      }

      return {
        crewNodeStates: newCrewNodeStates,
        crewTaskCounts: newCrewTaskCounts,
        crewCompletedTasks: newCrewCompletedTasks,
        crewFailed: newCrewFailed,
      };
    });
  },

  startTracking: (jobId: string) => {
    console.log('[FlowExecutionStore] Starting SSE-based tracking for:', jobId);
    set({
      currentJobId: jobId,
      isExecuting: true,
      crewNodeStates: new Map(),
      crewTaskCounts: new Map(),
      crewCompletedTasks: new Map(),
      crewFailed: new Set(),
    });
  },

  stopTracking: () => {
    console.log('[FlowExecutionStore] Stopping tracking');
    set({ isExecuting: false });
  },

  getCrewNodeStatus: (crewName: string) => {
    return get().crewNodeStates.get(crewName);
  },

  clearStates: () => {
    set({
      currentJobId: null,
      crewNodeStates: new Map(),
      isExecuting: false,
      crewTaskCounts: new Map(),
      crewCompletedTasks: new Map(),
      crewFailed: new Set(),
    });
  },

  loadCrewStates: async (jobId: string) => {
    try {
      console.log('[FlowExecutionStore] Loading crew states for job:', jobId);

      // Fetch traces for the job
      const response = await apiClient.get(`/traces/job/${jobId}`, {
        params: { limit: 500, offset: 0 }
      });

      if (!response.data || !response.data.traces) {
        console.log('[FlowExecutionStore] No traces found for job:', jobId);
        return;
      }

      const traces = response.data.traces as TraceData[];

      // Process traces to compute crew states
      const crewNodeStates = new Map<string, CrewNodeState>();
      const crewTaskCounts = new Map<string, number>();
      const crewCompletedTasks = new Map<string, number>();
      const crewFailed = new Set<string>();

      for (const trace of traces) {
        const eventType = trace.event_type?.toUpperCase() || '';

        if (!['TASK_STARTED', 'TASK_COMPLETED', 'TASK_FAILED'].includes(eventType)) {
          continue;
        }

        // Extract crew name from metadata
        let crewName: string | null = null;
        if (trace.trace_metadata && typeof trace.trace_metadata === 'object') {
          crewName = trace.trace_metadata.crew_name || null;

          if (!crewName) {
            const extraData = trace.trace_metadata.extra_data as Record<string, unknown> | undefined;
            if (extraData && typeof extraData === 'object') {
              crewName = (extraData.crew_name as string) || null;
            }
          }

          if (!crewName) {
            crewName = trace.trace_metadata.agent_role || null;
          }
        }

        if (!crewName) continue;

        // Initialize crew state if not exists
        if (!crewNodeStates.has(crewName)) {
          crewNodeStates.set(crewName, {
            status: 'pending',
            task_count: 0,
            completed_count: 0,
          });
          crewTaskCounts.set(crewName, 0);
          crewCompletedTasks.set(crewName, 0);
        }

        const currentState = crewNodeStates.get(crewName)!;
        const timestamp = trace.created_at || new Date().toISOString();

        if (eventType === 'TASK_STARTED') {
          const taskCount = (crewTaskCounts.get(crewName) || 0) + 1;
          crewTaskCounts.set(crewName, taskCount);

          if (currentState.status === 'pending') {
            crewNodeStates.set(crewName, {
              ...currentState,
              status: 'running',
              started_at: timestamp,
              task_count: taskCount,
            });
          } else {
            crewNodeStates.set(crewName, {
              ...currentState,
              task_count: taskCount,
            });
          }
        } else if (eventType === 'TASK_COMPLETED') {
          const completedCount = (crewCompletedTasks.get(crewName) || 0) + 1;
          crewCompletedTasks.set(crewName, completedCount);
          const taskCount = crewTaskCounts.get(crewName) || 0;

          if (completedCount >= taskCount && taskCount > 0 && !crewFailed.has(crewName)) {
            crewNodeStates.set(crewName, {
              ...currentState,
              status: 'completed',
              completed_at: timestamp,
              completed_count: completedCount,
            });
          } else {
            crewNodeStates.set(crewName, {
              ...currentState,
              completed_count: completedCount,
            });
          }
        } else if (eventType === 'TASK_FAILED') {
          crewFailed.add(crewName);
          crewNodeStates.set(crewName, {
            ...currentState,
            status: 'failed',
            failed_at: timestamp,
          });
        }
      }

      console.log('[FlowExecutionStore] Loaded crew states:', Array.from(crewNodeStates.entries()));

      set({
        currentJobId: jobId,
        isExecuting: true,
        crewNodeStates,
        crewTaskCounts,
        crewCompletedTasks,
        crewFailed,
      });
    } catch (error) {
      console.error('[FlowExecutionStore] Failed to load crew states:', error);
    }
  },
}));

// Listen for flow execution events and trace updates
if (typeof window !== 'undefined') {
  // Handle trace updates from SSE (via useGlobalExecutionSSE)
  window.addEventListener('traceUpdate', ((event: CustomEvent) => {
    const detail = event.detail;
    if (detail && detail.trace) {
      useFlowExecutionStore.getState().handleTraceUpdate(detail.trace);
    }
  }) as EventListener);

  window.addEventListener('jobCreated', ((event: CustomEvent) => {
    const detail = event.detail;
    if (detail && detail.jobId) {
      // ALWAYS clear previous states when any job is created
      // This ensures crew nodes reset to default state before new execution starts
      console.log('[FlowExecutionStore] Job created, clearing all previous crew node states');
      useFlowExecutionStore.getState().clearStates();

      // Check if this is a flow execution - prefer explicit isFlow flag, fallback to job name check
      const isFlowExecution = detail.isFlow === true || detail.jobName?.toLowerCase().includes('flow');
      if (isFlowExecution) {
        console.log('[FlowExecutionStore] Flow execution started, beginning SSE tracking for:', detail.jobId);
        useFlowExecutionStore.getState().startTracking(detail.jobId);
      }
    }
  }) as EventListener);

  window.addEventListener('jobCompleted', ((event: CustomEvent) => {
    const detail = event.detail;
    const store = useFlowExecutionStore.getState();
    if (detail && detail.jobId === store.currentJobId) {
      console.log('[FlowExecutionStore] Flow execution completed:', detail.jobId);

      // Mark all still-running crew nodes as completed.
      // Due to a race condition, the per-job SSE connection may close before the
      // TraceBroadcastService polls the final task_completed traces from the DB.
      // Since the job completed successfully, all crews must have finished.
      const { crewNodeStates } = store;
      if (crewNodeStates.size > 0) {
        const updatedStates = new Map(crewNodeStates);
        let updated = false;
        for (const [key, crewState] of updatedStates.entries()) {
          if (crewState.status === 'running' || crewState.status === 'pending') {
            updatedStates.set(key, {
              ...crewState,
              status: 'completed',
              completed_at: new Date().toISOString(),
            });
            updated = true;
            console.log('[FlowExecutionStore] Marked crew as completed on job finish:', key);
          }
        }
        if (updated) {
          useFlowExecutionStore.setState({ crewNodeStates: updatedStates });
        }
      }

      // Give time for final state update before stopping tracking
      setTimeout(() => {
        store.stopTracking();
      }, 1000);

      // DON'T clear crew node states on completion - keep them visible until next run starts
      // Crew node states will be cleared when a new job is created (in jobCreated event handler)
    }
  }) as EventListener);

  window.addEventListener('jobFailed', ((event: CustomEvent) => {
    const detail = event.detail;
    const store = useFlowExecutionStore.getState();
    if (detail && detail.jobId === store.currentJobId) {
      console.log('[FlowExecutionStore] Flow execution failed:', detail.jobId);

      // Mark all still-running crew nodes as failed since the job failed
      const { crewNodeStates } = store;
      if (crewNodeStates.size > 0) {
        const updatedStates = new Map(crewNodeStates);
        let updated = false;
        for (const [key, crewState] of updatedStates.entries()) {
          if (crewState.status === 'running' || crewState.status === 'pending') {
            updatedStates.set(key, {
              ...crewState,
              status: 'failed',
              failed_at: new Date().toISOString(),
            });
            updated = true;
            console.log('[FlowExecutionStore] Marked crew as failed on job failure:', key);
          }
        }
        if (updated) {
          useFlowExecutionStore.setState({ crewNodeStates: updatedStates });
        }
      }

      store.stopTracking();

      // DON'T clear crew node states on failure - keep them visible until next run starts
      // Crew node states will be cleared when a new job is created (in jobCreated event handler)
    }
  }) as EventListener);

  // Expose store for debugging
  (window as unknown as { useFlowExecutionStore: typeof useFlowExecutionStore }).useFlowExecutionStore = useFlowExecutionStore;
}

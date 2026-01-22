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

interface FlowExecutionState {
  // Current flow execution job ID
  currentJobId: string | null;
  // Map of crew name to their execution state
  crewNodeStates: Map<string, CrewNodeState>;
  // Is flow execution in progress
  isExecuting: boolean;
  // Polling interval ID
  pollingInterval: NodeJS.Timeout | null;

  // Actions
  setCurrentJobId: (jobId: string | null) => void;
  setIsExecuting: (isExecuting: boolean) => void;
  loadCrewNodeStates: (jobId: string) => Promise<void>;
  startPolling: (jobId: string) => void;
  stopPolling: () => void;
  getCrewNodeStatus: (crewName: string) => CrewNodeState | undefined;
  clearStates: () => void;
}

export const useFlowExecutionStore = create<FlowExecutionState>((set, get) => ({
  currentJobId: null,
  crewNodeStates: new Map(),
  isExecuting: false,
  pollingInterval: null,

  setCurrentJobId: (jobId: string | null) => {
    set({ currentJobId: jobId });
  },

  setIsExecuting: (isExecuting: boolean) => {
    set({ isExecuting });
  },

  loadCrewNodeStates: async (jobId: string) => {
    try {
      const response = await apiClient.get(`/traces/job/${jobId}/crew-node-states`);

      set((store) => {
        const newStates = new Map(store.crewNodeStates);

        Object.entries(response.data).forEach(([crewName, state]) => {
          newStates.set(crewName, state as CrewNodeState);
        });

        return { crewNodeStates: newStates };
      });
    } catch (error) {
      console.error('[FlowExecutionStore] Failed to load crew node states:', error);
    }
  },

  startPolling: (jobId: string) => {
    const state = get();

    // Stop any existing polling
    if (state.pollingInterval) {
      clearInterval(state.pollingInterval);
    }

    set({ currentJobId: jobId, isExecuting: true });

    // Initial fetch
    get().loadCrewNodeStates(jobId);

    // Poll every 2 seconds for real-time updates
    const interval = setInterval(async () => {
      const currentState = get();

      // Stop polling if execution is done or job changed
      if (!currentState.isExecuting || currentState.currentJobId !== jobId) {
        clearInterval(interval);
        set({ pollingInterval: null });
        return;
      }

      await currentState.loadCrewNodeStates(jobId);

      // Check if all crews are completed or failed to stop polling
      const allDone = Array.from(currentState.crewNodeStates.values()).every(
        (state) => state.status === 'completed' || state.status === 'failed'
      );

      if (allDone && currentState.crewNodeStates.size > 0) {
        // Give a small delay to ensure final state is captured
        setTimeout(() => {
          set({ isExecuting: false });
          clearInterval(interval);
          set({ pollingInterval: null });
        }, 1000);
      }
    }, 2000);

    set({ pollingInterval: interval });
  },

  stopPolling: () => {
    const state = get();
    if (state.pollingInterval) {
      clearInterval(state.pollingInterval);
    }
    set({ pollingInterval: null, isExecuting: false });
  },

  getCrewNodeStatus: (crewName: string) => {
    return get().crewNodeStates.get(crewName);
  },

  clearStates: () => {
    const state = get();
    if (state.pollingInterval) {
      clearInterval(state.pollingInterval);
    }
    set({
      currentJobId: null,
      crewNodeStates: new Map(),
      isExecuting: false,
      pollingInterval: null,
    });
  },
}));

// Listen for flow execution events
if (typeof window !== 'undefined') {
  window.addEventListener('jobCreated', ((event: CustomEvent) => {
    const detail = event.detail;
    if (detail && detail.jobId) {
      // ALWAYS clear previous states when any job is created
      // This ensures crew nodes reset to default state before new execution starts
      console.log('[FlowExecutionStore] Job created, clearing all previous crew node states');
      useFlowExecutionStore.getState().clearStates();

      // Check if this is a flow execution by checking the job name
      const isFlowExecution = detail.jobName?.toLowerCase().includes('flow');
      if (isFlowExecution) {
        console.log('[FlowExecutionStore] Flow execution started, beginning polling for:', detail.jobId);
        useFlowExecutionStore.getState().startPolling(detail.jobId);
      }
    }
  }) as EventListener);

  window.addEventListener('jobCompleted', ((event: CustomEvent) => {
    const detail = event.detail;
    const state = useFlowExecutionStore.getState();
    if (detail && detail.jobId === state.currentJobId) {
      console.log('[FlowExecutionStore] Flow execution completed:', detail.jobId);
      // Give time for final state update before stopping polling
      setTimeout(() => {
        state.stopPolling();
      }, 1000);

      // Clear crew node states after 10 seconds to give users time to see final status
      // This matches the behavior of clearTaskStates in WorkflowDesigner
      setTimeout(() => {
        console.log('[FlowExecutionStore] Clearing crew node states after completion delay');
        useFlowExecutionStore.getState().clearStates();
      }, 10000);
    }
  }) as EventListener);

  window.addEventListener('jobFailed', ((event: CustomEvent) => {
    const detail = event.detail;
    const state = useFlowExecutionStore.getState();
    if (detail && detail.jobId === state.currentJobId) {
      console.log('[FlowExecutionStore] Flow execution failed:', detail.jobId);
      state.stopPolling();

      // Clear crew node states after 10 seconds for failed jobs too
      setTimeout(() => {
        console.log('[FlowExecutionStore] Clearing crew node states after failure delay');
        useFlowExecutionStore.getState().clearStates();
      }, 10000);
    }
  }) as EventListener);

  // Expose store for debugging
  (window as unknown as { useFlowExecutionStore: typeof useFlowExecutionStore }).useFlowExecutionStore = useFlowExecutionStore;
}

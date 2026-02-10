import { create } from 'zustand';
import { apiClient } from '../config/api/ApiConfig';
import { extractTaskId, extractTaskName, mapEventToStatus } from '../utils/taskIdUtils';

type TaskStatus = 'planning' | 'running' | 'completed' | 'failed';

interface TaskState {
  status: TaskStatus;
  task_name: string;
  started_at?: string;
  completed_at?: string;
  failed_at?: string;
}

/**
 * Valid state transitions for task lifecycle.
 * Prevents impossible states like going from 'completed' back to 'running'.
 */
const VALID_TRANSITIONS: Record<TaskStatus, TaskStatus[]> = {
  planning: ['running', 'failed'],
  running: ['completed', 'failed'],
  completed: [],          // terminal state
  failed: ['running'],    // allow retry
};

/**
 * Status precedence for out-of-order event handling.
 * Higher number = later in lifecycle. Never go backwards.
 * Used ONLY by transitionAll() for batch operations (e.g. handleJobCompleted).
 */
const STATUS_PRECEDENCE: Record<TaskStatus, number> = {
  planning: 0,
  running: 1,
  completed: 2,
  failed: 2,  // same precedence as completed (both terminal)
};

interface TaskExecutionState {
  taskStates: Map<string, TaskState>;
  isPlanningPhase: boolean;

  // Guarded transition - returns false if transition is invalid
  transition: (taskId: string, newStatus: TaskStatus, metadata?: Partial<TaskState>) => boolean;
  // Batch transition - transitions all tasks matching a status to a new status
  transitionAll: (fromStatuses: TaskStatus[], toStatus: TaskStatus, metadata?: Partial<TaskState>) => void;
  loadTaskStates: (jobId: string) => Promise<void>;
  clearTaskStates: () => void;
  isTaskRunning: (taskId: string) => boolean;
  getTaskStatus: (taskId: string) => TaskState | undefined;
  setIsPlanningPhase: (value: boolean) => void;
}

export const useTaskExecutionStore = create<TaskExecutionState>((set, get) => ({
  taskStates: new Map(),
  isPlanningPhase: false,

  transition: (taskId: string, newStatus: TaskStatus, metadata?: Partial<TaskState>) => {
    const current = get().taskStates.get(taskId);
    const currentStatus = current?.status;

    // New task (no existing state) - allow any initial status
    if (!currentStatus) {
      set((store) => {
        const newStates = new Map(store.taskStates);
        newStates.set(taskId, {
          status: newStatus,
          task_name: metadata?.task_name ?? '',
          ...(newStatus === 'running' && { started_at: metadata?.started_at ?? new Date().toISOString() }),
          ...(newStatus === 'completed' && { completed_at: metadata?.completed_at ?? new Date().toISOString() }),
          ...(newStatus === 'failed' && { failed_at: metadata?.failed_at ?? new Date().toISOString() }),
        });
        return { taskStates: newStates };
      });
      return true;
    }

    // Same status - no-op
    if (currentStatus === newStatus) {
      return true;
    }

    // Guard: only allow valid transitions (VALID_TRANSITIONS is the single source of truth)
    if (!VALID_TRANSITIONS[currentStatus].includes(newStatus)) {
      console.warn(
        `[TaskFSM] Invalid transition: ${currentStatus} -> ${newStatus} for task ${taskId}`
      );
      return false;
    }

    set((store) => {
      const newStates = new Map(store.taskStates);
      newStates.set(taskId, {
        ...current,
        status: newStatus,
        task_name: metadata?.task_name ?? current.task_name,
        // Preserve existing timestamps, add new ones for the target status
        ...(current.started_at && { started_at: current.started_at }),
        ...(newStatus === 'running' && !current.started_at && { started_at: metadata?.started_at ?? new Date().toISOString() }),
        ...(newStatus === 'completed' && { completed_at: metadata?.completed_at ?? new Date().toISOString() }),
        ...(newStatus === 'failed' && { failed_at: metadata?.failed_at ?? new Date().toISOString() }),
      });
      return { taskStates: newStates };
    });
    return true;
  },

  transitionAll: (fromStatuses: TaskStatus[], toStatus: TaskStatus, metadata?: Partial<TaskState>) => {
    set((store) => {
      const newStates = new Map(store.taskStates);
      let changed = false;

      newStates.forEach((state, taskId) => {
        if (fromStatuses.includes(state.status)) {
          // Apply precedence check - don't go backwards (needed for batch ops like handleJobCompleted)
          if (STATUS_PRECEDENCE[toStatus] >= STATUS_PRECEDENCE[state.status]) {
            newStates.set(taskId, {
              ...state,
              status: toStatus,
              ...(toStatus === 'completed' && { completed_at: metadata?.completed_at ?? new Date().toISOString() }),
              ...(toStatus === 'failed' && { failed_at: metadata?.failed_at ?? new Date().toISOString() }),
            });
            changed = true;
          }
        }
      });

      return changed ? { taskStates: newStates } : {};
    });
  },

  loadTaskStates: async (jobId: string) => {
    try {
      const response = await apiClient.get(`/traces/job/${jobId}/task-states`);

      set((store) => {
        const newStates = new Map(store.taskStates);

        Object.entries(response.data).forEach(([taskId, state]) => {
          const typedState = state as TaskState;

          // Normalize task ID: strip common prefixes
          let normalizedTaskId = taskId;
          if (normalizedTaskId.startsWith('task_')) {
            normalizedTaskId = normalizedTaskId.substring(5);
          }
          if (normalizedTaskId.startsWith('task-')) {
            normalizedTaskId = normalizedTaskId.substring(5);
          }

          // Store by single canonical key only
          newStates.set(normalizedTaskId, typedState);
        });

        return { taskStates: newStates };
      });
    } catch (error) {
      console.error('[TaskExecutionStore] Failed to load task states:', error);
    }
  },

  clearTaskStates: () => {
    // Only clear task states - preserve isPlanningPhase (managed by setIsPlanningPhase)
    set({ taskStates: new Map() });
  },

  isTaskRunning: (taskId: string) => {
    const state = get().taskStates.get(taskId);
    return state?.status === 'running';
  },

  getTaskStatus: (taskId: string) => {
    return get().taskStates.get(taskId);
  },

  setIsPlanningPhase: (value: boolean) => {
    set({ isPlanningPhase: value });
  }
}));

export type { TaskState, TaskStatus };

// ---------------------------------------------------------------------------
// Module-level window event listeners
//
// These ensure task states are updated regardless of whether the Chat panel
// (and useExecutionMonitoring) is mounted. The FSM guards in transition()
// make duplicate calls from useExecutionMonitoring safe (no-ops).
// ---------------------------------------------------------------------------
if (typeof window !== 'undefined') {
  window.addEventListener('traceUpdate', ((event: CustomEvent) => {
    const { trace } = event.detail ?? {};
    if (!trace) return;

    const isTaskEvent =
      trace.event_type === 'task_started' ||
      trace.event_type === 'task_completed' ||
      trace.event_type === 'task_failed';

    if (!isTaskEvent) return;

    // Clear planning phase on first task event
    if (useTaskExecutionStore.getState().isPlanningPhase) {
      useTaskExecutionStore.getState().setIsPlanningPhase(false);
    }

    const taskId = extractTaskId(trace);
    const taskName = extractTaskName(trace);
    const status = mapEventToStatus(trace.event_type);

    if (taskId) {
      useTaskExecutionStore.getState().transition(taskId, status, {
        task_name: taskName ?? '',
        ...(status === 'running' && { started_at: trace.created_at }),
        ...(status === 'completed' && { completed_at: trace.created_at }),
        ...(status === 'failed' && { failed_at: trace.created_at }),
      });
    }
  }) as EventListener);

  window.addEventListener('jobCreated', (() => {
    useTaskExecutionStore.getState().clearTaskStates();
  }) as EventListener);

  window.addEventListener('jobCompleted', (() => {
    useTaskExecutionStore.getState().setIsPlanningPhase(false);
    useTaskExecutionStore.getState().transitionAll(
      ['running', 'planning'],
      'completed',
      { completed_at: new Date().toISOString() }
    );
  }) as EventListener);

  window.addEventListener('jobFailed', (() => {
    useTaskExecutionStore.getState().setIsPlanningPhase(false);
    useTaskExecutionStore.getState().transitionAll(
      ['running', 'planning'],
      'failed',
      { failed_at: new Date().toISOString() }
    );
  }) as EventListener);

  window.addEventListener('jobStopped', (() => {
    useTaskExecutionStore.getState().setIsPlanningPhase(false);
    useTaskExecutionStore.getState().clearTaskStates();
  }) as EventListener);
}

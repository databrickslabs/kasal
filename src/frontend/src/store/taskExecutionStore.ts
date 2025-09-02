import { create } from 'zustand';
import { apiClient } from '../config/api/ApiConfig';

interface TaskState {
  status: 'running' | 'completed' | 'failed';
  task_name: string;
  started_at?: string;
  completed_at?: string;
  failed_at?: string;
}

interface TaskExecutionState {
  taskStates: Map<string, TaskState>;
  
  setTaskState: (taskId: string, state: TaskState) => void;
  loadTaskStates: (jobId: string) => Promise<void>;
  clearTaskStates: () => void;
  isTaskRunning: (taskId: string) => boolean;
  getTaskStatus: (taskId: string) => TaskState | undefined;
}

export const useTaskExecutionStore = create<TaskExecutionState>((set, get) => ({
  taskStates: new Map(),
  
  setTaskState: (taskId: string, state: TaskState) => {
    set((store) => {
      const newStates = new Map(store.taskStates);
      newStates.set(taskId, state);
      return { taskStates: newStates };
    });
  },
  
  loadTaskStates: async (jobId: string) => {
    try {
      const response = await apiClient.get(`/traces/job/${jobId}/task-states`);
      const states = new Map<string, TaskState>();
      
      Object.entries(response.data).forEach(([taskId, state]) => {
        states.set(taskId, state as TaskState);
      });
      
      set({ taskStates: states });
    } catch (error) {
      console.error('Failed to load task states:', error);
    }
  },
  
  clearTaskStates: () => {
    set({ taskStates: new Map() });
  },
  
  isTaskRunning: (taskId: string) => {
    const state = get().taskStates.get(taskId);
    return state?.status === 'running';
  },
  
  getTaskStatus: (taskId: string) => {
    return get().taskStates.get(taskId);
  }
}));
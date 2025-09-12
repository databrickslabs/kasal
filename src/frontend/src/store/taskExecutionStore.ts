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
  seenTasks: Set<string>; // Track which tasks we've seen to detect first occurrence
  
  setTaskState: (taskId: string, state: TaskState) => void;
  loadTaskStates: (jobId: string) => Promise<void>;
  clearTaskStates: () => void;
  isTaskRunning: (taskId: string) => boolean;
  getTaskStatus: (taskId: string) => TaskState | undefined;
  hasSeenTask: (taskId: string) => boolean;
  markTaskSeen: (taskId: string) => void;
}

export const useTaskExecutionStore = create<TaskExecutionState>((set, get) => ({
  taskStates: new Map(),
  seenTasks: new Set(),
  
  setTaskState: (taskId: string, state: TaskState) => {
    console.log(`[TaskExecutionStore] setTaskState called - taskId: "${taskId}", state:`, state);
    set((store) => {
      const newStates = new Map(store.taskStates);
      newStates.set(taskId, state);
      console.log(`[TaskExecutionStore] After update - total states: ${newStates.size}, keys:`, Array.from(newStates.keys()).slice(0, 5));
      return { taskStates: newStates };
    });
  },
  
  hasSeenTask: (taskId: string) => {
    return get().seenTasks.has(taskId);
  },
  
  markTaskSeen: (taskId: string) => {
    set((store) => {
      const newSeen = new Set(store.seenTasks);
      newSeen.add(taskId);
      return { seenTasks: newSeen };
    });
  },
  
  loadTaskStates: async (jobId: string) => {
    try {
      const response = await apiClient.get(`/traces/job/${jobId}/task-states`);
      
      // Merge with existing states instead of replacing them
      set((store) => {
        const newStates = new Map(store.taskStates);
        
        Object.entries(response.data).forEach(([taskId, state]) => {
          // Handle different task ID formats
          // Backend might return: task_task-UUID, task_UUID, or just UUID
          let normalizedTaskId = taskId;
          
          // Remove "task_" prefix if present
          if (normalizedTaskId.startsWith('task_')) {
            normalizedTaskId = normalizedTaskId.substring(5);
          }
          
          // Remove "task-" prefix if present
          if (normalizedTaskId.startsWith('task-')) {
            normalizedTaskId = normalizedTaskId.substring(5);
          }
          
          newStates.set(normalizedTaskId, state as TaskState);
        });
        
        return { taskStates: newStates };
      });
    } catch (error) {
      console.error('Failed to load task states:', error);
    }
  },
  
  clearTaskStates: () => {
    set({ taskStates: new Map(), seenTasks: new Set() });
  },
  
  isTaskRunning: (taskId: string) => {
    const state = get().taskStates.get(taskId);
    return state?.status === 'running';
  },
  
  getTaskStatus: (taskId: string) => {
    return get().taskStates.get(taskId);
  }
}));
export type TaskStatus = 'planning' | 'running' | 'completed' | 'failed';

export interface TaskState {
  status: TaskStatus;
  task_name: string;
  started_at?: string;
  completed_at?: string;
  failed_at?: string;
}

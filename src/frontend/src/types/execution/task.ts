// 'warning' is a terminal state for a run that TIMED OUT while finalizing — the
// outputs were produced but the run was not cleanly closed. It is shown like a
// softer 'completed' (orange, not red). See src/utils/lateTimeout.ts.
export type TaskStatus = 'planning' | 'running' | 'completed' | 'failed' | 'warning';

export interface TaskState {
  status: TaskStatus;
  task_name: string;
  started_at?: string;
  completed_at?: string;
  failed_at?: string;
}

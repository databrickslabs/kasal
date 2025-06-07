// Define possible value types for RunResult
type RunResultValue = string | number | boolean | null | Record<string, unknown>;

export interface RunResult {
  output?: string;
  error?: string;
  metadata?: Record<string, unknown>;
  [key: string]: RunResultValue | undefined;
}

export interface Run {
  id: string;
  job_id: string;
  status: string;
  created_at: string;
  updated_at: string;
  completed_at?: string;
  run_name: string;
  agents_yaml: string;
  tasks_yaml: string;
  group_email?: string;
  inputs?: {
    agents_yaml: Record<string, any>;
    tasks_yaml: Record<string, any>;
    inputs?: Record<string, any>;
    planning?: boolean;
    model?: string;
    execution_type?: string;
    schema_detection_enabled?: boolean;
    [key: string]: any;
  };
  result?: RunResult;
  error?: string;
}

export interface ExtendedRun extends Run {
  currentTaskId?: string | null;
  completedTaskIds?: string[];
}

export interface RunsResponse {
  runs: Run[];
  total: number;
  limit: number;
  offset: number;
}

export interface JobStatus {
  status: string;
  error?: string;
} 
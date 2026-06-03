export type ExecutionStatus =
  | 'queued'
  | 'running'
  | 'completed'
  | 'failed'
  | 'stopped';

export interface ExecutionConfig {
  agents_yaml: Record<string, Record<string, unknown>>;
  tasks_yaml: Record<string, Record<string, unknown>>;
  inputs?: Record<string, unknown>;
  planning?: boolean;
  reasoning?: boolean;
  model?: string;
  execution_type?: string;
  schema_detection_enabled?: boolean;
  // Flow-specific fields
  nodes?: unknown[];
  edges?: unknown[];
  flow_id?: string;
  flow_config?: Record<string, unknown>;
}

export interface Execution {
  id: string;
  job_id: string;
  execution_id?: string;
  status: ExecutionStatus;
  result?: string;
  error?: string;
  run_name?: string;
  created_at: string;
  updated_at: string;
}

export interface ExecutionTrace {
  timestamp: string;
  message: string;
  level?: string;
  agent?: string;
  task?: string;
}

export interface SSEEvent {
  event: string;
  data: Record<string, unknown>;
}

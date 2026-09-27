export interface ExecutionContext {
  crewName: string;
  agents: { name: string; role?: string }[];
  tasks: { name: string }[];
}

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
  reasoning?: boolean;
  model?: string;
  execution_type?: string;
  schema_detection_enabled?: boolean;
  // Memory scoping (chat). crew_id is generated backend-side for tracing.
  /** Chat session id — scopes session-only memory recall. */
  session_id?: string;
  /** Memory read scope: true = workspace-wide (default), false = this chat session only. */
  memory_workspace_scope?: boolean;
  // Flow-specific fields
  nodes?: unknown[];
  edges?: unknown[];
  flow_id?: string;
  flow_config?: Record<string, unknown>;
}

/** What Auto resolved to, on a response to a request that asked for `auto`. */
export interface ModelSelection {
  requested: 'auto';
  model: string | null;
  /** `selected` by the decision model, or the workspace default on `fallback`. */
  status: 'selected' | 'fallback';
  /** Why it fell back, e.g. `timeout` (see `chat.autoModel.reasons`). */
  reason?: string | null;
}

export interface Execution {
  id: string;
  job_id: string;
  execution_id?: string;
  model_selection?: ModelSelection | null;
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

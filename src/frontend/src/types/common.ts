import { Agent } from './agent';
import { LogEntry } from '../api/ExecutionLogs';
import { ResultValue } from './result';

export type JobResult = {
  data?: unknown;
  error?: string;
  status?: string;
  timestamp?: string;
  [key: string]: unknown;
};

export interface JobStatus {
  status: 'starting' | 'pending' | 'running' | 'completed' | 'failed';
  error?: string;
  result?: unknown;
  currentTaskId?: string;
  completedTaskIds?: string[];
  taskStatus?: TaskStatus;
}

export interface TaskStatus {
  taskId: string;
  status: 'running' | 'completed' | 'failed';
  timestamp?: string;
}

export interface LLMLog {
  id: string;
  created_at: string;
  endpoint: string;
  model: string;
  tokens_used?: number;
  duration_ms: number;
  status: 'success' | 'error';
  prompt: string;
  response: string;
  extra_data?: Record<string, unknown>;
  error_message?: string;
}

export interface LogRowProps {
  log: LLMLog;
}

export interface Task {
  id?: string;
  name: string;
  description: string;
  expected_output: string;
  agent?: string;
  context: string;
  tools?: string[];
}

export interface Recipe {
  id: string;
  name: string;
  description: string;
  config: {
    agents: Agent[];
    tasks: Task[];
  };
}

export interface Log {
  id: string;
  timestamp: string;
  level: string;
  message: string;
  metadata?: Record<string, unknown>;
}

export interface Job {
  id: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  result?: unknown;
  error?: string;
  created_at: string;
  updated_at: string;
}

export interface APIKey {
  id: string;
  name: string;
  key: string;
  service: string;
  created_at: string;
}

export interface NotificationState {
  open: boolean;
  message: string;
  severity: 'success' | 'error' | 'info' | 'warning';
}

export interface ValidationState {
  open: boolean;
  message: string;
  severity: 'success' | 'error' | 'info' | 'warning';
}

export interface JobStatusIndicatorProps {
  open: boolean;
  jobId: string | null;
  onClose: () => void;
}

export interface FlowNode {
  id: string;
  type: string;
  position: {
    x: number;
    y: number;
  };
  data: {
    label: string;
    [key: string]: unknown;
  };
}

export interface FlowEdge {
  id: string;
  source: string;
  target: string;
  type?: string;
  animated?: boolean;
  label?: string;
  style?: {
    stroke?: string;
    strokeWidth?: number;
  };
  markerEnd?: {
    type: string;
    color: string;
  };
  stateType?: 'structured' | 'unstructured';
  stateDefinition?: string;
  stateData?: Record<string, unknown>;
}

export interface RecipeFormData {
  id: string;
  title: string;
  description: string;
  iconName: string;
  color: string;
  difficulty: 'Beginner' | 'Intermediate' | 'Advanced';
  agentsYaml: string;
  tasksYaml: string;
}

export interface RecipeSubmitData {
  id: string;
  title: string;
  description: string;
  iconName: string;
  color: string;
  agents: string[];
  difficulty: 'Beginner' | 'Intermediate' | 'Advanced';
  agents_yaml: string;
  tasks_yaml: string;
}

export interface RunResult {
  output?: string | Record<string, unknown>;
  error?: string;
  metadata?: Record<string, unknown>;
  [key: string]: string | Record<string, unknown> | undefined;
}

export interface ShowLogsProps {
  open: boolean;
  onClose: () => void;
  logs: LogEntry[];
  jobId: string;
  isConnecting: boolean;
  connectionError: string | null;
}

export interface ShowResultProps {
  open: boolean;
  onClose: () => void;
  result: Record<string, ResultValue>;
}

export interface NavigationProps {
  open: boolean;
  onDrawerToggle: () => void;
}

export interface MenuItem {
  text: string;
  path: string;
  icon: React.ReactNode;
} 
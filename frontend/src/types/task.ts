export interface Task {
  id: string;
  name: string;
  description: string;
  expected_output: string;
  tools: string[];
  agent_id: string | null;
  async_execution: boolean;
  markdown: boolean;
  context: string[];
  config: {
    cache_response: boolean;
    cache_ttl: number;
    retry_on_fail: boolean;
    max_retries: number;
    timeout: number | null;
    priority: number;
    error_handling: 'default' | 'retry' | 'ignore' | 'fail';
    output_file: string | null;
    output_json: string | null;
    output_pydantic: string | null;
    callback: string | null;
    human_input: boolean;
    markdown: boolean;
    condition?: string;
    guardrail?: string | null;
  };
  created_at?: string;
  updated_at?: string;
  output?: string;
  callback?: string;
  converter_cls?: string;
}

export interface TaskFormData extends Omit<Task, 'config' | 'context'> {
  async_execution: boolean;
  markdown: boolean;
  output_json?: string;
  output_pydantic?: string;
  output_file?: string;
  human_input?: boolean;
  retry_on_fail?: boolean;
  max_retries?: number;
  timeout?: number | null;
  priority?: number;
  error_handling?: 'default' | 'ignore' | 'retry' | 'fail';
  cache_response?: boolean;
  cache_ttl?: number;
  tools: string[];
  context: string[];
  agent_id: string | null;
}

export interface Tool {
  id: string;
  name: string;
  description: string;
}

export interface TaskDependency {
  task_name: string;
  required_before: string[];
}

export interface ConnectionAssignment {
  agent_name: string;
  tasks: Array<{ task_name: string }>;
}

export interface ConnectionResponse {
  assignments: ConnectionAssignment[];
  dependencies: TaskDependency[];
}

export interface SavedTasksProps {
  refreshTrigger?: number;
}

export interface AdvancedConfig {
  async_execution: boolean;
  cache_response: boolean;
  cache_ttl: number;
  callback: string | null;
  context: string[];
  dependencies: string[];
  error_handling: string;
  human_input: boolean;
  markdown: boolean;
  max_retries: number;
  output_file: string | null;
  output_json: string | null;
  output_parser: string | null;
  output_pydantic: string | null;
  priority: number;
  retry_on_fail: boolean;
  timeout: number | null;
  condition?: string;
  guardrail?: string | null;
}

export interface TaskAdvancedConfigProps {
  advancedConfig: AdvancedConfig;
  onConfigChange: (field: string, value: string | number | boolean | null) => void;
  availableTasks: Task[];
}

export interface TaskSelectionDialogProps {
  open: boolean;
  onClose: () => void;
  onTaskSelect: (tasks: Task[]) => void;
  tasks: Task[];
  onShowTaskForm: () => void;
  fetchTasks: () => Promise<void>;
}

export interface TaskGenerationDialogProps {
  open: boolean;
  onClose: () => void;
  onTaskGenerated: (task: Task) => void;
  selectedModel?: string;
} 
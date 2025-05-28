import { Tool } from './tool';

export interface CrewAgent {
  id?: string;
  name: string;
  role: string;
  goal: string;
  backstory: string;
  tools?: string[];
  llm?: string;
  function_calling_llm?: string | null;
  max_iter?: number;
  max_rpm?: number | null;
  max_execution_time?: number | null;
  verbose?: boolean;
  allow_delegation?: boolean;
  cache?: boolean;
  system_template?: string | null;
  prompt_template?: string | null;
  response_template?: string | null;
  allow_code_execution?: boolean;
  code_execution_mode?: string;
  max_retry_limit?: number;
  use_system_prompt?: boolean;
  respect_context_window?: boolean;
}

export interface CrewTask {
  id: string;
  name: string;
  description: string;
  expected_output: string;
  agent_id: string;
  tools?: string[];
  context: string[];
  agent?: string;
  assigned_agent?: string;
  converter_cls?: string | null;
  async_execution?: boolean;
  markdown?: boolean;
  output_file?: string | null;
  output_json?: string | null;
  output_pydantic?: string | null;
  output?: unknown | null;
  callback?: string | null;
  human_input?: boolean;
  config?: Record<string, unknown>;
}

export interface WorkflowConnection {
  from: string;
  to: string;
  from_type: string;
  to_type: string;
  status?: string;
  reasoning?: string;
}

export interface Crew {
  agents: CrewAgent[];
  tasks: CrewTask[];
  // workflow?: WorkflowConnection[]; // Reverted: Workflow derived from task.context
}

export interface CrewPlanningDialogProps {
  open: boolean;
  onClose: () => void;
  onGenerateCrew: (plan: Crew, executeAfterGeneration: boolean) => void;
  selectedModel: string;
  tools: Tool[];
  selectedTools: string[];
  onToolsChange: (tools: string[]) => void;
} 